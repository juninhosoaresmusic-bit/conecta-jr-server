package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"net/http"

	"encoding/binary"
	"errors"
	"fmt"
	"github.com/coder/websocket"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	protoVersion = 1
	maxFrame     = 16 << 20
	helloTimeout = 15 * time.Second
	idleTimeout  = 90 * time.Second
)

const (
	msgHello         byte = 1
	msgHelloAck      byte = 2
	msgConnect       byte = 3
	msgIncoming      byte = 4
	msgAccept        byte = 5
	msgReject        byte = 6
	msgSession       byte = 7
	msgPeerData      byte = 8
	msgPing          byte = 9
	msgPong          byte = 10
	msgPresenceQuery byte = 11
	msgPresenceReply byte = 12
	msgDisconnect    byte = 13
	msgError         byte = 14
)

type frame struct {
	typ  byte
	body []byte
}

type client struct {
	id            string
	conn          net.Conn
	rd            *bufio.Reader
	wr            *bufio.Writer
	send          chan frame
	done          chan struct{}
	lastSeen      atomic.Int64
	closeOnce     sync.Once
	attemptMu     sync.Mutex
	attemptWindow time.Time
	attemptCount  int
}

type pendingKey struct{ from, to string }
type pending struct {
	from, to     string
	initiatorKey []byte
	created      time.Time
}

type hub struct {
	mu         sync.RWMutex
	clients    map[string]*client
	pending    map[pendingKey]pending
	sessions   map[pendingKey]time.Time
	maxClients int
}

func newHub(max int) *hub {
	return &hub{clients: make(map[string]*client), pending: make(map[pendingKey]pending), sessions: make(map[pendingKey]time.Time), maxClients: max}
}

func validID(id string) bool {
	if len(id) != 9 {
		return false
	}
	if id[0] == '0' {
		return false
	}
	for _, r := range id {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func readFrame(r *bufio.Reader) (frame, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return frame{}, err
	}
	n := int(binary.BigEndian.Uint32(hdr[:4]))
	if n < 1 || n > maxFrame {
		return frame{}, fmt.Errorf("invalid frame length %d", n)
	}
	b := make([]byte, n-1)
	if _, err := io.ReadFull(r, b); err != nil {
		return frame{}, err
	}
	return frame{typ: hdr[4], body: b}, nil
}

func writeFrame(w *bufio.Writer, f frame) error {
	if len(f.body)+1 > maxFrame {
		return errors.New("frame too large")
	}
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(f.body)+1))
	hdr[4] = f.typ
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(f.body) > 0 {
		if _, err := w.Write(f.body); err != nil {
			return err
		}
	}
	return w.Flush()
}

func packStrings(parts ...[]byte) []byte {
	total := 0
	for _, p := range parts {
		total += 2 + len(p)
	}
	out := make([]byte, 0, total)
	for _, p := range parts {
		if len(p) > 65535 {
			p = p[:65535]
		}
		var l [2]byte
		binary.BigEndian.PutUint16(l[:], uint16(len(p)))
		out = append(out, l[:]...)
		out = append(out, p...)
	}
	return out
}

func unpackStrings(b []byte, count int) ([][]byte, error) {
	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		if len(b) < 2 {
			return nil, io.ErrUnexpectedEOF
		}
		n := int(binary.BigEndian.Uint16(b[:2]))
		b = b[2:]
		if n > len(b) {
			return nil, io.ErrUnexpectedEOF
		}
		p := make([]byte, n)
		copy(p, b[:n])
		b = b[n:]
		out = append(out, p)
	}
	if len(b) != 0 {
		return nil, errors.New("trailing data")
	}
	return out, nil
}

func (c *client) close() {
	c.closeOnce.Do(func() { close(c.done); _ = c.conn.Close() })
}

func (h *hub) register(c *client) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.clients) >= h.maxClients {
		return errors.New("server capacity reached")
	}
	if old := h.clients[c.id]; old != nil {
		return errors.New("ID already online")
	}
	h.clients[c.id] = c
	return nil
}

func (h *hub) unregister(c *client) {
	h.mu.Lock()
	if h.clients[c.id] == c {
		delete(h.clients, c.id)
	}
	for k, p := range h.pending {
		if p.from == c.id || p.to == c.id {
			delete(h.pending, k)
		}
	}
	for k := range h.sessions {
		if k.from == c.id || k.to == c.id {
			delete(h.sessions, k)
		}
	}
	h.mu.Unlock()
}

func (h *hub) get(id string) *client { h.mu.RLock(); c := h.clients[id]; h.mu.RUnlock(); return c }
func (h *hub) sendTo(id string, f frame) bool {
	c := h.get(id)
	if c == nil {
		return false
	}
	select {
	case c.send <- f:
		return true
	default:
		return false
	}
}

func (c *client) allowConnect() bool {
	c.attemptMu.Lock()
	defer c.attemptMu.Unlock()
	now := time.Now()
	if c.attemptWindow.IsZero() || now.Sub(c.attemptWindow) >= time.Minute {
		c.attemptWindow = now
		c.attemptCount = 0
	}
	if c.attemptCount >= 20 {
		return false
	}
	c.attemptCount++
	return true
}

func sessionKey(a, b string) pendingKey {
	if a < b {
		return pendingKey{a, b}
	}
	return pendingKey{b, a}
}

func (h *hub) hasSession(a, b string) bool {
	h.mu.RLock()
	_, ok := h.sessions[sessionKey(a, b)]
	h.mu.RUnlock()
	return ok
}

func randomToken(n int) []byte {
	b := make([]byte, n)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return b
}

func (h *hub) handle(c *client, f frame) error {
	c.lastSeen.Store(time.Now().UnixNano())
	switch f.typ {
	case msgPing:
		select {
		case c.send <- frame{typ: msgPong, body: f.body}:
		default:
		}
	case msgConnect:
		if !c.allowConnect() {
			c.send <- frame{typ: msgError, body: packStrings([]byte("RATE_LIMIT"))}
			return nil
		}
		parts, err := unpackStrings(f.body, 2)
		if err != nil {
			return err
		}
		target := string(parts[0])
		key := parts[1]
		if !validID(target) || target == c.id {
			return errors.New("invalid target")
		}
		if len(key) < 32 || len(key) > 1024 {
			return errors.New("invalid public key")
		}
		if h.get(target) == nil {
			c.send <- frame{typ: msgError, body: packStrings([]byte("OFFLINE"), []byte(target))}
			return nil
		}
		h.mu.Lock()
		h.pending[pendingKey{c.id, target}] = pending{from: c.id, to: target, initiatorKey: key, created: time.Now()}
		h.mu.Unlock()
		if !h.sendTo(target, frame{typ: msgIncoming, body: packStrings([]byte(c.id), key)}) {
			c.send <- frame{typ: msgError, body: packStrings([]byte("BUSY"), []byte(target))}
		}
	case msgAccept:
		parts, err := unpackStrings(f.body, 3)
		if err != nil {
			return err
		}
		from := string(parts[0])
		responderKey := parts[1]
		mode := parts[2]
		if len(responderKey) < 32 || len(responderKey) > 1024 || len(mode) != 1 {
			return errors.New("bad accept")
		}
		k := pendingKey{from, c.id}
		h.mu.Lock()
		p, ok := h.pending[k]
		if ok {
			delete(h.pending, k)
		}
		h.mu.Unlock()
		if !ok || time.Since(p.created) > 60*time.Second {
			return errors.New("no pending request")
		}
		sid := randomToken(16)
		h.mu.Lock()
		h.sessions[sessionKey(from, c.id)] = time.Now()
		h.mu.Unlock()
		h.sendTo(from, frame{typ: msgSession, body: packStrings([]byte(c.id), responderKey, mode, sid)})
		h.sendTo(c.id, frame{typ: msgSession, body: packStrings([]byte(from), p.initiatorKey, mode, sid)})
	case msgReject:
		parts, err := unpackStrings(f.body, 1)
		if err != nil {
			return err
		}
		from := string(parts[0])
		h.mu.Lock()
		delete(h.pending, pendingKey{from, c.id})
		h.mu.Unlock()
		h.sendTo(from, frame{typ: msgReject, body: packStrings([]byte(c.id))})
	case msgPeerData:
		parts, err := unpackStrings(f.body, 2)
		if err != nil {
			return err
		}
		peer := string(parts[0])
		payload := parts[1]
		if !validID(peer) || len(payload) == 0 || len(payload) > maxFrame-128 {
			return errors.New("bad peer data")
		}
		if !h.hasSession(c.id, peer) {
			return errors.New("peer data without accepted session")
		}
		if !h.sendTo(peer, frame{typ: msgPeerData, body: packStrings([]byte(c.id), payload)}) {
			c.send <- frame{typ: msgError, body: packStrings([]byte("PEER_OFFLINE"), []byte(peer))}
		}
	case msgPresenceQuery:
		parts, err := unpackStrings(f.body, 1)
		if err != nil {
			return err
		}
		ids := strings.Split(string(parts[0]), ",")
		if len(ids) > 100 {
			ids = ids[:100]
		}
		var sb strings.Builder
		for i, id := range ids {
			if !validID(id) {
				continue
			}
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(id)
			if h.get(id) != nil {
				sb.WriteString(":1")
			} else {
				sb.WriteString(":0")
			}
		}
		c.send <- frame{typ: msgPresenceReply, body: packStrings([]byte(sb.String()))}
	case msgDisconnect:
		parts, err := unpackStrings(f.body, 1)
		if err != nil {
			return err
		}
		peer := string(parts[0])
		h.mu.Lock()
		delete(h.sessions, sessionKey(c.id, peer))
		h.mu.Unlock()
		h.sendTo(peer, frame{typ: msgDisconnect, body: packStrings([]byte(c.id))})
	default:
		return fmt.Errorf("unsupported message type %d", f.typ)
	}
	return nil
}

func serveConn(h *hub, conn net.Conn) {
	c := &client{conn: conn, rd: bufio.NewReaderSize(conn, 64<<10), wr: bufio.NewWriterSize(conn, 64<<10), send: make(chan frame, 128), done: make(chan struct{})}
	defer c.close()
	_ = conn.SetReadDeadline(time.Now().Add(helloTimeout))
	f, err := readFrame(c.rd)
	if err != nil {
		return
	}
	if f.typ != msgHello {
		return
	}
	parts, err := unpackStrings(f.body, 2)
	if err != nil {
		return
	}
	id := string(parts[0])
	verBytes := parts[1]
	if !validID(id) || len(verBytes) != 2 || int(binary.BigEndian.Uint16(verBytes)) != protoVersion {
		return
	}
	c.id = id
	c.lastSeen.Store(time.Now().UnixNano())
	if err := h.register(c); err != nil {
		_ = writeFrame(c.wr, frame{typ: msgError, body: packStrings([]byte(err.Error()))})
		return
	}
	defer h.unregister(c)
	_ = conn.SetReadDeadline(time.Time{})
	ackToken := randomToken(16)
	if err := writeFrame(c.wr, frame{typ: msgHelloAck, body: packStrings([]byte("OK"), ackToken)}); err != nil {
		return
	}

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for {
			select {
			case f := <-c.send:
				_ = conn.SetWriteDeadline(time.Now().Add(20 * time.Second))
				if err := writeFrame(c.wr, f); err != nil {
					c.close()
					return
				}
			case <-c.done:
				return
			}
		}
	}()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(idleTimeout))
		f, err := readFrame(c.rd)
		if err != nil {
			break
		}
		if err := h.handle(c, f); err != nil {
			select {
			case c.send <- frame{typ: msgError, body: packStrings([]byte("PROTO"), []byte(err.Error()))}:
			default:
			}
			break
		}
		select {
		case <-ticker.C:
		default:
		}
	}
	c.close()
	<-writerDone
}

func cleanupLoop(h *hub, done <-chan struct{}) {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			now := time.Now()
			h.mu.Lock()
			for k, p := range h.pending {
				if now.Sub(p.created) > 60*time.Second {
					delete(h.pending, k)
				}
			}
			h.mu.Unlock()
		case <-done:
			return
		}
	}
}

func envInt(name string, def int) int {
	if s := os.Getenv(name); s != "" {
		if n, e := strconv.Atoi(s); e == nil && n > 0 {
			return n
		}
	}
	return def
}

func runRenderWebSocket(h *hub, port string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "CONECTA JR relay online\n")
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
		if err != nil {
			log.Printf("websocket accept: %v", err)
			return
		}
		c.SetReadLimit(maxFrame + 64)
		nc := websocket.NetConn(context.Background(), c, websocket.MessageBinary)
		serveConn(h, nc)
	})
	server := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Printf("CONECTA JR Render/WebSocket relay listening on :%s/ws (max clients %d)", port, h.maxClients)
	return server.ListenAndServe()
}

func main() {
	max := envInt("CJ_MAX_CLIENTS", 10000)
	h := newHub(max)
	done := make(chan struct{})
	go cleanupLoop(h, done)

	// Render fornece PORT e termina TLS na borda. Nesse modo o cliente usa wss://host/ws.
	if port := os.Getenv("PORT"); port != "" {
		if err := runRenderWebSocket(h, port); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
		return
	}

	addr := os.Getenv("CJ_LISTEN")
	if addr == "" {
		addr = ":44330"
	}
	var ln net.Listener
	var err error
	certFile, keyFile := os.Getenv("CJ_TLS_CERT"), os.Getenv("CJ_TLS_KEY")
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			log.Fatal("CJ_TLS_CERT and CJ_TLS_KEY must be set together")
		}
		cert, e := tls.LoadX509KeyPair(certFile, keyFile)
		if e != nil {
			log.Fatal(e)
		}
		cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		ln, err = tls.Listen("tcp", addr, cfg)
		log.Printf("CONECTA JR TLS relay listening on %s (max clients %d)", addr, max)
	} else {
		ln, err = net.Listen("tcp", addr)
		log.Printf("CONECTA JR DEVELOPMENT relay listening WITHOUT TLS on %s (max clients %d)", addr, max)
	}
	if err != nil {
		log.Fatal(err)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() { <-sig; close(done); _ = ln.Close() }()
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-done:
				return
			default:
				log.Printf("accept: %v", err)
				continue
			}
		}
		go serveConn(h, conn)
	}
}
