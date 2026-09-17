package main

import (
	"bufio"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestValidID(t *testing.T) {
	good := []string{"123456789", "999999999"}
	bad := []string{"012345678", "123", "12345678a", ""}
	for _, s := range good {
		if !validID(s) {
			t.Fatalf("expected valid %q", s)
		}
	}
	for _, s := range bad {
		if validID(s) {
			t.Fatalf("expected invalid %q", s)
		}
	}
}
func TestPackRoundtrip(t *testing.T) {
	in := [][]byte{[]byte("abc"), {0, 1, 2, 3}, []byte("")}
	b := packStrings(in...)
	out, err := unpackStrings(b, len(in))
	if err != nil {
		t.Fatal(err)
	}
	for i := range in {
		if string(in[i]) != string(out[i]) {
			t.Fatalf("mismatch %d", i)
		}
	}
}
func TestFrameRoundtrip(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	done := make(chan error, 1)
	go func() { w := bufio.NewWriter(a); done <- writeFrame(w, frame{typ: msgPing, body: []byte("hello")}) }()
	r := bufio.NewReader(b)
	f, err := readFrame(r)
	if err != nil {
		t.Fatal(err)
	}
	if f.typ != msgPing || string(f.body) != "hello" {
		t.Fatal("bad frame")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestHelloVersionBytes(t *testing.T) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], protoVersion)
	if binary.BigEndian.Uint16(b[:]) != 1 {
		t.Fatal("version")
	}
}
func TestPendingExpiryConstant(t *testing.T) {
	if helloTimeout < time.Second {
		t.Fatal("timeout too short")
	}
}

func TestSessionKeyCanonical(t *testing.T) {
	a := sessionKey("123456789", "987654321")
	b := sessionKey("987654321", "123456789")
	if a != b {
		t.Fatal("session key must be canonical")
	}
}

func TestConnectRateLimit(t *testing.T) {
	c := &client{}
	for i := 0; i < 20; i++ {
		if !c.allowConnect() {
			t.Fatalf("attempt %d unexpectedly blocked", i)
		}
	}
	if c.allowConnect() {
		t.Fatal("21st request in same minute should be blocked")
	}
}

func TestPeerDataRequiresSession(t *testing.T) {
	h := newHub(10)
	a := &client{id: "123456789", send: make(chan frame, 4), done: make(chan struct{})}
	b := &client{id: "987654321", send: make(chan frame, 4), done: make(chan struct{})}
	h.clients[a.id] = a
	h.clients[b.id] = b
	body := packStrings([]byte(b.id), []byte{1, 2, 3})
	if err := h.handle(a, frame{typ: msgPeerData, body: body}); err == nil {
		t.Fatal("relay accepted peer data without session")
	}
	h.sessions[sessionKey(a.id, b.id)] = time.Now()
	if err := h.handle(a, frame{typ: msgPeerData, body: body}); err != nil {
		t.Fatal(err)
	}
}
