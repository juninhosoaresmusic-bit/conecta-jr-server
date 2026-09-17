FROM golang:1.23-alpine AS build

WORKDIR /src

COPY . .

RUN go mod tidy

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/conectajr-server .

FROM alpine:3.20

RUN adduser -D -H -u 10001 conectajr

USER conectajr

COPY --from=build /out/conectajr-server /conectajr-server

ENV CJ_MAX_CLIENTS=1000

CMD ["/conectajr-server"]
