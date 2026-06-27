package link

import (
	"net/http"

	"github.com/FerroO2000/goccia/internal/future"
	"github.com/FerroO2000/goccia/internal/message"
)

type msg[T message.Body] = message.Message[T]

type HTTPResponseMessage struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func (m *HTTPResponseMessage) Destroy() {
	m.Header = nil
	m.Body = nil
}

const (
	DefaultHTTPFutureShards = 64
)

type HTTPConfig struct {
	FutureShards int
}

func NewHTTPConfig() *HTTPConfig {
	return &HTTPConfig{
		FutureShards: DefaultHTTPFutureShards,
	}
}

type HTTP struct {
	futureRegistry *future.Registry[*msg[*HTTPResponseMessage]]
}

func NewHTTP(config *HTTPConfig) *HTTP {
	return &HTTP{
		futureRegistry: future.NewRegistry[*msg[*HTTPResponseMessage]](config.FutureShards),
	}
}

func (h *HTTP) NewFuture() (uint64, *future.Future[*msg[*HTTPResponseMessage]]) {
	return h.futureRegistry.New()
}

func (h *HTTP) ResolveFuture(id uint64, value *msg[*HTTPResponseMessage]) bool {
	return h.futureRegistry.Resolve(id, value)
}

func (h *HTTP) RejectFuture(id uint64, err error) bool {
	return h.futureRegistry.Reject(id, err)
}
