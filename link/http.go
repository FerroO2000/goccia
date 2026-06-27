package link

import "github.com/FerroO2000/goccia/internal/future"

type HTTPResponseMessage struct {
	StatusCode int
	Header     map[string]string
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
	futureRegistry *future.Registry[*HTTPResponseMessage]
}

func NewHTTP(config *HTTPConfig) *HTTP {
	return &HTTP{
		futureRegistry: future.NewRegistry[*HTTPResponseMessage](config.FutureShards),
	}
}

func (h *HTTP) NewFuture() (uint64, *future.Future[*HTTPResponseMessage]) {
	return h.futureRegistry.New()
}

func (h *HTTP) ResolveFuture(id uint64, value *HTTPResponseMessage) bool {
	return h.futureRegistry.Resolve(id, value)
}

func (h *HTTP) RejectFuture(id uint64, err error) bool {
	return h.futureRegistry.Reject(id, err)
}
