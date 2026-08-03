package link

import (
	"runtime"

	"github.com/FerroO2000/goccia/internal/future"
	"github.com/FerroO2000/goccia/internal/message"
)

type HTTPFuture = message.Message[*message.HTTPResponse]

type HTTP struct {
	futureRegistry *future.Registry[*HTTPFuture]
}

func NewHTTP() *HTTP {
	return &HTTP{
		futureRegistry: future.NewRegistry[*HTTPFuture](runtime.NumCPU()),
	}
}

func (h *HTTP) NewFuture() (uint64, *future.Future[*HTTPFuture]) {
	return h.futureRegistry.New()
}

func (h *HTTP) ResolveFuture(id uint64, value *HTTPFuture) bool {
	return h.futureRegistry.Resolve(id, value)
}

func (h *HTTP) RejectFuture(id uint64, err error) bool {
	return h.futureRegistry.Reject(id, err)
}

func (h *HTTP) DeleteFuture(id uint64) bool {
	return h.futureRegistry.Delete(id)
}
