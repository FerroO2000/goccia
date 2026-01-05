package connector

import (
	"github.com/FerroO2000/goccia/internal/rb"
)

// ErrClosed is returned when the ring buffer is closed.
var ErrClosed = rb.ErrClosed

func newRingBuffer[T any](capacity uint32) *RingBuffer[T] {
	return rb.NewRingBuffer[T](capacity, rb.BufferKindSPSC)
}

// NewRingBuffer returns a new lock-free spsc generic ring buffer.
func NewRingBuffer[T msgBody](capacity uint32) *RingBuffer[*msgWrap[T]] {
	return newRingBuffer[*msgWrap[T]](capacity)
}
