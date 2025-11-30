package pool

import (
	"context"

	"github.com/FerroO2000/goccia/internal/rb"
)

// FanOut is an utility struct to be used by a worker pool
// that sends tasks (messages) to multiple workers.
type FanOut[T any] struct {
	buffer *rb.RingBuffer[T]
}

// NewFanOut returns a new fan-out struct.
func NewFanOut[T any](bufferCapacity int) *FanOut[T] {
	return &FanOut[T]{
		buffer: rb.NewRingBuffer[T](uint32(bufferCapacity), rb.BufferKindMPMC),
	}
}

// AddTask enqueues a task in the ring buffer.
func (fo *FanOut[T]) AddTask(ctx context.Context, task T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return fo.buffer.Write(task)
}

// ReadTask dequeues a task from the ring buffer.
func (fo *FanOut[T]) ReadTask() (T, error) {
	return fo.buffer.Read()
}

// Close closes the ring buffer.
func (fo *FanOut[T]) Close() {
	fo.buffer.Close()
}
