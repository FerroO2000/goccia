package pool

import (
	"context"

	"github.com/FerroO2000/goccia/internal/rb"
)

// FanIn is an utility struct to be used by a worker pool
// that receives tasks (messages) from multiple workers.
type FanIn[T any] struct {
	buffer *rb.RingBuffer[T]
}

// NewFanIn returns a new fan-in struct.
func NewFanIn[T any](bufferCapacity int) *FanIn[T] {
	return &FanIn[T]{
		buffer: rb.NewRingBuffer[T](uint32(bufferCapacity), rb.BufferKindMPMC),
	}
}

// AddTask enqueues a task in the ring buffer.
func (fi *FanIn[T]) AddTask(task T) error {
	return fi.buffer.Write(task)
}

// ReadTask dequeues a task from the ring buffer.
func (fi *FanIn[T]) ReadTask(ctx context.Context) (T, error) {
	return fi.buffer.Read(ctx)
}

// Close closes the ring buffer.
func (fi *FanIn[T]) Close() {
	fi.buffer.Close()
}
