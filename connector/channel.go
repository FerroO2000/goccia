package connector

import (
	"sync/atomic"
	"time"
)

// Channel implements a [Connector] using a channel.
type Channel[T any] struct {
	buffer chan T

	size uint64

	closed atomic.Bool
}

// NewChannel creates a new [Channel] with the given capacity.
func NewChannel[T any](size uint64) *Channel[T] {
	return &Channel[T]{
		buffer: make(chan T, size),

		size: size,
	}
}

func (c *Channel[T]) Write(item T) error {
	if c.closed.Load() {
		return ErrClosed
	}

	// Try to send the item without blocking
	select {
	case c.buffer <- item:
		return nil
	default:
		// If channel is full, check if closed again before blocking
		if c.closed.Load() {
			return ErrClosed
		}

		// Try to send with potential for channel to be closed
		select {
		case c.buffer <- item:
			return nil
		default:
			if c.closed.Load() {
				return ErrClosed
			}
			// Block until we can send
			c.buffer <- item
			return nil
		}
	}
}

func (c *Channel[T]) Read() (T, error) {
	if c.closed.Load() && len(c.buffer) == 0 {
		var zero T
		return zero, ErrClosed
	}

	// Try to receive without blocking
	select {
	case item := <-c.buffer:
		return item, nil
	default:
		// If channel is empty, check if closed again
		if c.closed.Load() && len(c.buffer) == 0 {
			var zero T
			return zero, ErrClosed
		}

		// Block until we get an item or channel is closed
		select {
		case item, ok := <-c.buffer:
			if !ok {
				var zero T
				return zero, ErrClosed
			}
			return item, nil
		}
	}
}

// Close closes the [Channel] connector.
func (c *Channel[T]) Close() {
	c.closed.Store(true)
}

func (c *Channel[T]) SetReadTimeout(_ time.Duration) {}
