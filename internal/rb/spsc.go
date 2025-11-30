package rb

import (
	"sync/atomic"

	"golang.org/x/sys/cpu"
)

type spscBuffer[T any] struct {
	head uint32

	_ cpu.CacheLinePad

	tail uint32

	_ cpu.CacheLinePad

	headShared atomic.Uint32

	_ cpu.CacheLinePad

	tailShared atomic.Uint32

	_ cpu.CacheLinePad

	capacity uint32
	capMask  uint32

	_ cpu.CacheLinePad

	buffer []T
}

func newSPSCBuffer[T any](capacity uint32) *spscBuffer[T] {
	return &spscBuffer[T]{
		capacity: capacity,
		capMask:  capacity - 1,

		buffer: make([]T, capacity),
	}
}

func (b *spscBuffer[T]) push(item T) bool {
	// Get head and tail
	head := b.head
	tail := b.tailShared.Load()

	// Check if buffer is full
	if head-tail >= b.capacity {
		// Buffer is full
		return false
	}

	// Add the item to the buffer
	itemIndex := head & b.capMask
	b.buffer[itemIndex] = item

	// Increase head
	b.head = head + 1
	b.headShared.Store(head + 1)

	return true
}

func (b *spscBuffer[T]) pop() (T, bool) {
	var zero T

	// Get head and tail
	head := b.headShared.Load()
	tail := b.tail

	// Check if buffer is empty
	if head == tail {
		// Buffer is empty
		return zero, false
	}

	// Get the item
	itemIndex := tail & b.capMask
	item := b.buffer[itemIndex]

	// Increase tail
	b.tail = tail + 1
	b.tailShared.Store(tail + 1)

	return item, true
}

func (b *spscBuffer[T]) len() uint32 {
	head := b.headShared.Load()
	tail := b.tailShared.Load()

	// Check if tail has wrapped around
	if head < tail {
		return head + b.capacity - tail
	}

	return head - tail
}
