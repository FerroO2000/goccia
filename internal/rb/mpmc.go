package rb

import (
	"runtime"
	"sync/atomic"

	"golang.org/x/sys/cpu"
)

type slot[T any] struct {
	dataReady atomic.Bool
	data      T
}

type mpmcBuffer[T any] struct {
	// headTail is a uint64 where the top 32 bits are head and the bottom 32 bits are tail.
	// This allows us to atomically read both head and tail in a single load.
	headTail atomic.Uint64

	// used to avoid false sharing
	_ cpu.CacheLinePad

	capacity uint32
	capMask  uint32

	_ cpu.CacheLinePad

	// buffer is a ring buffer of slots
	buffer []slot[T]
}

func newMPMCBuffer[T any](capacity uint32) *mpmcBuffer[T] {
	return &mpmcBuffer[T]{
		capacity: capacity,
		capMask:  capacity - 1,

		buffer: make([]slot[T], capacity),
	}
}

func (rb *mpmcBuffer[T]) pack(head, tail uint32) uint64 {
	const mask = 1<<32 - 1
	return (uint64(head)<<32 | uint64(tail&mask))
}

func (rb *mpmcBuffer[T]) unpack(headTail uint64) (head, tail uint32) {
	const mask = 1<<32 - 1
	head = uint32((headTail >> 32) & mask)
	tail = uint32(headTail & mask)
	return
}

func (rb *mpmcBuffer[T]) push(item T) bool {
	for {
		// Load head and tail
		headTail := rb.headTail.Load()
		head, tail := rb.unpack(headTail)

		// Check if buffer is full
		if head-tail >= rb.capacity {
			// Buffer is full
			return false
		}

		// Get slot and check it's not still being read
		slotIndex := head & rb.capMask
		slot := &rb.buffer[slotIndex]

		// If dataReady is true, it means this slot hasn't been consumed yet
		if slot.dataReady.Load() {
			// Someone else is reading this slot, retry
			runtime.Gosched()
			continue
		}

		// Claim this slot by advancing head pointer
		newHeadTail := rb.pack(head+1, tail)
		if !rb.headTail.CompareAndSwap(headTail, newHeadTail) {
			// Someone else modified the buffer, retry
			runtime.Gosched()
			continue
		}

		// Write the data
		slot.data = item

		// Mark data as ready
		slot.dataReady.Store(true)

		return true
	}
}

func (rb *mpmcBuffer[T]) pop() (T, bool) {
	for {
		// Load head and tail
		headTail := rb.headTail.Load()
		head, tail := rb.unpack(headTail)

		// Check if buffer is empty
		if head == tail {
			// Buffer is empty
			return *new(T), false
		}

		// Get slot
		slotIndex := tail & rb.capMask
		slot := &rb.buffer[slotIndex]

		// Check if data is actually ready to be read
		if !slot.dataReady.Load() {
			// Data not yet ready, retry
			runtime.Gosched()
			continue
		}

		// Try to claim this slot for reading by advancing tail
		nextHeadTail := rb.pack(head, tail+1)
		if !rb.headTail.CompareAndSwap(headTail, nextHeadTail) {
			// Someone else modified the buffer, retry
			runtime.Gosched()
			continue
		}

		// Read the item
		item := slot.data

		// Mark slot as available for reuse
		slot.dataReady.Store(false)

		return item, true
	}
}

func (rb *mpmcBuffer[T]) len() uint32 {
	headTail := rb.headTail.Load()
	head, tail := rb.unpack(headTail)

	// Check if tail has wrapped around
	if head < tail {
		return head + rb.capacity - tail
	}

	return head - tail
}
