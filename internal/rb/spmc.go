package rb

import (
	"runtime"
)

type spmcBuffer[T any] struct {
	*commonBuffer

	buffer []slot[T]
}

func newSPMCBuffer[T any](capacity uint64) *spmcBuffer[T] {
	return &spmcBuffer[T]{
		commonBuffer: newCommonBuffer[T](capacity),

		buffer: make([]slot[T], capacity),
	}
}

func (rb *spmcBuffer[T]) push(item T) bool {
	head := rb.head.Load()

	slotIndex := head & rb.capMask
	slot := &rb.buffer[slotIndex]

	// Check if the slot is clean/already consumed
	if !slot.dataReady.Load() {
		// Write data
		slot.data = item
		slot.dataReady.Store(true)

		// Advance head
		rb.head.Add(1)

		return true
	}

	// Check if the buffer is full
	tail := rb.tail.Load()
	if head-tail >= rb.capacity {
		return false
	}

	// The slot hasn't been consumed yet
	runtime.Gosched()

	return false
}

func (rb *spmcBuffer[T]) pop() (T, bool) {
	for {
		tail := rb.tail.Load()
		head := rb.head.Load()

		// Check if the buffer is empty
		if tail == head {
			return *new(T), false
		}

		slotIndex := tail & rb.capMask
		slot := &rb.buffer[slotIndex]

		// Check if the producer has written to the slot
		if !slot.dataReady.Load() {
			runtime.Gosched()
			continue
		}

		// Claim this slot over the other consumers
		if !rb.tail.CompareAndSwap(tail, tail+1) {
			// Raced with another consumer
			runtime.Gosched()
			continue
		}

		// Read data
		item := slot.data
		slot.dataReady.Store(false)

		return item, true
	}
}
