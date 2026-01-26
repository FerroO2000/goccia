package rb

import (
	"runtime"
)

type mpscBuffer[T any] struct {
	*commonBuffer

	buffer []slot[T]
}

func newMPSCBuffer[T any](capacity uint64) *mpscBuffer[T] {
	return &mpscBuffer[T]{
		commonBuffer: newCommonBuffer[T](capacity),

		buffer: make([]slot[T], capacity),
	}
}

func (rb *mpscBuffer[T]) push(item T) bool {
	for {
		head := rb.head.Load()
		tail := rb.tail.Load()

		// Check if the buffer is full
		if head-tail >= rb.capacity {
			return false
		}

		slotIndex := head & rb.capMask
		slot := &rb.buffer[slotIndex]

		if slot.dataReady.Load() {
			runtime.Gosched()
			continue
		}

		if !rb.head.CompareAndSwap(head, head+1) {
			runtime.Gosched()
			continue
		}

		// Write data
		slot.data = item
		slot.dataReady.Store(true)

		return true
	}
}

func (rb *mpscBuffer[T]) pop() (T, bool) {
	tail := rb.tail.Load()

	slotIndex := tail & rb.capMask
	slot := &rb.buffer[slotIndex]

	if slot.dataReady.Load() {
		item := slot.data
		slot.dataReady.Store(false)

		rb.tail.Add(1)
		return item, true
	}

	// Check if the buffer is empty
	if tail == rb.head.Load() {
		return *new(T), false
	}

	runtime.Gosched()

	return *new(T), false
}
