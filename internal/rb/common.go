package rb

import (
	"sync/atomic"

	"golang.org/x/sys/cpu"
)

type slot[T any] struct {
	dataReady atomic.Bool
	data      T
}

type commonBuffer struct {
	head atomic.Uint64

	_ cpu.CacheLinePad

	tail atomic.Uint64

	_ cpu.CacheLinePad

	capacity uint64
	capMask  uint64

	_ cpu.CacheLinePad
}

func newCommonBuffer[T any](capacity uint64) *commonBuffer {
	return &commonBuffer{
		capacity: capacity,
		capMask:  capacity - 1,
	}
}

func (cb *commonBuffer) len() uint64 {
	tail := cb.tail.Load()
	head := cb.head.Load()

	if head < tail {
		return head + cb.capacity - tail
	}

	return head - tail
}
