// Package rb provides a lock-free spsc/mpmc generic ring buffer.
package rb

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/cpu"
)

var maxSpins = runtime.NumCPU() * 32

// ErrClosed is returned when the buffer is closed.
var ErrClosed = errors.New("ring buffer: buffer is closed")

// BufferKind is the type of the internal buffer implementation.
type BufferKind uint8

const (
	//BufferKindSPSC is the single producer/single consumer ring buffer implementation.
	BufferKindSPSC BufferKind = iota
	//BufferKindMPMC is the multiple producer/multiple consumer ring buffer implementation.
	BufferKindMPMC
)

func (bk BufferKind) String() string {
	switch bk {
	case BufferKindSPSC:
		return "SPSC"
	case BufferKindMPMC:
		return "MPMC"
	default:
		return "unknown"
	}
}

// RingBuffer is a lock-free spsc/mpmc generic ring buffer.
type RingBuffer[T any] struct {
	// kind is the type of the internal buffer
	kind BufferKind

	_ cpu.CacheLinePad

	// spsc is the single producer/single consumer ring buffer implementation
	spsc *spscBuffer[T]

	// mpmc is the multiple producer/multiple consumer ring buffer implementation
	mpmc *mpmcBuffer[T]

	_ cpu.CacheLinePad

	// isClosed states whether the buffer is closed.
	isClosed atomic.Bool

	_ cpu.CacheLinePad

	// isFull states whether the buffer is full.
	isFull atomic.Bool

	_ cpu.CacheLinePad

	// isEmpty states whether the buffer is empty.
	isEmpty atomic.Bool

	_ cpu.CacheLinePad

	// notEmpty and notFull are used to signal that the buffer is not empty or full
	notEmpty *sync.Cond
	notFull  *sync.Cond
	mux      *sync.Mutex
}

// NewRingBuffer returns a new lock-free spsc/mpmc generic ring buffer.
func NewRingBuffer[T any](capacity uint32, kind BufferKind) *RingBuffer[T] {
	mux := &sync.Mutex{}

	rb := &RingBuffer[T]{
		kind: kind,

		mux:      mux,
		notEmpty: sync.NewCond(mux),
		notFull:  sync.NewCond(mux),
	}

	parsedCapacity := roundToPowerOf2(capacity)

	switch kind {
	case BufferKindSPSC:
		rb.spsc = newSPSCBuffer[T](parsedCapacity)
	case BufferKindMPMC:
		rb.mpmc = newMPMCBuffer[T](parsedCapacity)
	}

	return rb
}

func (rb *RingBuffer[T]) push(item T) bool {
	switch rb.kind {
	case BufferKindSPSC:
		return rb.spsc.push(item)
	case BufferKindMPMC:
		return rb.mpmc.push(item)
	default:
		return false
	}
}

func (rb *RingBuffer[T]) pop() (T, bool) {
	switch rb.kind {
	case BufferKindSPSC:
		return rb.spsc.pop()
	case BufferKindMPMC:
		return rb.mpmc.pop()
	default:
		return *new(T), false
	}
}

func (rb *RingBuffer[T]) len() uint32 {
	switch rb.kind {
	case BufferKindSPSC:
		return rb.spsc.len()
	case BufferKindMPMC:
		return rb.mpmc.len()
	default:
		return 0
	}
}

func (rb *RingBuffer[T]) wait(ctx context.Context, cond *sync.Cond) error {
	done := make(chan struct{})

	go func() {
		defer close(done)
		cond.Wait()
	}()

	select {
	case <-done:
		return nil

	case <-ctx.Done():
		// Wake up the waiting goroutine
		cond.Broadcast()
		<-done
		return ctx.Err()
	}
}

func (rb *RingBuffer[T]) Write(item T) error {
	// Check if buffer is closed
	if rb.isClosed.Load() {
		return ErrClosed
	}

	for range maxSpins {
		// Try to push the item
		if rb.push(item) {
			goto cleanup
		}

		// The buffer is full, yield to other goroutines
		runtime.Gosched()
	}

	// Try to push the item
	for !rb.push(item) {
		// Buffer is still full, yield to other goroutines
		runtime.Gosched()

		// Retry to push the item
		if rb.push(item) {
			goto cleanup
		}

		// Buffer is full, wait for space
		rb.mux.Lock()

		// Set buffer as full
		rb.isFull.Store(true)

		// Check if buffer is closed
		if rb.isClosed.Load() {
			rb.mux.Unlock()
			return ErrClosed
		}

		// Wait for space
		rb.notFull.Wait()

		// Someone signaled the buffer as not full
		rb.mux.Unlock()
	}

cleanup:
	// Check if the buffer is marked as empty,
	// if so, signal that the buffer is not empty
	if rb.isEmpty.CompareAndSwap(true, false) {
		rb.mux.Lock()
		rb.notEmpty.Broadcast()
		rb.mux.Unlock()
	}

	return nil
}

func (rb *RingBuffer[T]) Read(ctx context.Context) (T, error) {
	var item T
	var popOk bool

	for range maxSpins {
		// Try to pop an item
		item, popOk = rb.pop()
		if popOk {
			goto cleanup
		}

		// The buffer is empty, yield to other goroutines
		runtime.Gosched()
	}

	// Try to pop an item
	for {
		item, popOk = rb.pop()
		if popOk {
			goto cleanup
		}

		// Buffer is still empty, yield to other goroutines
		runtime.Gosched()

		// Retry to pop an item
		item, popOk = rb.pop()
		if popOk {
			goto cleanup
		}

		// Buffer is empty, wait for data
		rb.mux.Lock()

		// Set buffer as empty
		rb.isEmpty.Store(true)

		// Check if buffer is closed
		if rb.isClosed.Load() {
			rb.mux.Unlock()
			return item, ErrClosed
		}

		// Wait for data, return an error if the timeout is reached
		if err := rb.wait(ctx, rb.notEmpty); err != nil {
			rb.mux.Unlock()
			return item, err
		}

		// Someone signaled the buffer as not empty
		rb.mux.Unlock()
	}

cleanup:
	// Check if buffer is marked as full,
	// if so, signal buffer as not full
	if rb.isFull.CompareAndSwap(true, false) {
		rb.mux.Lock()
		rb.notFull.Broadcast()
		rb.mux.Unlock()
	}

	return item, nil
}

// Len returns the number of items in the buffer.
func (rb *RingBuffer[T]) Len() uint32 {
	return rb.len()
}

// Close closes the buffer.
func (rb *RingBuffer[T]) Close() {
	if !rb.isClosed.CompareAndSwap(false, true) {
		return
	}

	rb.mux.Lock()
	rb.notEmpty.Broadcast()
	rb.notFull.Broadcast()
	rb.mux.Unlock()
}
