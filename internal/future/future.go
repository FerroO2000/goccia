// Package future provides types and functions for working with futures.
package future

import "context"

// Future represents a future value that can be resolved or rejected.
type Future[T any] struct {
	done  chan struct{}
	value T
	err   error
}

func newFuture[T any]() *Future[T] {
	return &Future[T]{
		done: make(chan struct{}),
	}
}

func (f *Future[T]) resolve(value T) {
	f.value = value
	close(f.done)
}

func (f *Future[T]) reject(err error) {
	f.err = err
	close(f.done)
}

// Await blocks until the future is resolved or rejected.
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	select {
	case <-f.done:
		return f.value, f.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
