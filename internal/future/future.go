// Package future provides types and functions for working with futures.
package future

import (
	"context"
	"errors"
)

// State represents the state of a future.
type State = uint8

const (
	// StatePending represents a pending future.
	StatePending State = iota
	// StateResolved represents a resolved future.
	StateResolved
	// StateRejected represents a rejected future.
	StateRejected
	// StateCanceled represents a canceled future.
	StateCanceled
	// StateTimeout represents a timed out future.
	StateTimeout
)

// StateToString returns the string representation of a future state.
func StateToString(state State) string {
	switch state {
	case StatePending:
		return "pending"
	case StateResolved:
		return "resolved"
	case StateRejected:
		return "rejected"
	case StateCanceled:
		return "canceled"
	case StateTimeout:
		return "timeout"
	default:
		return "unknown"
	}
}

// Future represents a future value that can be resolved or rejected.
type Future[T any] struct {
	state State
	done  chan struct{}
	err   error
	value T
}

func newFuture[T any]() *Future[T] {
	return &Future[T]{
		state: StatePending,
		done:  make(chan struct{}),
	}
}

func (f *Future[T]) resolve(value T) {
	f.state = StateResolved
	f.value = value
	close(f.done)
}

func (f *Future[T]) reject(err error) {
	f.state = StateRejected
	f.err = err
	close(f.done)
}

// Await blocks until the future is resolved or rejected.
// It returns the resolved value, the state of the future, and any error.
func (f *Future[T]) Await(ctx context.Context) (T, State, error) {
	select {
	case <-f.done:
		return f.value, f.state, f.err

	case <-ctx.Done():
		var zero T
		state := StateCanceled

		if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			state = StateTimeout
		}

		return zero, state, ctx.Err()
	}
}

// Result returns the current result without blocking.
// If the future is still pending, it returns the zero value, StatePending,
// and a nil error.
func (f *Future[T]) Result() (T, State, error) {
	select {
	case <-f.done:
		return f.value, f.state, f.err
	default:
		var zero T
		return zero, StatePending, nil
	}
}
