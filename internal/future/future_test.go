package future

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Future_AwaitReturnsResolvedValue(t *testing.T) {
	assert := assert.New(t)

	f := newFuture[int]()

	f.resolve(42)

	value, state, err := f.Await(t.Context())
	assert.NoError(err)
	assert.Equal(42, value)
	assert.Equal(StateResolved, state)
}

func Test_Future_AwaitReturnsRejectedError(t *testing.T) {
	assert := assert.New(t)

	expectedErr := errors.New("future rejected")
	f := newFuture[int]()

	f.reject(expectedErr)

	value, state, err := f.Await(t.Context())
	assert.Zero(value)
	assert.ErrorIs(err, expectedErr)
	assert.Equal(StateRejected, state)
}

func Test_Future_AwaitReturnsContextError(t *testing.T) {
	assert := assert.New(t)

	f := newFuture[int]()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	value, state, err := f.Await(ctx)
	assert.Zero(value)
	assert.ErrorIs(err, context.Canceled)
	assert.Equal(StateCanceled, state)
}

func Test_Future_AwaitReturnsContextTimeoutError(t *testing.T) {
	assert := assert.New(t)

	f := newFuture[int]()
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	value, state, err := f.Await(ctx)
	assert.Zero(value)
	assert.ErrorIs(err, context.DeadlineExceeded)
	assert.Equal(StateTimeout, state)
}

func Test_Future_AwaitBlocksUntilResolved(t *testing.T) {
	assert := assert.New(t)

	f := newFuture[int]()

	go func() {
		time.Sleep(10 * time.Millisecond)
		f.resolve(7)
	}()

	value, state, err := f.Await(t.Context())
	assert.NoError(err)
	assert.Equal(7, value)
	assert.Equal(StateResolved, state)
}

func Test_Future_ResultReturnsPendingWithoutBlocking(t *testing.T) {
	assert := assert.New(t)

	f := newFuture[int]()

	value, state, err := f.Result()
	assert.NoError(err)
	assert.Zero(value)
	assert.Equal(StatePending, state)
}

func Test_Future_ResultReturnsCompletedResult(t *testing.T) {
	assert := assert.New(t)

	f := newFuture[int]()
	f.resolve(42)

	value, state, err := f.Result()
	assert.NoError(err)
	assert.Equal(42, value)
	assert.Equal(StateResolved, state)
}
