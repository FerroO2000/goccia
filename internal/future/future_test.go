package future

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Future_AwaitReturnsResolvedValue(t *testing.T) {
	f := newFuture[int]()

	f.resolve(42)

	value, err := f.Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 42, value)
}

func Test_Future_AwaitReturnsRejectedError(t *testing.T) {
	expectedErr := errors.New("future rejected")
	f := newFuture[int]()

	f.reject(expectedErr)

	value, err := f.Await(t.Context())
	assert.Zero(t, value)
	assert.ErrorIs(t, err, expectedErr)
}

func Test_Future_AwaitReturnsContextError(t *testing.T) {
	f := newFuture[int]()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	value, err := f.Await(ctx)
	assert.Zero(t, value)
	assert.ErrorIs(t, err, context.Canceled)
}

func Test_Future_AwaitBlocksUntilResolved(t *testing.T) {
	f := newFuture[int]()

	go func() {
		time.Sleep(10 * time.Millisecond)
		f.resolve(7)
	}()

	value, err := f.Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 7, value)
}
