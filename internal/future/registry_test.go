package future

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Registry_NewDefaultsInvalidShardCount(t *testing.T) {
	assert := assert.New(t)

	r := NewRegistry[int](0)

	id, f := r.New()

	require.NotZero(t, id)
	require.True(t, r.Resolve(id, 12))

	value, state, err := f.Await(t.Context())
	assert.NoError(err)
	assert.Equal(12, value)
	assert.Equal(StateResolved, state)
}

func Test_Registry_NewRoundsShardCountToPowerOfTwo(t *testing.T) {
	assert := assert.New(t)

	r := NewRegistry[int](3)

	assert.Len(r.shards, 4)
	assert.Equal(uint64(3), r.shardMask)
}

func Test_Registry_ResolveCompletesAndDeletesFuture(t *testing.T) {
	assert := assert.New(t)

	r := NewRegistry[string](4)
	id, f := r.New()

	assert.True(r.Resolve(id, "ok"))
	assert.False(r.Resolve(id, "again"))

	value, state, err := f.Await(t.Context())
	assert.NoError(err)
	assert.Equal("ok", value)
	assert.Equal(StateResolved, state)
}

func Test_Registry_RejectCompletesAndDeletesFuture(t *testing.T) {
	assert := assert.New(t)

	expectedErr := errors.New("request timed out")
	r := NewRegistry[int](4)
	id, f := r.New()

	assert.True(r.Reject(id, expectedErr))
	assert.False(r.Reject(id, errors.New("again")))

	value, state, err := f.Await(t.Context())
	assert.Zero(value)
	assert.ErrorIs(err, expectedErr)
	assert.Equal(StateRejected, state)
}

func Test_Registry_ResolveUnknownFuture(t *testing.T) {
	assert := assert.New(t)

	r := NewRegistry[int](4)

	assert.False(r.Resolve(0, 1))
	assert.False(r.Resolve(99, 1))
}

func Test_Registry_RejectUnknownFuture(t *testing.T) {
	assert := assert.New(t)

	r := NewRegistry[int](4)

	assert.False(r.Reject(0, errors.New("missing")))
	assert.False(r.Reject(99, errors.New("missing")))
}

func Test_Registry_DeleteReportsWhetherFutureWasPending(t *testing.T) {
	assert := assert.New(t)

	r := NewRegistry[int](4)
	id, _ := r.New()

	assert.True(r.Delete(id))
	assert.False(r.Delete(id))
	assert.False(r.Resolve(id, 1))
}

func Test_Registry_DeleteLosesToCompletedFuture(t *testing.T) {
	assert := assert.New(t)

	r := NewRegistry[int](4)
	id, f := r.New()

	assert.True(r.Resolve(id, 42))
	assert.False(r.Delete(id))

	value, state, err := f.Result()
	assert.NoError(err)
	assert.Equal(42, value)
	assert.Equal(StateResolved, state)
}

func Test_Registry_UsesEveryShard(t *testing.T) {
	assert := assert.New(t)

	r := NewRegistry[int](4)

	for range 4 {
		id, _ := r.New()
		shard := r.getShard(id)
		shard.mux.Lock()
		_, ok := shard.pendingFutures[id]
		shard.mux.Unlock()
		assert.True(ok)
	}

	for idx := range r.shards {
		r.shards[idx].mux.Lock()
		pendingCount := len(r.shards[idx].pendingFutures)
		r.shards[idx].mux.Unlock()
		assert.Equal(1, pendingCount)
	}
}

func Test_Registry_ConcurrentNewAndResolve(t *testing.T) {
	assert := assert.New(t)

	r := NewRegistry[int](8)
	const futureCount = 512

	var wg sync.WaitGroup
	errCh := make(chan error, futureCount)
	for idx := range futureCount {
		wg.Add(1)
		go func(value int) {
			defer wg.Done()

			id, f := r.New()
			if !r.Resolve(id, value) {
				errCh <- errors.New("failed to resolve future")
				return
			}

			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()

			got, _, err := f.Await(ctx)
			if err != nil {
				errCh <- err
				return
			}

			if got != value {
				errCh <- errors.New("resolved value mismatch")
				return
			}
		}(idx)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		assert.NoError(err)
	}
}
