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
	r := NewRegistry[int](0)

	id, f := r.New()

	require.NotZero(t, id)
	require.True(t, r.Resolve(id, 12))

	value, err := f.Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 12, value)
}

func Test_Registry_NewRoundsShardCountToPowerOfTwo(t *testing.T) {
	r := NewRegistry[int](3)

	assert.Len(t, r.shards, 4)
	assert.Equal(t, uint64(3), r.shardMask)
}

func Test_Registry_ResolveCompletesAndDeletesFuture(t *testing.T) {
	r := NewRegistry[string](4)
	id, f := r.New()

	require.True(t, r.Resolve(id, "ok"))
	assert.False(t, r.Resolve(id, "again"))

	value, err := f.Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "ok", value)
}

func Test_Registry_RejectCompletesAndDeletesFuture(t *testing.T) {
	expectedErr := errors.New("request timed out")
	r := NewRegistry[int](4)
	id, f := r.New()

	require.True(t, r.Reject(id, expectedErr))
	assert.False(t, r.Reject(id, errors.New("again")))

	value, err := f.Await(t.Context())
	assert.Zero(t, value)
	assert.ErrorIs(t, err, expectedErr)
}

func Test_Registry_ResolveUnknownFuture(t *testing.T) {
	r := NewRegistry[int](4)

	assert.False(t, r.Resolve(0, 1))
	assert.False(t, r.Resolve(99, 1))
}

func Test_Registry_RejectUnknownFuture(t *testing.T) {
	r := NewRegistry[int](4)

	assert.False(t, r.Reject(0, errors.New("missing")))
	assert.False(t, r.Reject(99, errors.New("missing")))
}

func Test_Registry_UsesEveryShard(t *testing.T) {
	r := NewRegistry[int](4)

	for range 4 {
		id, _ := r.New()
		shard := r.getShard(id)
		shard.mux.Lock()
		_, ok := shard.pendingFutures[id]
		shard.mux.Unlock()
		assert.True(t, ok)
	}

	for idx := range r.shards {
		r.shards[idx].mux.Lock()
		pendingCount := len(r.shards[idx].pendingFutures)
		r.shards[idx].mux.Unlock()
		assert.Equal(t, 1, pendingCount)
	}
}

func Test_Registry_ConcurrentNewAndResolve(t *testing.T) {
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

			got, err := f.Await(ctx)
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
		require.NoError(t, err)
	}
}
