package future

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

const defaultShardCount = 64
const defaultShardCapacity = 4096

func nextPowerOfTwo(value int) int {
	if value <= 1 {
		return 1
	}

	return 1 << bits.Len(uint(value-1))
}

type registryShard[T any] struct {
	mux            sync.Mutex
	pendingFutures map[uint64]*Future[T]
}

func newRegistryShard[T any](capacity int) registryShard[T] {
	return registryShard[T]{
		pendingFutures: make(map[uint64]*Future[T], capacity),
	}
}

func (rs *registryShard[T]) insert(id uint64, future *Future[T]) {
	rs.mux.Lock()
	rs.pendingFutures[id] = future
	rs.mux.Unlock()
}

func (rs *registryShard[T]) extract(id uint64) *Future[T] {
	rs.mux.Lock()

	future, ok := rs.pendingFutures[id]
	if ok {
		delete(rs.pendingFutures, id)
	}

	rs.mux.Unlock()

	return future
}

// Registry holds a collection of pending futures.
type Registry[T any] struct {
	next atomic.Uint64

	shardMask uint64
	shards    []registryShard[T]
}

// NewRegistry returns a new registry with the given shard count.
func NewRegistry[T any](shardCount int) *Registry[T] {
	if shardCount <= 0 {
		shardCount = defaultShardCount
	}
	shardCount = nextPowerOfTwo(shardCount)

	shards := make([]registryShard[T], 0, shardCount)
	perShardCapacity := max(1, defaultShardCapacity/shardCount)
	for range shardCount {
		shards = append(shards, newRegistryShard[T](perShardCapacity))
	}

	return &Registry[T]{
		shardMask: uint64(shardCount - 1),
		shards:    shards,
	}
}

func (r *Registry[T]) getShard(id uint64) *registryShard[T] {
	return &r.shards[id&r.shardMask]
}

// New returns a new future and its ID.
func (r *Registry[T]) New() (uint64, *Future[T]) {
	id := r.next.Add(1)
	future := newFuture[T]()

	shard := r.getShard(id)
	shard.insert(id, future)

	return id, future
}

// Resolve completes the future with the given value.
// It returns true if the future was found and completed.
func (r *Registry[T]) Resolve(id uint64, value T) bool {
	shard := r.getShard(id)

	future := shard.extract(id)
	if future != nil {
		future.resolve(value)
		return true
	}

	return false
}

// Reject rejects the future with the given error.
// It returns true if the future was found and completed.
func (r *Registry[T]) Reject(id uint64, err error) bool {
	shard := r.getShard(id)

	future := shard.extract(id)
	if future != nil {
		future.reject(err)
		return true
	}

	return false
}
