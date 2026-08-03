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

func (rs *registryShard[T]) resolve(id uint64, value T) (resolved bool) {
	rs.mux.Lock()
	future, ok := rs.pendingFutures[id]
	if ok {
		delete(rs.pendingFutures, id)
		future.resolve(value)
		resolved = true
	}
	rs.mux.Unlock()
	return resolved
}

func (rs *registryShard[T]) reject(id uint64, err error) (rejected bool) {
	rs.mux.Lock()
	future, ok := rs.pendingFutures[id]
	if ok {
		delete(rs.pendingFutures, id)
		future.reject(err)
		rejected = true
	}
	rs.mux.Unlock()
	return rejected
}

func (rs *registryShard[T]) delete(id uint64) (deleted bool) {
	rs.mux.Lock()
	if _, ok := rs.pendingFutures[id]; ok {
		delete(rs.pendingFutures, id)
		deleted = true
	}
	rs.mux.Unlock()
	return deleted
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
	return shard.resolve(id, value)
}

// Reject rejects the future with the given error.
// It returns true if the future was found and completed.
func (r *Registry[T]) Reject(id uint64, err error) bool {
	shard := r.getShard(id)
	return shard.reject(id, err)
}

// Delete deletes the future with the given ID.
// It returns true if the future was pending and has been deleted.
func (r *Registry[T]) Delete(id uint64) bool {
	shard := r.getShard(id)
	return shard.delete(id)
}
