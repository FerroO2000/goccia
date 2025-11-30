package connector

// import (
// 	"sync"
// 	"sync/atomic"
// 	"testing"
// )

// func Test_RingBuffer_PushPop(t *testing.T) {
// 	const (
// 		itemCount       = 1_000_000
// 		pushConcurrency = 4
// 		popConcurrency  = 4
// 	)

// 	rb := NewRingBuffer[int](128)

// 	var popCount atomic.Uint64

// 	popWg := &sync.WaitGroup{}
// 	popWg.Add(popConcurrency)

// 	readCount := &sync.Map{}
// 	for val := range itemCount {
// 		readCount.Store(val, 0)
// 	}

// 	var popFalseCount atomic.Uint64

// 	for range popConcurrency {
// 		go func() {
// 			defer popWg.Done()

// 			for {
// 				if popCount.Load() == uint64(itemCount) {
// 					break
// 				}

// 				item, ok := rb.pop()
// 				if !ok {
// 					popFalseCount.Add(1)
// 					continue
// 				}
// 				popCount.Add(1)

// 				readCount.Store(item, true)
// 			}
// 		}()
// 	}

// 	pushWg := &sync.WaitGroup{}
// 	pushWg.Add(pushConcurrency)

// 	pushTargetItems := itemCount / pushConcurrency

// 	for idx := range pushConcurrency {
// 		go func(idx int) {
// 			defer pushWg.Done()

// 			offset := idx * pushTargetItems
// 			val := 0
// 			failCount := 0

// 			for val < pushTargetItems {
// 				if !rb.push(val + offset) {
// 					failCount++
// 					continue
// 				}
// 				val++
// 			}

// 			t.Logf("push -> idx: %d; fails: %d", idx, failCount)
// 		}(idx)
// 	}

// 	pushWg.Wait()
// 	popWg.Wait()

// 	if popCount.Load() != uint64(itemCount) {
// 		t.Errorf("Expected %d items, got %d", itemCount, popCount.Load())
// 	}

// 	missingItems := 0
// 	for i := range itemCount {
// 		if _, ok := readCount.Load(i); !ok {
// 			missingItems++
// 		}
// 	}

// 	if missingItems > 0 {
// 		t.Errorf("Missing %d items in total", missingItems)
// 	}

// 	t.Logf("pop false count: %d", popFalseCount.Load())
// }
