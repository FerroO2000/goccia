package rb

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type buffer[T any] interface {
	push(item T) bool
	pop() (T, bool)
}

func Test_bufferImplementations(t *testing.T) {
	const (
		capacity = 128
		items    = 100_000
	)

	suite := []struct {
		kind             BufferKind
		buffer           buffer[int]
		prodNum, consNum int
	}{
		{BufferKindSPSC, newSPSCBuffer[int](capacity), 1, 1},
		{BufferKindMPMC, newMPMCBuffer[int](capacity), 1, 1},
		{BufferKindMPMC, newMPMCBuffer[int](capacity), 1, 8},
		{BufferKindMPMC, newMPMCBuffer[int](capacity), 8, 1},
		{BufferKindMPMC, newMPMCBuffer[int](capacity), 8, 8},
	}

	for _, tCase := range suite {
		tName := fmt.Sprintf("%s-P%d-C%d", tCase.kind, tCase.prodNum, tCase.consNum)

		t.Run(tName, func(t *testing.T) {
			testBuffer(t, tCase.buffer, tCase.prodNum, tCase.consNum, items)
		})
	}
}

func testBuffer(t *testing.T, buffer buffer[int], prodNum, consNum, items int) {
	assert := assert.New(t)

	pushWg := &sync.WaitGroup{}
	pushWg.Add(prodNum)

	valueMap := &sync.Map{}
	for val := range items {
		valueMap.Store(val, true)
	}

	var skippedPush atomic.Int64
	var skippedPop atomic.Int64

	itemsPerProducer := items / prodNum
	for idx := range prodNum {
		go func(idx int) {
			defer pushWg.Done()

			baseVal := idx * itemsPerProducer
			produced := 0
			for {
				if !buffer.push(baseVal + produced) {
					skippedPush.Add(1)
					continue
				}

				produced++
				if produced == itemsPerProducer {
					break
				}
			}
		}(idx)
	}

	popWg := &sync.WaitGroup{}
	popWg.Add(consNum)

	var totalConsumed atomic.Int64

	itemsPerConsumer := items / consNum
	for range consNum {
		go func() {
			defer popWg.Done()

			consumed := 0
			for {
				val, ok := buffer.pop()
				if !ok {
					skippedPop.Add(1)
					continue
				}

				assert.True(valueMap.CompareAndSwap(val, true, false))
				totalConsumed.Add(1)

				consumed++
				if consumed == itemsPerConsumer {
					break
				}
			}
		}()
	}

	pushWg.Wait()
	t.Log("Producers done")

	popWg.Wait()
	t.Log("Consumers done")

	t.Logf("Total consumed items: %d", totalConsumed.Load())
	t.Logf("Skipped push call: %d", skippedPush.Load())
	t.Logf("Skipped pop call: %d", skippedPop.Load())

	assert.Equal(int64(items), totalConsumed.Load())
}

func Test_RingBuffer(t *testing.T) {
	const (
		capacity   = 1024
		totalItems = 1_000_000
	)

	suite := []struct {
		kind             BufferKind
		capacity         int
		prodNum, consNum int
	}{
		{BufferKindSPSC, capacity, 1, 1},
		{BufferKindMPMC, capacity, 1, 1},
		{BufferKindMPMC, capacity, 1, 4},
		{BufferKindMPMC, capacity, 4, 1},
		{BufferKindMPMC, capacity, 8, 8},
	}

	for _, tCase := range suite {
		tName := fmt.Sprintf("%s-P%d-C%d", tCase.kind, tCase.prodNum, tCase.consNum)

		t.Run(tName, func(t *testing.T) {
			testRingBuffer(t, tCase.kind, tCase.capacity, tCase.prodNum, tCase.consNum, totalItems)
		})
	}
}

func testRingBuffer(t *testing.T, kind BufferKind, capacity, prodNum, consNum, totalItems int) {
	assert := assert.New(t)

	itemsPerProd := totalItems / prodNum

	rb := NewRingBuffer[int](uint32(capacity), kind)

	// Used to track received items
	var receivedItems sync.Map
	var receivedCount atomic.Uint64

	// WaitGroups to coordinate test completion
	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup

	// Start time for performance measurement
	startTime := time.Now()

	// Start consumers
	consumerWg.Add(consNum)
	for range consNum {
		go func() {
			defer consumerWg.Done()

			// Each consumer reads until the buffer is closed
			for {
				item, err := rb.Read(t.Context())
				if err != nil {
					assert.ErrorIs(err, ErrClosed)
					return
				}

				// Mark this item as received
				receivedItems.Store(item, true)
				receivedCount.Add(1)
			}
		}()
	}

	// Start producers
	producerWg.Add(prodNum)
	for i := range prodNum {
		go func(producerID int) {
			defer producerWg.Done()

			base := producerID * itemsPerProd
			for j := range itemsPerProd {
				item := base + j
				err := rb.Write(item)
				assert.NoError(err)
				if err != nil {
					return
				}
			}
		}(i)
	}

	// Wait for all producers to finish
	producerWg.Wait()

	t.Log("Finished producing")

	// Close the buffer
	rb.Close()

	// Wait for all consumers to finish
	consumerWg.Wait()
	t.Log("Finished consuming")

	// Verify all items were received
	assert.Equal(uint64(totalItems), receivedCount.Load())

	// Verify all items were received exactly once
	missingItems := 0
	for i := range totalItems {
		if _, ok := receivedItems.Load(i); !ok {
			missingItems++
		}
	}
	assert.Zero(missingItems)

	// Report performance
	duration := time.Since(startTime)
	itemsPerSec := int(float64(totalItems) / duration.Seconds())
	t.Logf("Processed %d items in %v (%d items/sec)", totalItems, duration, itemsPerSec)
}

func Benchmark_RingBuffers(b *testing.B) {
	b.ReportAllocs()

	kinds := []BufferKind{BufferKindSPSC, BufferKindMPMC}
	capacities := []int{512, 1024, 4096}
	for _, kind := range kinds {
		kindStr := kind.String()

		for _, capacity := range capacities {
			capacityStr := strconv.Itoa(capacity)

			b.Run("WriteReadCycle-"+kindStr+"-"+capacityStr, func(b *testing.B) {
				benchWriteReadCycle(b, kind, capacity)
			})

			b.Run("WriteReadSteady-"+kindStr+"-"+capacityStr, func(b *testing.B) {
				benchWriteReadSteady(b, kind, capacity)
			})

			b.Run("WriteReadSteady-"+kindStr+"-"+capacityStr, func(b *testing.B) {
				benchWriteReadSteady(b, kind, capacity)
			})
		}
	}
}

func Benchmark_RingBuffers_Contention(b *testing.B) {
	capacity := 4096

	// SPSC
	kind := BufferKindSPSC
	b.Run("Mixed-"+kind.String(), func(b *testing.B) {
		benchContention(b, capacity, kind, 1, 1)
	})

	// MPMC
	kind = BufferKindMPMC
	b.Run("Mixed-"+kind.String(), func(b *testing.B) {
		benchContention(b, capacity, kind, 1, 1)
	})

	contentions := []int{2, 4, 8, 16}
	for _, cont := range contentions {
		contStr := strconv.Itoa(cont)

		b.Run("Read-"+kind.String()+"-"+contStr, func(b *testing.B) {
			benchContention(b, capacity, kind, 1, cont)
		})

		b.Run("Write-"+kind.String()+"-"+contStr, func(b *testing.B) {
			benchContention(b, capacity, kind, cont, 1)
		})
	}
}

func benchWriteReadCycle(b *testing.B, kind BufferKind, capacity int) {
	rb := NewRingBuffer[int](uint32(capacity), kind)

	cycles := (b.N + capacity - 1) / capacity
	remainder := b.N % capacity
	if remainder == 0 {
		remainder = capacity
	}

	b.ResetTimer()

	for cycleIdx := range cycles {
		itemsPerCycle := capacity
		if cycleIdx == cycles-1 {
			itemsPerCycle = remainder
		}

		// Fill the buffer
		for val := range itemsPerCycle {
			err := rb.Write(val)
			if err != nil {
				b.Logf("Write error: %v,", err)
				continue
			}
		}

		// Empty the buffer
		for range itemsPerCycle {
			_, err := rb.Read(b.Context())
			if err != nil {
				b.Logf("Read error: %v", err)
				continue
			}
		}
	}
}

func benchWriteReadSteady(b *testing.B, kind BufferKind, capacity int) {
	rb := NewRingBuffer[int](uint32(capacity), kind)

	val := 0
	for b.Loop() {
		if err := rb.Write(val); err != nil {
			b.Logf("Write error: %v,", err)
			continue
		}

		_, err := rb.Read(b.Context())
		if err != nil {
			b.Logf("Read error: %v", err)
			continue
		}

		val++
	}
}

func benchContention(b *testing.B, capacity int, bufferKind BufferKind, numWriters, numReaders int) {
	rb := NewRingBuffer[int](uint32(capacity), bufferKind)

	b.ResetTimer()

	// Multiple writers
	itemsPerWriter := b.N / numWriters
	writerRemainder := b.N % numWriters

	var written atomic.Uint64

	for w := range numWriters {
		items := itemsPerWriter
		if w == 0 {
			items += writerRemainder
		}

		go func(count int) {
			for i := range count {
				if err := rb.Write(i); err != nil {
					b.Errorf("write error: %v", err)
					return
				}
				written.Add(1)
			}
		}(items)
	}

	var wg sync.WaitGroup
	wg.Add(numReaders)

	ctx, cancelCtx := context.WithTimeout(b.Context(), 30*time.Second)
	defer cancelCtx()

	// Multiple readers
	itemsPerReader := b.N / numReaders
	readerRemainder := b.N % numReaders

	var hasError atomic.Bool
	hasError.Store(false)

	for r := range numReaders {
		items := itemsPerReader
		if r == 0 {
			items += readerRemainder
		}

		go func(target int) {
			defer wg.Done()
			count := 0

			for count < target {
				_, err := rb.Read(ctx)
				if err != nil {
					hasError.Store(true)
					b.Errorf("read error: %v", err)
					return
				}
				count++
			}
		}(items)
	}

	wg.Wait()

	if hasError.Load() {
		b.Logf("written %d over %d", written.Load(), b.N)
	}
}
