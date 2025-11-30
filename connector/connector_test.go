package connector

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	bufferCapacity          = 2048
	numProducers            = 8
	numConsumers            = 8
	itemsPerProducer        = 1_000_000
	benchmarkBufferCapacity = 2048
)

func getConnectorFormKind[T any](connKind string, size uint64) Connector[T] {
	var connector Connector[T]
	switch connKind {
	case "channel":
		connector = NewChannel[T](size)
	case "ring_buffer":
		connector = newRingBuffer[T](uint32(size))
	}
	return connector
}

func Test_Connector_MultipleProducersConsumers(t *testing.T) {
	connKinds := []string{"channel"}
	for _, connKind := range connKinds {
		t.Run(connKind, func(t *testing.T) {
			connector := getConnectorFormKind[int](connKind, uint64(bufferCapacity))

			testMultipleProducersConsumers(t, connector, numProducers, numConsumers, itemsPerProducer)
		})
	}
}

func testMultipleProducersConsumers(t *testing.T, connector Connector[int], numProducers, numConsumers, itemsPerProducer int) {
	totalItems := numProducers * itemsPerProducer

	// Used to track received items
	var receivedItems sync.Map
	var receivedCount atomic.Uint64

	// WaitGroups to coordinate test completion
	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup

	// Start time for performance measurement
	startTime := time.Now()

	// Start consumers
	consumerWg.Add(numConsumers)
	for i := range numConsumers {
		go func(consumerID int) {
			defer consumerWg.Done()

			// Each consumer reads until the buffer is closed
			for {
				item, err := connector.Read()
				if err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Errorf("Consumer %d received unexpected error: %v", consumerID, err)
					}
					return
				}

				// Mark this item as received
				receivedItems.Store(item, true)
				receivedCount.Add(1)
			}
		}(i)
	}

	// Start producers
	producerWg.Add(numProducers)
	for i := range numProducers {
		go func(producerID int) {
			defer producerWg.Done()

			base := producerID * itemsPerProducer
			for j := 0; j < itemsPerProducer; j++ {
				item := base + j
				err := connector.Write(item)
				if err != nil {
					t.Errorf("Producer %d failed to write item %d: %v", producerID, item, err)
					return
				}
			}
		}(i)
	}

	// Wait for all producers to finish
	producerWg.Wait()

	t.Log("Finished producing")

	// Close the buffer
	connector.Close()

	// Wait for all consumers to finish
	consumerWg.Wait()
	t.Log("Finished consuming")

	// Verify all items were received exactly once
	missingItems := 0
	for i := range totalItems {
		if _, ok := receivedItems.Load(i); !ok {
			missingItems++
		}
	}

	if missingItems > 0 {
		t.Errorf("Missing %d items in total", missingItems)
	}

	// Report performance
	duration := time.Since(startTime)
	itemsPerSec := int(float64(totalItems) / duration.Seconds())
	t.Logf("Processed %d items in %v (%d items/sec)", totalItems, duration, itemsPerSec)
}

// func Benchmark_Connectors(b *testing.B) {
// 	b.ReportAllocs()

// 	connKinds := []string{"ring_buffer", "channel"}
// 	for _, connKind := range connKinds {
// 		b.Run("WriteSequential-"+connKind, func(b *testing.B) {
// 			benchmarkWriteSequential(b, connKind)
// 		})

// 		b.Run("WriteParallel-"+connKind, func(b *testing.B) {
// 			benchmarkWriteParallel(b, connKind)
// 		})

// 		b.Run("ReadSequential-"+connKind, func(b *testing.B) {
// 			benchmarkReadSequential(b, connKind)
// 		})

// 		b.Run("ReadParallel-"+connKind, func(b *testing.B) {
// 			benchmarkReadParallel(b, connKind)
// 		})
// 	}
// }

// func benchmarkWriteSequential(b *testing.B, connKind string) {
// 	type dummy struct {
// 		data []byte
// 	}

// 	connector := getConnectorFormKind[*dummy](connKind, benchmarkBufferCapacity)

// 	data := &dummy{
// 		data: make([]byte, 2048),
// 	}

// 	go func() {
// 		for {
// 			_, err := connector.Read()
// 			if err != nil {
// 				b.Logf("Read error: %v,", err)
// 				return
// 			}
// 		}
// 	}()

// 	b.ResetTimer()
// 	for range b.N {
// 		err := connector.Write(data)
// 		if err != nil {
// 			b.Logf("Write error: %v,", err)
// 			return
// 		}
// 	}
// }

// func benchmarkWriteParallel(b *testing.B, connKind string) {
// 	type dummy struct {
// 		data []byte
// 	}

// 	connector := getConnectorFormKind[*dummy](connKind, benchmarkBufferCapacity)

// 	data := &dummy{
// 		data: make([]byte, 2048),
// 	}

// 	go func() {
// 		for {
// 			_, err := connector.Read()
// 			if err != nil {
// 				b.Logf("Read error: %v,", err)
// 				return
// 			}
// 		}
// 	}()

// 	b.ResetTimer()
// 	b.RunParallel(func(pb *testing.PB) {
// 		for pb.Next() {
// 			err := connector.Write(data)
// 			if err != nil {
// 				b.Logf("Write error: %v,", err)
// 				return
// 			}
// 		}
// 	})
// }

// func benchmarkReadSequential(b *testing.B, connKind string) {
// 	type dummy struct {
// 		data []byte
// 	}

// 	connector := getConnectorFormKind[*dummy](connKind, benchmarkBufferCapacity)

// 	data := &dummy{
// 		data: make([]byte, 2048),
// 	}

// 	go func() {
// 		for range b.N {
// 			err := connector.Write(data)
// 			if err != nil {
// 				b.Logf("Write error: %v,", err)
// 				return
// 			}
// 		}
// 	}()

// 	b.ResetTimer()
// 	for range b.N {
// 		_, err := connector.Read()
// 		if err != nil {
// 			b.Logf("Read error: %v", err)
// 			return
// 		}
// 	}
// }

// func benchmarkReadParallel(b *testing.B, connKind string) {
// 	type dummy struct {
// 		data []byte
// 	}

// 	connector := getConnectorFormKind[*dummy](connKind, benchmarkBufferCapacity)

// 	data := &dummy{
// 		data: make([]byte, 2048),
// 	}

// 	go func() {
// 		for range b.N {
// 			err := connector.Write(data)
// 			if err != nil {
// 				b.Logf("Write error: %v,", err)
// 				return
// 			}
// 		}
// 	}()

// 	b.ResetTimer()
// 	b.RunParallel(func(pb *testing.PB) {
// 		for pb.Next() {
// 			_, err := connector.Read()
// 			if err != nil {
// 				b.Logf("Read error: %v", err)
// 				return
// 			}
// 		}
// 	})
// }
