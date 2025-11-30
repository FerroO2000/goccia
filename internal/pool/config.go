// Package pool contains the inner components for implementing a worker pool.
package pool

import (
	"runtime"
	"time"
)

// Config is the configuration for the worker pool.
type Config struct {
	// AutoScaleEnabled states whether the worker pool should scale automatically.
	//
	// Default: true
	AutoScaleEnabled bool

	// InitialWorkers is the initial number of workers.
	//
	// Default: 1
	InitialWorkers int

	// MinWorkers is the minimum number of workers.
	//
	// Default: 1
	MinWorkers int
	// MaxWorkers is the maximum number of workers.
	//
	// Default: number of CPUs
	MaxWorkers int

	// InputQueueSize is the size of the queue that holds messages to be processed
	// by the workers. It is basically the size of the buffer used to fan out the
	// messages to the workers.
	//
	// Default: 512
	InputQueueSize int

	// OutputQueueSize is the size of the queue that holds messages which have been
	// processed by the workers. It is basically the size of the buffer used to fan in
	// the messages from the workers. It is NOT used by the egress stage.
	//
	// Default: 512
	OutputQueueSize int

	// QueueDepthPerWorker is the target length of the task queue per worker.
	//
	// Default: 64
	QueueDepthPerWorker int

	// ScaleDownFactor is the factor by which to scale down the number of workers.
	//
	// Default: 0.1
	ScaleDownFactor float64
	// ScaleDownBackoff is the factor by which to increase the time to scale down.
	//
	// Default: 1.5
	ScaleDownBackoff float64

	// AutoScaleInterval is the interval at which the auto scaler is triggered.
	//
	// Default: 3 seconds
	AutoScaleInterval time.Duration
}

// DefaultConfig returns the default configuration for the worker pool.
func DefaultConfig() *Config {
	return &Config{
		AutoScaleEnabled:    true,
		InitialWorkers:      max(1, runtime.NumCPU()/2),
		MinWorkers:          1,
		MaxWorkers:          runtime.NumCPU(),
		InputQueueSize:      512,
		OutputQueueSize:     512,
		QueueDepthPerWorker: 64,
		ScaleDownFactor:     0.1,
		ScaleDownBackoff:    1.5,
		AutoScaleInterval:   3 * time.Second,
	}
}
