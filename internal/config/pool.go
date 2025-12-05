package config

import (
	"runtime"
	"time"
)

// Default configuration values for the pool.
const (
	DefaultPoolAutoScaleEnabled    = true
	DefaultPoolMinWorkers          = 1
	DefaultPoolInputQueueSize      = 512
	DefaultPoolOutputQueueSize     = 512
	DefaultPoolQueueDepthPerWorker = 64
	DefaultPoolScaleDownFactor     = 0.1
	DefaultPoolScaleDownBackoff    = 1.5
	DefaultPoolAutoScaleInterval   = 3 * time.Second
)

// DefaultPoolMaxWorkers returns the default maximum number of workers
// (number of CPUs).
func DefaultPoolMaxWorkers() int {
	return runtime.NumCPU()
}

// DefaultPoolInitialWorkers returns the default initial number of workers
// (half of CPUs, minimum 1).
func DefaultPoolInitialWorkers() int {
	return max(1, DefaultPoolMaxWorkers()/2)
}

// Pool represents the configuration for the worker pool.
type Pool struct {
	// AutoScaleEnabled states whether the worker pool should scale automatically.
	AutoScaleEnabled bool

	// MaxWorkers is the maximum number of workers.
	MaxWorkers int

	// MinWorkers is the minimum number of workers.
	MinWorkers int

	// InitialWorkers is the initial number of workers.
	InitialWorkers int

	// InputQueueSize is the size of the queue that holds messages to be processed
	// by the workers. It is basically the size of the buffer used to fan out the
	// messages to the workers.
	InputQueueSize int

	// OutputQueueSize is the size of the queue that holds messages which have been
	// processed by the workers. It is basically the size of the buffer used to fan in
	// the messages from the workers. It is NOT used by the egress stage.
	OutputQueueSize int

	// QueueDepthPerWorker is the target length of the task queue per worker.
	QueueDepthPerWorker int

	// ScaleDownFactor is the factor by which to scale down the number of workers.
	ScaleDownFactor float64
	// ScaleDownBackoff is the factor by which to increase the time to scale down.
	ScaleDownBackoff float64

	// AutoScaleInterval is the interval at which the auto scaler is triggered.
	AutoScaleInterval time.Duration
}

// NewPool returns the default configuration for the worker pool.
func NewPool() *Pool {
	return &Pool{
		AutoScaleEnabled: DefaultPoolAutoScaleEnabled,

		MaxWorkers:     DefaultPoolMaxWorkers(),
		MinWorkers:     DefaultPoolMinWorkers,
		InitialWorkers: DefaultPoolInitialWorkers(),

		InputQueueSize:  DefaultPoolInputQueueSize,
		OutputQueueSize: DefaultPoolOutputQueueSize,

		QueueDepthPerWorker: DefaultPoolQueueDepthPerWorker,

		ScaleDownFactor:  DefaultPoolScaleDownFactor,
		ScaleDownBackoff: DefaultPoolScaleDownBackoff,

		AutoScaleInterval: DefaultPoolAutoScaleInterval,
	}
}

// Validate validates the configuration.
func (p *Pool) Validate(ac *AnomalyCollector) {
	CheckNotNegative(ac, "MaxWorkers", &p.MaxWorkers, DefaultPoolMaxWorkers())

	CheckNotNegative(ac, "MinWorkers", &p.MinWorkers, DefaultPoolMinWorkers)
	CheckNotGreaterThan(ac, "MinWorkers", "MaxWorkers", &p.MinWorkers, p.MaxWorkers)

	CheckNotNegative(ac, "InitialWorkers", &p.InitialWorkers, DefaultPoolInitialWorkers())
	CheckNotLowerThan(ac, "InitialWorkers", "MinWorkers", &p.InitialWorkers, p.MinWorkers)
	CheckNotGreaterThan(ac, "InitialWorkers", "MaxWorkers", &p.InitialWorkers, p.MaxWorkers)

	CheckNotNegative(ac, "InputQueueSize", &p.InputQueueSize, DefaultPoolInputQueueSize)
	CheckNotNegative(ac, "OutputQueueSize", &p.OutputQueueSize, DefaultPoolOutputQueueSize)

	CheckNotNegative(ac, "QueueDepthPerWorker", &p.QueueDepthPerWorker, DefaultPoolQueueDepthPerWorker)

	CheckNotNegative(ac, "ScaleDownFactor", &p.ScaleDownFactor, DefaultPoolScaleDownFactor)
	CheckNotNegative(ac, "ScaleDownBackoff", &p.ScaleDownBackoff, DefaultPoolScaleDownBackoff)

	CheckNotNegative(ac, "AutoScaleInterval", &p.AutoScaleInterval, DefaultPoolAutoScaleInterval)
}
