package pool

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/FerroO2000/goccia/internal"
)

type scalerConfig struct {
	enabled             bool
	minWorkers          int
	maxWorkers          int
	queueDepthThreshold float64
	scaleDownFactor     float64
	scaleDownBackoff    float64
	interval            time.Duration
}

func newScalerConfig(poolCfg *Config) *scalerConfig {
	return &scalerConfig{
		enabled:             poolCfg.AutoScaleEnabled,
		maxWorkers:          poolCfg.MaxWorkers,
		minWorkers:          poolCfg.MinWorkers,
		queueDepthThreshold: float64(poolCfg.QueueDepthPerWorker),
		scaleDownFactor:     poolCfg.ScaleDownFactor,
		scaleDownBackoff:    poolCfg.ScaleDownBackoff,
		interval:            poolCfg.AutoScaleInterval,
	}
}

// Scaler is an utility struct for a worker pool
// that implements worker auto-scaling.
type Scaler struct {
	tel *internal.Telemetry

	cfg *scalerConfig

	consecuriveScaleDown int
	scaleDownAt          float64

	startCh    chan struct{}
	stopChList []chan struct{}

	currWorkers   atomic.Int32
	activeWorkers atomic.Int32

	pendingTasks atomic.Int64
}

// NewScaler returns a new auto-scaler instance.
func NewScaler(tel *internal.Telemetry, poolCfg *Config) *Scaler {
	cfg := newScalerConfig(poolCfg)

	return &Scaler{
		tel: tel,

		cfg: cfg,

		consecuriveScaleDown: 0,
		scaleDownAt:          1,

		startCh:    make(chan struct{}, cfg.maxWorkers),
		stopChList: make([]chan struct{}, 0, cfg.maxWorkers),
	}
}

func (s *Scaler) initMetrics() {
	s.tel.NewUpDownCounter("worker_pool_pending_tasks", func() int64 {
		return s.pendingTasks.Load()
	})

	s.tel.NewUpDownCounter("worker_pool_active_workers", func() int64 {
		return int64(s.activeWorkers.Load())
	})
}

// Init initializes the auto-scaler.
func (s *Scaler) Init(ctx context.Context, initialWorkers int) {
	for range s.cfg.maxWorkers {
		s.stopChList = append(s.stopChList, make(chan struct{}, 1))
	}

	for range initialWorkers {
		s.sendStart(ctx)
	}

	s.currWorkers.Store(int32(initialWorkers))

	s.initMetrics()
}

// Run starts the auto-scaler.
func (s *Scaler) Run(ctx context.Context) {
	if !s.cfg.enabled {
		return
	}

	ticker := time.NewTicker(s.cfg.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			s.evaluateAndScale(ctx)
		}
	}
}

func (s *Scaler) evaluateAndScale(ctx context.Context) {
	currWorkers := int(s.currWorkers.Load())
	pendingTasks := int(s.pendingTasks.Load())
	activeWorkers := int(s.activeWorkers.Load())

	// Calculate queue depth per worker
	queueDepthPerWorker := float64(pendingTasks) / float64(currWorkers)

	s.tel.LogInfo("auto-scaling metrics",
		"current_workers", currWorkers,
		"active_workers", activeWorkers,
		"pending_tasks", pendingTasks,
		"queue_depth_per_worker", queueDepthPerWorker,
	)

	// Scale up if queue depth per worker is higher than target
	if queueDepthPerWorker > s.cfg.queueDepthThreshold {
		// Calculate how many workers to add based on queue depth
		workersToAdd := max(int(math.Ceil(float64(pendingTasks)/s.cfg.queueDepthThreshold)), 1)
		targetWorkers := min(currWorkers+workersToAdd, s.cfg.maxWorkers)

		if targetWorkers > currWorkers {
			s.tel.LogInfo("scaling up", "from", currWorkers, "to", targetWorkers)
			s.scaleWorkers(ctx, int(targetWorkers))
		}

		s.resetScaleDownTiming()

		return
	}

	// Scale down if we have more than min workers and there are fewer pending tasks than workers
	if currWorkers > s.cfg.minWorkers && pendingTasks < currWorkers {
		// Check if it is the right time to scale down
		if !s.checkScaleDownTiming() {
			return
		}

		// Remove workers
		workersToRemove := max(int(math.Ceil(float64(currWorkers)*s.cfg.scaleDownFactor)), 1)
		targetWorkers := max(currWorkers-workersToRemove, s.cfg.minWorkers)

		if targetWorkers < currWorkers {
			s.tel.LogInfo("scaling down", "from", currWorkers, "to", targetWorkers)
			s.scaleWorkers(ctx, int(targetWorkers))
		}
	}
}

func (s *Scaler) resetScaleDownTiming() {
	s.consecuriveScaleDown = 0
	s.scaleDownAt = 1
}

// checkScaleDownTiming states if it is the right time to scale down
// and updates the necessary parameters
func (s *Scaler) checkScaleDownTiming() bool {
	s.consecuriveScaleDown++

	// Check if it is the right time to scale down
	if float64(s.consecuriveScaleDown) < s.scaleDownAt {
		return false
	}

	// Exponentially increase the time to scale down so
	// it gets harder to scale down when multiple consecutive
	// scales down are triggered (exponential backoff)
	nextTime := s.scaleDownAt * s.cfg.scaleDownBackoff
	s.scaleDownAt = min(nextTime, 15)

	return true
}

func (s *Scaler) sendStart(ctx context.Context) {
	select {
	case <-ctx.Done():
	case s.startCh <- struct{}{}:
	}
}

func (s *Scaler) sendStop(ctx context.Context, id int) {
	if id > s.cfg.maxWorkers {
		return
	}

	select {
	case <-ctx.Done():
	case s.stopChList[id] <- struct{}{}:
	default:
	}
}

func (s *Scaler) scaleWorkers(ctx context.Context, targetCount int) {
	currWorkerCount := int(s.currWorkers.Swap(int32(targetCount)))
	delta := targetCount - currWorkerCount

	if delta == 0 {
		return
	}

	// Check if it has to scale up worker
	if delta > 0 {
		for range delta {
			s.sendStart(ctx)
		}

		return
	}

	// Scale down
	for i := currWorkerCount - 1; i >= targetCount; i-- {
		s.sendStop(ctx, i)
	}
}

// Close closes the auto-scaler.
func (s *Scaler) Close() {
	for _, stopCh := range s.stopChList {
		close(stopCh)
	}

	close(s.startCh)
}

// NotifyWorkerStart notifies the scaler that a new worker has started.
func (s *Scaler) NotifyWorkerStart() int {
	workerID := int(s.activeWorkers.Add(1)) - 1
	return min(workerID, s.cfg.maxWorkers-1)
}

// NotifyWorkerStop notifies the scaler that a worker has stopped.
func (s *Scaler) NotifyWorkerStop() {
	s.activeWorkers.Add(-1)
}

// NotifyTaskAdded notifies the scaler that
// a new task has been added to the worker pool.
func (s *Scaler) NotifyTaskAdded() {
	s.pendingTasks.Add(1)
}

// NotifyTaskCompleted notifies the scaler that
// a task has been completed.
func (s *Scaler) NotifyTaskCompleted() {
	s.pendingTasks.Add(-1)
}

// GetStopCh returns the stop channel used to stop a
// specific worker.
func (s *Scaler) GetStopCh(workerID int) <-chan struct{} {
	if workerID >= s.cfg.maxWorkers {
		return nil
	}

	return s.stopChList[workerID]
}

// GetStartCh returns the start channel used to trigger
// the creation of a new workers.
func (s *Scaler) GetStartCh() <-chan struct{} {
	return s.startCh
}
