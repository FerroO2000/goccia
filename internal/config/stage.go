package config

// StageRunningMode represents the running mode of a stage.
type StageRunningMode = uint8

const (
	// StageRunningModeSingle enforces a single-threaded running mode.
	StageRunningModeSingle StageRunningMode = iota

	// StageRunningModePool enforces a multi-threaded running mode (worker pool).
	StageRunningModePool
)

// Stage represents the configuration for a stage.
type Stage struct {
	// RunningMode is the running mode of the stage.
	RunningMode StageRunningMode

	// Pool is the configuration for the worker pool.
	// It is only used when RunningMode is RunningModePool.
	Pool *Pool
}

// NewStage returns the default configuration for a stage
// based on the running mode.
func NewStage(runningMode StageRunningMode) *Stage {
	cfg := &Stage{
		RunningMode: runningMode,
	}

	if runningMode == StageRunningModePool {
		cfg.Pool = NewPool()
	}

	return cfg
}

// Validate checks the configuration.
func (s *Stage) Validate(ac *AnomalyCollector) {
	if s.RunningMode == StageRunningModePool {
		s.Pool.Validate(ac)
	}
}
