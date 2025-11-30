// Package stage contains the configuration for a stage.
package stage

import "github.com/FerroO2000/goccia/internal/pool"

// RunningMode represents the running mode of a stage.
type RunningMode = uint8

const (
	// RunningModeSingle enforces a single-threaded running mode.
	RunningModeSingle RunningMode = iota

	// RunningModePool enforces a multi-threaded running mode (worker pool).
	RunningModePool
)

// Config is the configuration for a stage.
type Config struct {
	// RunningMode is the running mode of the stage.
	RunningMode RunningMode

	// Pool is the configuration for the worker pool.
	// It is only used when RunningMode is RunningModePool.
	Pool *pool.Config
}

// DefaultConfig returns the default configuration for a stage
// based on the running mode.
func DefaultConfig(runningMode RunningMode) *Config {
	switch runningMode {
	case RunningModeSingle:
		return &Config{
			RunningMode: RunningModeSingle,
		}

	case RunningModePool:
		return &Config{
			RunningMode: RunningModePool,
			Pool:        pool.DefaultConfig(),
		}

	default:
		return nil
	}
}
