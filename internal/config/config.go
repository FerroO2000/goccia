// Package config contains utility structs/functions and types
// for validating the configurations across the library.
package config

// Config defines the minimal interface for a configuration
// in order to be validated.
type Config interface {
	// Validate checks the configuration.
	Validate(ac *AnomalyCollector)
}

// WithStage defines the interface for a stage configuration (processor/egress).
type WithStage interface {
	Config

	// GetStage returns the stage configuration.
	GetStage() *Stage
}

// Base is the base struct used for processor/egress configurations.
// It implements the WithStage interface.
type Base struct {
	Stage *Stage
}

// NewBase returns a new base configuration struct.
func NewBase(runningMod StageRunningMode) *Base {
	return &Base{
		Stage: NewStage(runningMod),
	}
}

// Validate checks the configuration of the stage.
func (b *Base) Validate(ac *AnomalyCollector) {
	b.Stage.Validate(ac)
}

// GetStage returns the stage configuration.
func (b *Base) GetStage() *Stage {
	return b.Stage
}
