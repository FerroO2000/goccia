package rob

import (
	"github.com/FerroO2000/goccia/internal/config"
)

// Default configuration values for the re-order buffer.
const (
	DefaultMaxSeqNum           = 255
	DefaultPrimaryBufferSize   = 128
	DefaultAuxiliaryBufferSize = 128
	DefaultFlushTreshold       = 0.3
	DefaultTimeSmootherEnabled = true
	DefaultEstimatorAlpha      = 0.8
	DefaultEstimatorBeta       = 0.5
)

// Config is the configuration for the re-order buffer structure [ROB].
type Config struct {
	// MaxSeqNum is the maximum possible sequence number.
	MaxSeqNum uint64

	// PrimaryBufferSize is the size of the primary buffer.
	PrimaryBufferSize uint64

	// AuxiliaryBufferSize is the size of the auxiliary buffer.
	AuxiliaryBufferSize uint64

	// FlushTreshold is the value of the fullness of the auxiliary buffer
	// needed for flushing the primary buffer.
	FlushTreshold float64

	// TimeSmootherEnabled states whether the time smoother is enabled or not.
	TimeSmootherEnabled bool

	// EstimatorAlpha is the value for the alpha parameter for
	// the double exponential estimator (data smoothing factor).
	// It must be between 0 and 1.
	EstimatorAlpha float64

	// EstimatorBeta is the value for the beta parameter for
	// the double exponential estimator (trend smoothing factor).
	// It must be between 0 and 1.
	EstimatorBeta float64
}

// NewConfig returns a new ROB configuration with default values.
func NewConfig() *Config {
	return &Config{
		MaxSeqNum:           DefaultMaxSeqNum,
		PrimaryBufferSize:   DefaultPrimaryBufferSize,
		AuxiliaryBufferSize: DefaultAuxiliaryBufferSize,
		FlushTreshold:       DefaultFlushTreshold,
		TimeSmootherEnabled: DefaultTimeSmootherEnabled,
		EstimatorAlpha:      DefaultEstimatorAlpha,
		EstimatorBeta:       DefaultEstimatorBeta,
	}
}

// Validate checks the configuration.
func (c *Config) Validate(ac *config.AnomalyCollector) {
	config.CheckNotZero(ac, "MaxSeqNum", &c.MaxSeqNum, DefaultMaxSeqNum)

	config.CheckNotZero(ac, "PrimaryBufferSize", &c.PrimaryBufferSize, DefaultPrimaryBufferSize)
	config.CheckNotGreaterThan(ac, "PrimaryBufferSize", "MaxSeqNum", &c.PrimaryBufferSize, c.MaxSeqNum+1)

	config.CheckNotZero(ac, "AuxiliaryBufferSize", &c.AuxiliaryBufferSize, DefaultAuxiliaryBufferSize)
	config.CheckNotGreaterThan(ac,
		"AuxiliaryBufferSize", "MaxSeqNum - PrimaryBufferSize",
		&c.AuxiliaryBufferSize, c.MaxSeqNum+1-c.PrimaryBufferSize,
	)

	config.CheckNotNegative(ac, "FlushTreshold", &c.FlushTreshold, DefaultFlushTreshold)
	config.CheckNotZero(ac, "FlushTreshold", &c.FlushTreshold, DefaultFlushTreshold)

	config.CheckNotNegative(ac, "EstimatorAlpha", &c.EstimatorAlpha, DefaultEstimatorAlpha)
	config.CheckNotZero(ac, "EstimatorAlpha", &c.EstimatorAlpha, DefaultEstimatorAlpha)
	config.CheckNotGreaterThan(ac, "EstimatorAlpha", "1", &c.EstimatorAlpha, 1.0)

	config.CheckNotNegative(ac, "EstimatorBeta", &c.EstimatorBeta, DefaultEstimatorBeta)
	config.CheckNotZero(ac, "EstimatorBeta", &c.EstimatorBeta, DefaultEstimatorBeta)
	config.CheckNotGreaterThan(ac, "EstimatorBeta", "1", &c.EstimatorBeta, 1.0)
}
