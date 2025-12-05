package config

import (
	"github.com/FerroO2000/goccia/internal"
)

// Validator is an utility struct for validating a configuration.
type Validator struct {
	tel *internal.Telemetry

	anomalyCollector *AnomalyCollector
}

// NewValidator returns a new validator.
func NewValidator(tel *internal.Telemetry) *Validator {
	return &Validator{
		tel: tel,

		anomalyCollector: newAnomalyCollector(),
	}
}

// Validate validates the given configuration.
func (m *Validator) Validate(config Config) {
	config.Validate(m.anomalyCollector)

	for anomaly := range m.anomalyCollector.iter() {
		m.handleAnomaly(anomaly)
	}
}

func (m *Validator) handleAnomaly(an *anomaly) {
	m.tel.LogWarn("config anomaly",
		"field", an.field, "reason", an.reason,
		"actual", an.actual, "fallback", an.fallback)
}
