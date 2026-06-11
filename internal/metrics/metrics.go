package metrics

import "github.com/FerroO2000/goccia/internal/telemetry"

type Metrics interface {
	InitMetrics(tel *telemetry.Telemetry) error
}

type EmptyMetrics struct{}

func NewEmptyMetrics() *EmptyMetrics {
	return &EmptyMetrics{}
}

func (m *EmptyMetrics) InitMetrics(_ *telemetry.Telemetry) error {
	return nil
}
