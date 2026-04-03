// Package telemetry provides OpenTelemetry utilities
// for managing OpenTelemetry traces, logs, and metrics.
package telemetry

import "go.opentelemetry.io/otel/attribute"

// Telemetry struct holds the OpenTelemetry logger, tracer and meter.
type Telemetry struct {
	*logger
	*tracer
	*meter
}

// NewTelemetry creates a new Telemetry instance.
func NewTelemetry(stageKind, stageName string) *Telemetry {
	attributes := []attribute.KeyValue{
		attribute.String("stage.kind", stageKind),
		attribute.String("stage.name", stageName),
	}

	return &Telemetry{
		logger: newLogger(attributes),
		tracer: newTracer(attributes),
		meter:  newMeter(attributes),
	}
}
