// Package pool contains the structs used by the worker pools across the library.
package pool

import (
	"github.com/FerroO2000/goccia/internal/telemetry"
)

// BaseWorker is the base struct for a worker that can be embedded.
type BaseWorker struct {
	Tel *telemetry.Telemetry
}

// SetTelemetry sets the telemetry for the worker.
func (w *BaseWorker) SetTelemetry(tel *telemetry.Telemetry) {
	w.Tel = tel
}
