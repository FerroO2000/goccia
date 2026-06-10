package ingress

import (
	"errors"

	"github.com/FerroO2000/goccia/internal/config"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Default configuration values for the generic ingress stages.
const (
	DefaultGenericConfigName = "generic"
)

// GenericConfig structs contains the configuration for a generic ingress stage.
type GenericConfig struct {
	Name string
}

// NewGenericConfig returns the default configuration for a generic ingress stage.
func NewGenericConfig(name string) *GenericConfig {
	return &GenericConfig{
		Name: name,
	}
}

// Validate checks the configuration.
func (c *GenericConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotEmpty(ac, "Name", &c.Name, DefaultGenericConfigName)
}

// ─── Error ──────────────────────────────────────────────────────────────────|

// ErrQuitLoop is used to quit the main/sub loop.
var ErrQuitLoop = errors.New("quit loop")
