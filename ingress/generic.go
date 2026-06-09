package ingress

import (
	"errors"

	"github.com/FerroO2000/goccia/internal/config"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

const (
	DefaultGenericConfigName = "generic"
)

type GenericConfig struct {
	Name string
}

func NewGenericConfig(name string) *GenericConfig {
	return &GenericConfig{
		Name: name,
	}
}

func (c *GenericConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotEmpty(ac, "Name", &c.Name, DefaultGenericConfigName)
}

// ─── Error ──────────────────────────────────────────────────────────────────|

var ErrQuitLoop = errors.New("quit loop")
