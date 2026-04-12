package stage

import (
	"context"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

// Stage defines the interface for a generic stage.
type Stage interface {
	Kind() Kind

	Name() string

	Telemetry() *telemetry.Telemetry

	Config() config.Config

	// Init initializes the stage.
	Init(ctx context.Context) error

	// Run runs the stage.
	Run(ctx context.Context)

	// Close closes (forever) the stage.
	Close()

	// Inputs returns a slice of pointers to input connectors.
	Inputs() []uintptr

	// Outputs returns a slice of pointers to output connectors.
	Outputs() []uintptr
}

type Kind = string

const (
	KindIngress   Kind = "ingress"
	KindProcessor Kind = "processor"
	KindEgress    Kind = "egress"
)

type BaseStage[Cfg config.Config] struct {
	kind Kind
	name string

	tel *telemetry.Telemetry

	cfg Cfg
}

func newBaseStage[Cfg config.Config](kind Kind, name string, cfg Cfg) *BaseStage[Cfg] {

	return &BaseStage[Cfg]{
		kind: kind,
		name: name,

		tel: telemetry.NewTelemetry(kind, name),

		cfg: cfg,
	}
}

func NewIngressStage[Cfg config.Config](name string, cfg Cfg) *BaseStage[Cfg] {
	return newBaseStage(KindIngress, name, cfg)
}

func NewProcessorStage[Cfg config.Config](name string, cfg Cfg) *BaseStage[Cfg] {
	return newBaseStage(KindProcessor, name, cfg)
}

func NewEgressStage[Cfg config.Config](name string, cfg Cfg) *BaseStage[Cfg] {
	return newBaseStage(KindEgress, name, cfg)
}

func (s *BaseStage[Cfg]) Kind() Kind {
	return s.kind
}

func (s *BaseStage[Cfg]) Name() string {
	return s.name
}

func (s *BaseStage[Cfg]) Telemetry() *telemetry.Telemetry {
	return s.tel
}

func (s *BaseStage[Cfg]) Config() Cfg {
	return s.cfg
}

func (s *BaseStage[Cfg]) Init(_ context.Context) error {
	s.tel.LogDebug("validating configuration")
	configValidator := config.NewValidator(s.tel)
	configValidator.Validate(s.cfg)
	s.tel.LogDebug("validated configuration")

	return nil
}
