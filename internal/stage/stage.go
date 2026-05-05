package stage

import (
	"context"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage/metrics"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/FerroO2000/goccia/internal/worker"
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

func (s *BaseStage[Cfg]) initConfig() {
	s.tel.LogDebug("validating configuration")
	configValidator := config.NewValidator(s.tel)
	configValidator.Validate(s.cfg)
	s.tel.LogDebug("validated configuration")
}

type ProcessorStage[In, Out msgBody, WArgs any, W worker.Processor[WArgs, In, Out], Cfg config.Config] struct {
	*BaseStage[Cfg]
	Runner[WArgs, W]
}

func (ps *ProcessorStage[In, Out, WArgs, W, Cfg]) Init(ctx context.Context) error {
	ps.BaseStage.initConfig()
	return ps.Runner.Init(ctx)
}

func NewProcessorStageSingle[In, Out msgBody, WArgs any, W worker.Processor[WArgs, In, Out], Cfg config.Config](
	name string, inConn msgConn[In], outConn msgConn[Out], workerMaker func() W, workerArgs WArgs, cfg Cfg,
) *ProcessorStage[In, Out, WArgs, W, Cfg] {

	tel := telemetry.NewTelemetry(KindProcessor, name)

	input := newBaseInput(inConn)
	output := newBaseOutput(outConn)

	workerRunnerFactory := newProcessorWorkerRunnerFactory(input, output, workerMaker, metrics.NewProcessorStage())

	return &ProcessorStage[In, Out, WArgs, W, Cfg]{
		BaseStage: newBaseStage(KindProcessor, name, cfg),
		Runner:    newRunnerSingle(tel, workerArgs, workerRunnerFactory),
	}
}

func NewProcessorStagePool[In, Out msgBody, WArgs any, W worker.Processor[WArgs, In, Out], Cfg config.WithStage](
	name string, inConn msgConn[In], outConn msgConn[Out], workerMaker func() W, workerArgs WArgs, cfg Cfg,
) *ProcessorStage[In, Out, WArgs, W, Cfg] {

	tel := telemetry.NewTelemetry(KindProcessor, name)

	poolCfg := cfg.GetStage().Pool

	input := newFanOut(inConn, uint64(poolCfg.InputQueueSize))
	output := newFanIn(outConn, uint64(poolCfg.OutputQueueSize))

	workerRunnerFactory := newProcessorWorkerRunnerFactory(input, output, workerMaker, metrics.NewProcessorStage())

	return &ProcessorStage[In, Out, WArgs, W, Cfg]{
		BaseStage: newBaseStage(KindProcessor, name, cfg),
		Runner:    newRunnerPool(tel, workerArgs, workerRunnerFactory, poolCfg),
	}
}

type EgressStage[In msgBody, WArgs any, W worker.Egress[WArgs, In], Cfg config.Config] struct {
	*BaseStage[Cfg]
	Runner[WArgs, W]
}

func (ps *EgressStage[In, WArgs, W, Cfg]) Init(ctx context.Context) error {
	ps.BaseStage.initConfig()
	return ps.Runner.Init(ctx)
}

func NewEgressStageSingle[In msgBody, WArgs any, W worker.Egress[WArgs, In], Cfg config.Config](
	name string, inConn msgConn[In], workerMaker func() W, workerArgs WArgs, cfg Cfg,
) *EgressStage[In, WArgs, W, Cfg] {

	tel := telemetry.NewTelemetry(KindEgress, name)

	input := newBaseInput(inConn)

	workerRunnerFactory := newEgressWorkerRunnerFactory(input, workerMaker, metrics.NewEgressStage())

	return &EgressStage[In, WArgs, W, Cfg]{
		BaseStage: newBaseStage(KindEgress, name, cfg),
		Runner:    newRunnerSingle(tel, workerArgs, workerRunnerFactory),
	}
}

func NewEgressStagePool[In msgBody, WArgs any, W worker.Egress[WArgs, In], Cfg config.WithStage](
	name string, inConn msgConn[In], workerMaker func() W, workerArgs WArgs, cfg Cfg,
) *EgressStage[In, WArgs, W, Cfg] {

	tel := telemetry.NewTelemetry(KindEgress, name)

	poolCfg := cfg.GetStage().Pool
	input := newFanOut(inConn, uint64(poolCfg.InputQueueSize))

	workerRunnerFactory := newEgressWorkerRunnerFactory(input, workerMaker, metrics.NewEgressStage())

	return &EgressStage[In, WArgs, W, Cfg]{
		BaseStage: newBaseStage(KindEgress, name, cfg),
		Runner:    newRunnerPool(tel, workerArgs, workerRunnerFactory, poolCfg),
	}
}
