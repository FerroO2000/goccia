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

//
//
//
//
//
//

type ProcessorStage[In, Out msgBody, WArgs any, W worker.Processor[WArgs, In, Out]] struct {
	runner[WArgs, W]
}

func NewProcessorStageSingle[In, Out msgBody, WArgs any, W worker.Processor[WArgs, In, Out]](
	name string, inConn msgConn[In], outConn msgConn[Out], workerMaker func() W, workerArgs WArgs,
) *ProcessorStage[In, Out, WArgs, W] {

	tel := telemetry.NewTelemetry(KindProcessor, name)

	input := newBaseInput(inConn)
	output := newBaseOutput(outConn)

	workerRunnerFactory := newProcessorWorkerRunnerFactory(input, output, workerMaker, metrics.NewProcessorStage())

	return &ProcessorStage[In, Out, WArgs, W]{
		runner: newRunnerSingle(tel, workerArgs, workerRunnerFactory),
	}
}

func NewProcessorStagePool[In, Out msgBody, WArgs any, W worker.Processor[WArgs, In, Out]](
	name string, inConn msgConn[In], outConn msgConn[Out], workerMaker func() W, workerArgs WArgs, cfg *config.Pool,
) *ProcessorStage[In, Out, WArgs, W] {

	tel := telemetry.NewTelemetry(KindProcessor, name)

	input := newFanOut(inConn, uint64(cfg.InputQueueSize))
	output := newFanIn(outConn, uint64(cfg.OutputQueueSize))

	workerRunnerFactory := newProcessorWorkerRunnerFactory(input, output, workerMaker, metrics.NewProcessorStage())

	return &ProcessorStage[In, Out, WArgs, W]{
		runner: newRunnerPool(tel, workerArgs, workerRunnerFactory, cfg),
	}
}

type EgressStage[In msgBody, WArgs any, W worker.Egress[WArgs, In]] struct {
	runner[WArgs, W]
}

func NewEgressStageSingle[In msgBody, WArgs any, W worker.Egress[WArgs, In]](
	name string, inConn msgConn[In], workerMaker func() W, workerArgs WArgs,
) *EgressStage[In, WArgs, W] {

	tel := telemetry.NewTelemetry(KindEgress, name)

	input := newBaseInput(inConn)

	workerRunnerFactory := newEgressWorkerRunnerFactory(input, workerMaker, metrics.NewEgressStage())

	return &EgressStage[In, WArgs, W]{
		runner: newRunnerSingle(tel, workerArgs, workerRunnerFactory),
	}
}

func NewEgressStagePool[In msgBody, WArgs any, W worker.Egress[WArgs, In]](
	name string, inConn msgConn[In], workerMaker func() W, workerArgs WArgs, cfg *config.Pool,
) *EgressStage[In, WArgs, W] {

	tel := telemetry.NewTelemetry(KindEgress, name)

	input := newFanOut(inConn, uint64(cfg.InputQueueSize))

	workerRunnerFactory := newEgressWorkerRunnerFactory(input, workerMaker, metrics.NewEgressStage())

	return &EgressStage[In, WArgs, W]{
		runner: newRunnerPool(tel, workerArgs, workerRunnerFactory, cfg),
	}
}
