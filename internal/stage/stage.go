package stage

import (
	"context"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage/metrics"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/FerroO2000/goccia/internal/worker"
)

type Kind = string

const (
	KindIngress   Kind = "ingress"
	KindProcessor Kind = "processor"
	KindEgress    Kind = "egress"
)

// Stage defines the interface for a generic stage.
type Stage interface {
	Kind() Kind

	Name() string

	Telemetry() *telemetry.Telemetry

	// Init initializes the stage.
	Init(ctx context.Context) error

	// Run runs the stage.
	Run(ctx context.Context)

	// Close closes (forever) the stage.
	Close(ctx context.Context)

	// Inputs returns a slice of pointers to input connectors.
	Inputs() []uintptr

	// Outputs returns a slice of pointers to output connectors.
	Outputs() []uintptr
}

type BaseStage[Cfg config.Config, IArgs any] struct {
	tel *telemetry.Telemetry

	kind Kind
	name string

	runner Runner[IArgs]

	cfg Cfg
}

func newBaseStage[Cfg config.Config, IArgs any](
	kind Kind, name string, runner Runner[IArgs], cfg Cfg,
) *BaseStage[Cfg, IArgs] {

	return &BaseStage[Cfg, IArgs]{
		tel: telemetry.NewTelemetry(kind, name),

		kind: kind,
		name: name,

		runner: runner,

		cfg: cfg,
	}
}

func (s *BaseStage[Cfg, IArgs]) Telemetry() *telemetry.Telemetry {
	return s.tel
}

func (s *BaseStage[Cfg, IArgs]) Kind() Kind {
	return s.kind
}

func (s *BaseStage[Cfg, IArgs]) Name() string {
	return s.name
}

func (s *BaseStage[Cfg, IArgs]) Config() Cfg {
	return s.cfg
}

func (s *BaseStage[Cfg, IArgs]) InitConfig() {
	s.tel.LogDebug("validating configuration")
	configValidator := config.NewValidator(s.tel)
	configValidator.Validate(s.cfg)
	s.tel.LogDebug("validated configuration")
}

func (s *BaseStage[Cfg, IArgs]) InitWithArgs(ctx context.Context, initArgs IArgs) error {
	s.InitConfig()

	s.runner.SetTelemetry(s.tel)
	return s.runner.Init(ctx, initArgs)
}

func (s *BaseStage[Cfg, IArgs]) Init(ctx context.Context) error {
	var zeroInitArgs IArgs
	return s.InitWithArgs(ctx, zeroInitArgs)
}

func (s *BaseStage[Cfg, IArgs]) Run(ctx context.Context) {
	s.runner.Run(ctx)
}

func (s *BaseStage[Cfg, IArgs]) Close(ctx context.Context) {
	s.runner.Close(ctx)
}

func (s *BaseStage[Cfg, IArgs]) Inputs() []uintptr {
	return s.runner.Inputs()
}

func (s *BaseStage[Cfg, IArgs]) Outputs() []uintptr {
	return s.runner.Outputs()
}

type IngressStage[In msgBody, IArgs any, Cfg config.Config] struct {
	*BaseStage[Cfg, IArgs]
}

func NewIngressStageFromRunner[In msgBody, IArgs any, Cfg config.Config](
	name string, runner Runner[IArgs], cfg Cfg,
) *IngressStage[In, IArgs, Cfg] {

	return &IngressStage[In, IArgs, Cfg]{
		BaseStage: newBaseStage(KindIngress, name, runner, cfg),
	}
}

type ProcessorStage[In, Out msgBody, IArgs any, Cfg config.Config] struct {
	*BaseStage[Cfg, IArgs]
}

func NewProcessorStage[In, Out msgBody, IArgs any, W worker.Processor[IArgs, In, Out], Cfg stageConfig](
	name string, inConn msgConn[In], outConn msgConn[Out], workerMaker func() W, cfg Cfg,
) *ProcessorStage[In, Out, IArgs, Cfg] {

	stageCfg := cfg.GetStage()

	input := newInput(inConn, stageCfg)
	output := newOutput(outConn, stageCfg)

	workerRunnerFactory := newProcessorWorkerRunnerFactory(input, output, workerMaker, metrics.NewProcessorStage())
	runner := newRunner(workerRunnerFactory, stageCfg)

	return NewProcessorStageFromRunner[In, Out](name, runner, cfg)
}

func NewProcessorStageFromRunner[In, Out msgBody, IArgs any, Cfg config.Config](
	name string, runner Runner[IArgs], cfg Cfg,
) *ProcessorStage[In, Out, IArgs, Cfg] {

	return &ProcessorStage[In, Out, IArgs, Cfg]{
		BaseStage: newBaseStage(KindProcessor, name, runner, cfg),
	}
}

type EgressStage[In msgBody, IArgs any, Cfg config.Config] struct {
	*BaseStage[Cfg, IArgs]
}

func NewEgressStage[In msgBody, IArgs any, W worker.Egress[IArgs, In], Cfg stageConfig](
	name string, inConn msgConn[In], workerMaker func() W, cfg Cfg,
) *EgressStage[In, IArgs, Cfg] {

	stageCfg := cfg.GetStage()

	input := newInput(inConn, stageCfg)

	workerRunnerFactory := newEgressWorkerRunnerFactory(input, workerMaker, metrics.NewEgressStage())
	runner := newRunner(workerRunnerFactory, stageCfg)

	return &EgressStage[In, IArgs, Cfg]{
		BaseStage: newBaseStage(KindEgress, name, runner, cfg),
	}
}
