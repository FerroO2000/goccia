package stage

import (
	"context"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"github.com/FerroO2000/goccia/internal/telemetry"
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

type BaseStage[Env env.Env] struct {
	kind Kind
	name string

	env Env

	runner Runner[Env]
}

func newBaseStage[Env env.Env](
	kind Kind, name string, env Env, runner Runner[Env],
) *BaseStage[Env] {

	tel := telemetry.NewTelemetry(kind, name)
	env.SetTelemetry(tel)

	return &BaseStage[Env]{
		kind: kind,
		name: name,

		env: env,

		runner: runner,
	}
}

func (s *BaseStage[Env]) Telemetry() *telemetry.Telemetry {
	return s.env.Telemetry()
}

func (s *BaseStage[Env]) Kind() Kind {
	return s.kind
}

func (s *BaseStage[Env]) Name() string {
	return s.name
}

func (s *BaseStage[Env]) Env() Env {
	return s.env
}

func (s *BaseStage[Env]) Init(ctx context.Context) error {
	s.Telemetry().LogDebug("initializing stage")

	s.Telemetry().LogDebug("initializing environment")
	if err := s.env.Init(ctx); err != nil {
		return err
	}

	s.Telemetry().LogDebug("initializing runner")
	s.runner.SetEnvironment(s.env)
	if err := s.runner.Init(ctx); err != nil {
		return err
	}

	s.Telemetry().LogInfo("stage initialized")

	return nil
}

func (s *BaseStage[Env]) Run(ctx context.Context) {
	s.Telemetry().LogInfo("running stage")

	s.runner.Run(ctx)

	s.Telemetry().LogInfo("stage stopped")
}

func (s *BaseStage[Env]) Close(ctx context.Context) {
	s.Telemetry().LogDebug("closing stage")

	s.Telemetry().LogDebug("closing runner")
	s.runner.Close(ctx)

	s.Telemetry().LogDebug("closing environment")
	s.env.Close(ctx)

	s.Telemetry().LogInfo("stage closed")
}

func (s *BaseStage[Env]) Inputs() []uintptr {
	return s.runner.Inputs()
}

func (s *BaseStage[Env]) Outputs() []uintptr {
	return s.runner.Outputs()
}

// ─── Ingress ────────────────────────────────────────────────────────────────|

type IngressStage[In msgBody, Env env.Env] struct {
	*BaseStage[Env]
}

func NewIngressStageFromRunner[In msgBody, Env env.Env](
	name string, env Env, runner Runner[Env],
) *IngressStage[In, Env] {

	return &IngressStage[In, Env]{
		BaseStage: newBaseStage(KindIngress, name, env, runner),
	}
}

// ─── Processor ──────────────────────────────────────────────────────────────|

type ProcessorStage[In, Out msgBody, Env env.Env] struct {
	*BaseStage[Env]
}

func NewProcessorStage[In, Out msgBody, Env env.Env, W worker.Processor[Env, In, Out]](
	name string, inConn msgConn[In], outConn msgConn[Out], env Env, workerMaker func() W, stageCfg *config.Stage,
) *ProcessorStage[In, Out, Env] {

	input := newInput(inConn, stageCfg)
	output := newOutput(outConn, stageCfg)

	workerRunnerFactory := newProcessorWorkerRunnerFactory(input, output, workerMaker)
	runner := newRunner(workerRunnerFactory, stageCfg)

	return NewProcessorStageFromRunner[In, Out](name, env, runner)
}

func NewProcessorStageFromRunner[In, Out msgBody, Env env.Env](
	name string, env Env, runner Runner[Env],
) *ProcessorStage[In, Out, Env] {

	return &ProcessorStage[In, Out, Env]{
		BaseStage: newBaseStage(KindProcessor, name, env, runner),
	}
}

// ─── Egress ─────────────────────────────────────────────────────────────────|

type EgressStage[In msgBody, Env env.Env] struct {
	*BaseStage[Env]
}

func NewEgressStage[In msgBody, Env env.Env, W worker.Egress[Env, In]](
	name string, inConn msgConn[In], env Env, workerMaker func() W, stageCfg *config.Stage,
) *EgressStage[In, Env] {

	input := newInput(inConn, stageCfg)

	workerRunnerFactory := newEgressWorkerRunnerFactory(input, workerMaker)
	runner := newRunner(workerRunnerFactory, stageCfg)

	return NewEgressStageFromRunner[In](name, env, runner)
}

func NewEgressStageFromRunner[In msgBody, Env env.Env](
	name string, env Env, runner Runner[Env],
) *EgressStage[In, Env] {

	return &EgressStage[In, Env]{
		BaseStage: newBaseStage(KindEgress, name, env, runner),
	}
}
