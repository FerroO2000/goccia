// Package stage is an internal package that contains the primitives for creating stages.
package stage

import (
	"context"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

// Kind defines the kind of stage.
type Kind = string

const (
	// KindIngress is the kind of ingress stage.
	KindIngress Kind = "ingress"
	// KindProcessor is the kind of processor stage.
	KindProcessor Kind = "processor"
	// KindEgress is the kind of egress stage.
	KindEgress Kind = "egress"
)

// Stage defines the interface for a generic stage.
type Stage interface {
	// Kind returns the kind of the stage.
	Kind() Kind

	// Name returns the name of the stage.
	Name() string

	// Telemetry returns the stage telemetry object.
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

// BaseStage is the base implementation of a stage.
// It is meant to be embedded in the struct of the different kinds of stages.
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

// Telemetry returns the stage telemetry object.
func (s *BaseStage[Env]) Telemetry() *telemetry.Telemetry {
	return s.env.Telemetry()
}

// Kind returns the kind of the stage.
func (s *BaseStage[Env]) Kind() Kind {
	return s.kind
}

// Name returns the name of the stage.
func (s *BaseStage[Env]) Name() string {
	return s.name
}

// Env returns the environment of the stage.
// The environment can be seen as the shared state of the stage.
func (s *BaseStage[Env]) Env() Env {
	return s.env
}

// Init initializes the stage by initializing the environment and the runner.
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

// Run runs the stage by running the runner.
func (s *BaseStage[Env]) Run(ctx context.Context) {
	s.Telemetry().LogInfo("running stage")

	s.runner.Run(ctx)

	s.Telemetry().LogInfo("stage stopped")
}

// Close closes the stage by closing the runner and the environment.
func (s *BaseStage[Env]) Close(ctx context.Context) {
	s.Telemetry().LogDebug("closing stage")

	s.Telemetry().LogDebug("closing runner")
	s.runner.Close(ctx)

	s.Telemetry().LogDebug("closing environment")
	s.env.Close(ctx)

	s.Telemetry().LogInfo("stage closed")
}

// Inputs returns the input connector IDs.
func (s *BaseStage[Env]) Inputs() []uintptr {
	return s.runner.Inputs()
}

// Outputs returns the output connector IDs.
func (s *BaseStage[Env]) Outputs() []uintptr {
	return s.runner.Outputs()
}

// ─── Ingress ────────────────────────────────────────────────────────────────|

// IngressStage is a generic ingress stage to embed in the different ingress stages.
type IngressStage[In msgBody, Env env.Env] struct {
	*BaseStage[Env]
}

// NewIngressStageFromRunner returns a new ingress stage from a custom runner.
func NewIngressStageFromRunner[In msgBody, Env env.Env](
	name string, env Env, runner Runner[Env],
) *IngressStage[In, Env] {

	return &IngressStage[In, Env]{
		BaseStage: newBaseStage(KindIngress, name, env, runner),
	}
}

// ─── Processor ──────────────────────────────────────────────────────────────|

// ProcessorStage is a generic processor stage to embed in the different processor stages.
type ProcessorStage[In, Out msgBody, Env env.Env] struct {
	*BaseStage[Env]
}

// NewProcessorStageFromRunner returns a new processor stage from a custom runner.
func NewProcessorStageFromRunner[In, Out msgBody, Env env.Env](
	name string, env Env, runner Runner[Env],
) *ProcessorStage[In, Out, Env] {

	return &ProcessorStage[In, Out, Env]{
		BaseStage: newBaseStage(KindProcessor, name, env, runner),
	}
}

// NewProcessorStage returns a new processor stage from a custom worker.
func NewProcessorStage[In, Out msgBody, Env env.Env, W worker.Processor[Env, In, Out]](
	name string, inConn msgConn[In], outConn msgConn[Out], env Env, workerMaker func() W, stageCfg *config.Stage,
) *ProcessorStage[In, Out, Env] {

	input := newInput(inConn, stageCfg)
	output := newOutput(outConn, stageCfg)

	backend := newProcessorRunnerBackend(input, output, workerMaker)
	runner := newRunner(backend, stageCfg)

	return NewProcessorStageFromRunner[In, Out](name, env, runner)
}

// ─── Egress ─────────────────────────────────────────────────────────────────|

// EgressStage is a generic egress stage to embed in the different egress stages.
type EgressStage[In msgBody, Env env.Env] struct {
	*BaseStage[Env]
}

// NewEgressStageFromRunner returns a new egress stage from a custom runner.
func NewEgressStageFromRunner[In msgBody, Env env.Env](
	name string, env Env, runner Runner[Env],
) *EgressStage[In, Env] {

	return &EgressStage[In, Env]{
		BaseStage: newBaseStage(KindEgress, name, env, runner),
	}
}

// NewEgressStage returns a new egress stage from a custom worker.
func NewEgressStage[In msgBody, Env env.Env, W worker.Egress[Env, In]](
	name string, inConn msgConn[In], env Env, workerMaker func() W, stageCfg *config.Stage,
) *EgressStage[In, Env] {

	input := newInput(inConn, stageCfg)

	backend := newEgressRunnerBackend(input, workerMaker)
	runner := newRunner(backend, stageCfg)

	return NewEgressStageFromRunner[In](name, env, runner)
}
