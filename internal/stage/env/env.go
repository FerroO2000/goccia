// Package env is an internal package that contains the environment definitions for stages.
package env

import (
	"context"

	"github.com/FerroO2000/goccia/internal/config"
	metricsPkg "github.com/FerroO2000/goccia/internal/metrics"
	stageMetrics "github.com/FerroO2000/goccia/internal/stage/metrics"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

type cfg = config.Config
type met = metricsPkg.Metrics

// Env is the environment interface used by the stages.
type Env interface {
	// SetTelemetry sets the telemetry for the environment.
	SetTelemetry(tel *telemetry.Telemetry)
	// Telemetry returns the environment telemetry object.
	Telemetry() *telemetry.Telemetry

	// Init initializes the environment.
	Init(ctx context.Context) error

	// Close closes the environment.
	Close(ctx context.Context)

	// GetIngressMetrics returns the ingress metrics for the environment.
	GetIngressMetrics() *metricsPkg.EmptyMetrics
	// GetProcessorMetrics returns the processor metrics for the environment.
	GetProcessorMetrics() *stageMetrics.ProcessorStage
	// GetEgressMetrics returns the egress metrics for the environment.
	GetEgressMetrics() *stageMetrics.EgressStage
}

var _ Env = (*BaseEnv[cfg, met])(nil)

// BaseEnv is the base environment implementation for the stages.
type BaseEnv[Cfg cfg, M met] struct {
	Tel *telemetry.Telemetry

	// Config is the configuration object for the given environment.
	Config Cfg

	ingressMetrics   *metricsPkg.EmptyMetrics
	processorMetrics *stageMetrics.ProcessorStage
	egressMetrics    *stageMetrics.EgressStage

	// Metrics is the metrics object for the given environment.
	Metrics M
}

func newBaseEnv[Cfg cfg, M met](config Cfg, metrics M) *BaseEnv[Cfg, M] {
	return &BaseEnv[Cfg, M]{
		Tel: nil,

		Config: config,

		ingressMetrics:   nil,
		processorMetrics: nil,
		egressMetrics:    nil,

		Metrics: metrics,
	}
}

// NewIngressEnv returns a new ingress environment.
func NewIngressEnv[Cfg cfg, M met](config Cfg, metrics M) *BaseEnv[Cfg, M] {
	env := newBaseEnv(config, metrics)
	env.ingressMetrics = metricsPkg.NewEmptyMetrics()
	return env
}

// NewProcessorEnv returns a new processor environment.
func NewProcessorEnv[Cfg cfg, M met](config Cfg, metrics M) *BaseEnv[Cfg, M] {
	env := newBaseEnv(config, metrics)
	env.processorMetrics = stageMetrics.NewProcessorStage()
	return env
}

// NewEgressEnv returns a new egress environment.
func NewEgressEnv[Cfg cfg, M met](config Cfg, metrics M) *BaseEnv[Cfg, M] {
	env := newBaseEnv(config, metrics)
	env.egressMetrics = stageMetrics.NewEgressStage()
	return env
}

// SetTelemetry sets the telemetry for the environment.
func (e *BaseEnv[Cfg, M]) SetTelemetry(tel *telemetry.Telemetry) {
	e.Tel = tel
}

// Telemetry returns the environment telemetry object.
func (e *BaseEnv[Cfg, M]) Telemetry() *telemetry.Telemetry {
	return e.Tel
}

func (e *BaseEnv[Cfg, M]) validateConfig() {
	e.Tel.LogDebug("validating configuration")
	defer e.Tel.LogDebug("validated configuration")

	configValidator := config.NewValidator(e.Tel)
	configValidator.Validate(e.Config)
}

func (e *BaseEnv[Cfg, M]) getStageMetrics() met {
	if e.ingressMetrics != nil {
		return e.ingressMetrics
	}

	if e.processorMetrics != nil {
		return e.processorMetrics
	}

	if e.egressMetrics != nil {
		return e.egressMetrics
	}

	panic("missing stage metrics")
}

func (e *BaseEnv[Cfg, M]) initMetrics() error {
	e.Tel.LogDebug("initializing metrics")
	defer e.Tel.LogDebug("initialized metrics")

	if err := e.getStageMetrics().InitMetrics(e.Tel); err != nil {
		return err
	}

	return e.Metrics.InitMetrics(e.Tel)
}

// Init initializes the environment by validating the configuration
// and initializing the metrics.
func (e *BaseEnv[Cfg, M]) Init(_ context.Context) error {
	e.validateConfig()
	return e.initMetrics()
}

// Close is a no-op.
func (e *BaseEnv[Cfg, M]) Close(_ context.Context) {}

// GetIngressMetrics returns the ingress metrics for the environment,
// if the environment belongs to an ingress stage.
func (e *BaseEnv[Cfg, M]) GetIngressMetrics() *metricsPkg.EmptyMetrics {
	if e.ingressMetrics == nil {
		panic("no ingress metrics")
	}

	return e.ingressMetrics
}

// GetProcessorMetrics returns the processor metrics for the environment,
// if the environment belongs to a processor stage.
func (e *BaseEnv[Cfg, M]) GetProcessorMetrics() *stageMetrics.ProcessorStage {
	if e.processorMetrics == nil {
		panic("no processor metrics")
	}

	return e.processorMetrics
}

// GetEgressMetrics returns the egress metrics for the environment,
// if the environment belongs to an egress stage.
func (e *BaseEnv[Cfg, M]) GetEgressMetrics() *stageMetrics.EgressStage {
	if e.egressMetrics == nil {
		panic("no egress metrics")
	}

	return e.egressMetrics
}
