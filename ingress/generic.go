package ingress

import (
	"context"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

const (
	DefaultGenericConfigName        = "generic"
	DefaultGenericConfigStopOnError = false
)

type GenericConfig struct {
	Name        string
	StopOnError bool
}

func NewGenericConfig(name string) *GenericConfig {
	return &GenericConfig{
		Name:        name,
		StopOnError: DefaultGenericConfigStopOnError,
	}
}

func (c *GenericConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotEmpty(ac, "Name", &c.Name, DefaultGenericConfigName)
}

// ─── Handler ────────────────────────────────────────────────────────────────|

type GenericHandler[Out msgBody] interface {
	// Init method is called once when the stage is initialized.
	Init(ctx context.Context) error

	HandleContextDone()

	Handle(ctx context.Context) (*msg[Out], error)

	// Close is called once when the stage is closed.
	Close()

	// SetTelemetry sets the telemetry for the custom handler.
	// It can be used to add traces, logs, and metrics to the
	// user defined handler.
	SetTelemetry(tel *telemetry.Telemetry)
}

type GenericHandlerBase[Out msgBody] struct {
	Telemetry *telemetry.Telemetry
}

func (ghb *GenericHandlerBase[Out]) HandleContextDone() {}

func (ghb *GenericHandlerBase[Out]) Init(_ context.Context) error {
	return nil
}

func (ghb *GenericHandlerBase[Out]) Close() {}

func (ghb *GenericHandlerBase[Out]) SetTelemetry(tel *telemetry.Telemetry) {
	ghb.Telemetry = tel
}

type GenericHandlerFanIn[T any, Out msgBody] interface {
	// Init method is called once when the stage is initialized.
	Init(ctx context.Context) error

	HandleSourceContextDone()
	HandleSource(ctx context.Context) (T, error)

	HandleContextDone()
	Handle(ctx context.Context, data T) (*msg[Out], error)

	// Close is called once when the stage is closed.
	Close()

	// SetTelemetry sets the telemetry for the custom handler.
	// It can be used to add traces, logs, and metrics to the
	// user defined handler.
	SetTelemetry(tel *telemetry.Telemetry)
}

// ─── Environment ────────────────────────────────────────────────────────────|

type genericEnv[Out msgBody] struct {
	*env.BaseEnv[*GenericConfig, *metrics.EmptyMetrics]

	handler GenericHandler[Out]

	stopOnError bool
}

func newGenericEnv[Out msgBody](config *GenericConfig, handler GenericHandler[Out]) *genericEnv[Out] {
	return &genericEnv[Out]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewEmptyMetrics()),

		handler: handler,
	}
}

func (ge *genericEnv[Out]) Init(ctx context.Context) error {
	if err := ge.BaseEnv.Init(ctx); err != nil {
		return err
	}

	ge.initConfig()

	ge.handler.SetTelemetry(ge.Tel)
	return ge.handler.Init(ctx)
}

func (ge *genericEnv[Out]) Close(ctx context.Context) {
	ge.BaseEnv.Close(ctx)

	ge.handler.Close()
}

func (ge *genericEnv[Out]) initConfig() {
	ge.stopOnError = ge.Config.StopOnError
}

type genericEnvFanIn[T any, Out msgBody] struct {
	*env.BaseEnv[*GenericConfig, *metrics.EmptyMetrics]

	handler GenericHandlerFanIn[T, Out]

	stopOnError bool
}

func newGenericEnvFanIn[T any, Out msgBody](
	config *GenericConfig, handler GenericHandlerFanIn[T, Out],
) *genericEnvFanIn[T, Out] {

	return &genericEnvFanIn[T, Out]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewEmptyMetrics()),

		handler: handler,
	}
}

func (ge *genericEnvFanIn[T, Out]) Init(ctx context.Context) error {
	if err := ge.BaseEnv.Init(ctx); err != nil {
		return err
	}

	ge.initConfig()

	ge.handler.SetTelemetry(ge.Tel)
	return ge.handler.Init(ctx)
}

func (ge *genericEnvFanIn[T, Out]) Close(ctx context.Context) {
	ge.BaseEnv.Close(ctx)

	ge.handler.Close()
}

func (ge *genericEnvFanIn[T, Out]) initConfig() {
	ge.stopOnError = ge.Config.StopOnError
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*genericEnv[msgBody]] = (*genericRunner[msgBody])(nil)

type genericRunner[Out msgBody] struct {
	*runnerBase[*genericEnv[Out], Out]
}

func newGenericRunner[Out msgBody](outConnector msgConn[Out]) *genericRunner[Out] {
	return &genericRunner[Out]{
		runnerBase: newRunnerBase[*genericEnv[Out]](outConnector),
	}
}

func (gr *genericRunner[Out]) Run(ctx context.Context) {
	defer gr.notifyRunDone()

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			gr.env.handler.HandleContextDone()
		case <-done:
		}
	}()

	for {
		msgOut, err := gr.env.handler.Handle(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			if gr.env.stopOnError {
				gr.env.Tel.LogError("failed to handle message", err)
				return
			}

			gr.env.Tel.LogWarn("failed to handle message", err)
			continue
		}

		if err := gr.outConnector.Write(msgOut); err != nil {
			msgOut.Destroy()
			gr.env.Tel.LogError("failed to write message to output connector", err)
		}
	}
}

type genericRunnerFanIn[T any, Out msgBody] struct {
	*runnerFanInBase[*genericEnvFanIn[T, Out], Out]
}

func newGenericRunnerFanIn[T any, Out msgBody](outConnector msgConn[Out]) *genericRunnerFanIn[T, Out] {
	return &genericRunnerFanIn[T, Out]{
		runnerFanInBase: newRunnerFanInBase[*genericEnvFanIn[T, Out]](outConnector),
	}
}

func (gr *genericRunnerFanIn[T, Out]) Init(ctx context.Context) error {
	gr.initFanIn(512)
	return nil
}

func (gr *genericRunnerFanIn[T, Out]) Run(ctx context.Context) {
	defer gr.drainAndNotifyRunDone()

	go gr.runOutputBridge(context.WithoutCancel(ctx))

	go func() {
		<-ctx.Done()
		gr.env.handler.HandleSourceContextDone()
	}()

	for {
		data, err := gr.env.handler.HandleSource(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			if gr.env.stopOnError {
				gr.env.Tel.LogError("failed to handle message", err)
				return
			}

			gr.env.Tel.LogWarn("failed to handle message", err)
			continue
		}

		gr.addWork()
		go gr.handleData(ctx, data)
	}
}

func (gr *genericRunnerFanIn[T, Out]) handleData(ctx context.Context, data T) {
	defer gr.notifyWorkDone()

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			gr.env.handler.HandleContextDone()
		case <-done:
		}
	}()

	for {
		msgOut, err := gr.env.handler.Handle(ctx, data)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			if gr.env.stopOnError {
				gr.env.Tel.LogError("failed to handle message", err)
				return
			}

			gr.env.Tel.LogWarn("failed to handle message", err)
			continue
		}

		if err := gr.outConnector.Write(msgOut); err != nil {
			msgOut.Destroy()
			gr.env.Tel.LogError("failed to write message to output connector", err)
		}
	}
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

type GenericStage[Out msgBody] struct {
	*stage.IngressStage[Out, *genericEnv[Out]]
}

func NewGenericStage[Out msgBody](
	handler GenericHandler[Out], outConnector msgConn[Out], cfg *GenericConfig,
) *GenericStage[Out] {

	return &GenericStage[Out]{
		IngressStage: stage.NewIngressStageFromRunner[Out](
			cfg.Name, newGenericEnv(cfg, handler), newGenericRunner(outConnector),
		),
	}
}

type GenericStageFanIn[T any, Out msgBody] struct {
	*stage.IngressStage[Out, *genericEnvFanIn[T, Out]]
}

func NewGenericStageFanIn[T any, Out msgBody](
	handler GenericHandlerFanIn[T, Out], outConnector msgConn[Out], cfg *GenericConfig,
) *GenericStageFanIn[T, Out] {

	return &GenericStageFanIn[T, Out]{
		IngressStage: stage.NewIngressStageFromRunner[Out](
			cfg.Name, newGenericEnvFanIn(cfg, handler), newGenericRunnerFanIn[T](outConnector),
		),
	}
}
