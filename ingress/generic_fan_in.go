package ingress

import (
	"context"
	"errors"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

const (
	DefaultGenericFanInConfigQueueSize = 512
)

type GenericFanInConfig struct {
	*GenericConfig

	QueueSize int
}

func NewGenericFanInConfig(name string) *GenericFanInConfig {
	return &GenericFanInConfig{
		GenericConfig: NewGenericConfig(name),

		QueueSize: DefaultGenericFanInConfigQueueSize,
	}
}

func (c *GenericFanInConfig) Validate(ac *config.AnomalyCollector) {
	c.GenericConfig.Validate(ac)

	config.CheckNotNegative(ac, "QueueSize", &c.QueueSize, DefaultGenericFanInConfigQueueSize)
	config.CheckNotZero(ac, "QueueSize", &c.QueueSize, DefaultGenericFanInConfigQueueSize)
}

// ─── Handler ────────────────────────────────────────────────────────────────|

type GenericFanInHandler[Data any, Out msgBody] interface {
	// Init method is called once when the stage is initialized.
	Init(ctx context.Context) error

	HandleSource(ctx context.Context) (Data, error)
	OnHandleSourceDone()

	HandleData(ctx context.Context, data Data) (*msg[Out], error)
	OnHandleDataDone()

	// Close is called once when the stage is closed.
	Close()

	// SetTelemetry sets the telemetry for the custom handler.
	// It can be used to add traces, logs, and metrics to the
	// user defined handler.
	SetTelemetry(tel *telemetry.Telemetry)
}

type GenericFanInHandlerBase[Data any, Out msgBody] struct {
	Telemetry *telemetry.Telemetry
}

func (hb *GenericFanInHandlerBase[Data, Out]) Init(_ context.Context) error {
	return nil
}

func (hb *GenericFanInHandlerBase[Data, Out]) OnHandleSourceDone() {}

func (hb *GenericFanInHandlerBase[Data, Out]) OnHandleDataDone() {}

func (hb *GenericFanInHandlerBase[Data, Out]) Close() {}

func (hb *GenericFanInHandlerBase[Data, Out]) SetTelemetry(tel *telemetry.Telemetry) {
	hb.Telemetry = tel
}

// ─── Environment ────────────────────────────────────────────────────────────|

type genericFanInEnv[Data any, Out msgBody] struct {
	*env.BaseEnv[*GenericFanInConfig, *metrics.EmptyMetrics]

	handler GenericFanInHandler[Data, Out]

	queueSize uint64
}

func newGenericFanInEnv[Data any, Out msgBody](
	config *GenericFanInConfig, handler GenericFanInHandler[Data, Out],
) *genericFanInEnv[Data, Out] {

	return &genericFanInEnv[Data, Out]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewEmptyMetrics()),

		handler: handler,
	}
}

func (e *genericFanInEnv[Data, Out]) Init(ctx context.Context) error {
	if err := e.BaseEnv.Init(ctx); err != nil {
		return err
	}

	e.cloneConfig()

	e.handler.SetTelemetry(e.Tel)
	return e.handler.Init(ctx)
}

func (e *genericFanInEnv[Data, Out]) cloneConfig() {
	e.queueSize = uint64(e.Config.QueueSize)
}

func (e *genericFanInEnv[Data, Out]) Close(ctx context.Context) {
	e.BaseEnv.Close(ctx)

	e.handler.Close()
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

type genericFanInRunner[Data any, Out msgBody] struct {
	*runnerFanInBase[*genericFanInEnv[Data, Out], Out]
}

func newGenericFanInRunner[Data any, Out msgBody](outConnector msgConn[Out]) *genericFanInRunner[Data, Out] {
	return &genericFanInRunner[Data, Out]{
		runnerFanInBase: newRunnerFanInBase[*genericFanInEnv[Data, Out]](outConnector),
	}
}

func (r *genericFanInRunner[Data, Out]) Init(_ context.Context) error {
	r.initFanIn(r.env.queueSize)
	return nil
}

func (r *genericFanInRunner[Data, Out]) Run(ctx context.Context) {
	defer r.drainAndNotifyRunDone()

	go r.runOutputBridge(context.WithoutCancel(ctx))

	go func() {
		<-ctx.Done()
		r.env.handler.OnHandleSourceDone()
	}()

	for {
		data, err := r.env.handler.HandleSource(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, ErrQuitLoop) {
				r.env.Tel.LogDebug("quitting loop")
				return
			}

			r.env.Tel.LogWarn("failed to handle message", err)
			continue
		}

		r.addWork()
		go r.handleData(ctx, data)
	}
}

func (r *genericFanInRunner[Data, Out]) handleData(ctx context.Context, data Data) {
	defer r.notifyWorkDone()

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			r.env.handler.OnHandleDataDone()
		case <-done:
		}
	}()

	for {
		msgOut, err := r.env.handler.HandleData(ctx, data)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, ErrQuitLoop) {
				return
			}

			r.env.Tel.LogWarn("failed to handle message", err)
			continue
		}

		if err := r.fanIn.Write(msgOut); err != nil {
			msgOut.Destroy()
			r.env.Tel.LogError("failed to write message to fan in connector", err)
		}
	}
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

type GenericFanInStage[Data any, Out msgBody] struct {
	*stage.IngressStage[Out, *genericFanInEnv[Data, Out]]
}

func NewGenericFanInStage[Data any, Out msgBody](
	handler GenericFanInHandler[Data, Out], outConnector msgConn[Out], config *GenericFanInConfig,
) *GenericFanInStage[Data, Out] {

	return &GenericFanInStage[Data, Out]{
		IngressStage: stage.NewIngressStageFromRunner[Out](
			config.Name, newGenericFanInEnv(config, handler), newGenericFanInRunner[Data](outConnector),
		),
	}
}
