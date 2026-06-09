package ingress

import (
	"context"
	"errors"

	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

// ─── Handler ────────────────────────────────────────────────────────────────|

type GenericLoopHandler[Out msgBody] interface {
	// Init method is called once when the stage is initialized.
	Init(ctx context.Context) error

	Handle(ctx context.Context) (*msg[Out], error)
	OnHandleDone()

	// Close is called once when the stage is closed.
	Close()

	// SetTelemetry sets the telemetry for the custom handler.
	// It can be used to add traces, logs, and metrics to the
	// user defined handler.
	SetTelemetry(tel *telemetry.Telemetry)
}

type GenericLoopHandlerBase[Out msgBody] struct {
	Telemetry *telemetry.Telemetry
}

func (hb *GenericLoopHandlerBase[Out]) Init(ctx context.Context) error {
	return nil
}

func (hb *GenericLoopHandlerBase[Out]) OnHandleDone() {}

func (hb *GenericLoopHandlerBase[Out]) Close() {}

func (hb *GenericLoopHandlerBase[Out]) SetTelemetry(tel *telemetry.Telemetry) {
	hb.Telemetry = tel
}

// ─── Environment ────────────────────────────────────────────────────────────|

type genericLoopEnv[Out msgBody] struct {
	*env.BaseEnv[*GenericConfig, *metrics.EmptyMetrics]

	handler GenericLoopHandler[Out]
}

func newGenericLoopEnv[Out msgBody](
	config *GenericConfig, handler GenericLoopHandler[Out],
) *genericLoopEnv[Out] {

	return &genericLoopEnv[Out]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewEmptyMetrics()),

		handler: handler,
	}
}

func (e *genericLoopEnv[Out]) Init(ctx context.Context) error {
	if err := e.BaseEnv.Init(ctx); err != nil {
		return err
	}

	e.handler.SetTelemetry(e.Tel)
	return e.handler.Init(ctx)
}

func (e *genericLoopEnv[Out]) Close(ctx context.Context) {
	e.BaseEnv.Close(ctx)

	e.handler.Close()
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*genericLoopEnv[msgBody]] = (*genericLoopRunner[msgBody])(nil)

type genericLoopRunner[Out msgBody] struct {
	*runnerBase[*genericLoopEnv[Out], Out]
}

func newGenericLoopRunner[Out msgBody](outConnector msgConn[Out]) *genericLoopRunner[Out] {
	return &genericLoopRunner[Out]{
		runnerBase: newRunnerBase[*genericLoopEnv[Out]](outConnector),
	}
}

func (r *genericLoopRunner[Out]) Run(ctx context.Context) {
	defer r.notifyRunDone()

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			r.env.handler.OnHandleDone()
		case <-done:
		}
	}()

	for {
		msgOut, err := r.env.handler.Handle(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, ErrQuitLoop) {
				r.env.Tel.LogDebug("quitting loop")
				return
			}

			r.env.Tel.LogWarn("failed to handle message", err)
			continue
		}

		if err := r.outConnector.Write(msgOut); err != nil {
			msgOut.Destroy()
			r.env.Tel.LogError("failed to write message to output connector", err)
		}
	}
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

type GenericLoopStage[Out msgBody] struct {
	*stage.IngressStage[Out, *genericLoopEnv[Out]]
}

func NewGenericLoopStage[Out msgBody](
	handler GenericLoopHandler[Out], outConnector msgConn[Out], cfg *GenericConfig,
) *GenericLoopStage[Out] {

	return &GenericLoopStage[Out]{
		IngressStage: stage.NewIngressStageFromRunner[Out](
			cfg.Name, newGenericLoopEnv(cfg, handler), newGenericLoopRunner(outConnector),
		),
	}
}
