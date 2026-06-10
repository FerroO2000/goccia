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

// Default values for the generic fan-in ingress stage configuration.
const (
	DefaultGenericFanInConfigQueueSize = 512
)

// GenericFanInConfig structs contains the configuration
// for a generic fan-in ingress stage.
type GenericFanInConfig struct {
	*GenericConfig

	QueueSize int
}

// NewGenericFanInConfig returns the default configuration for a generic fan-in ingress stage.
func NewGenericFanInConfig(name string) *GenericFanInConfig {
	return &GenericFanInConfig{
		GenericConfig: NewGenericConfig(name),

		QueueSize: DefaultGenericFanInConfigQueueSize,
	}
}

// Validate checks the configuration.
func (c *GenericFanInConfig) Validate(ac *config.AnomalyCollector) {
	c.GenericConfig.Validate(ac)

	config.CheckNotNegative(ac, "QueueSize", &c.QueueSize, DefaultGenericFanInConfigQueueSize)
	config.CheckNotZero(ac, "QueueSize", &c.QueueSize, DefaultGenericFanInConfigQueueSize)
}

// ─── Handler ────────────────────────────────────────────────────────────────|

// GenericFanInHandler defines the methods that a generic fan-in ingress stage
// handler must implement.
type GenericFanInHandler[Data any, Out msgBody] interface {
	// Init method is called once when the stage is initialized.
	Init(ctx context.Context) error

	// HandleSource is called in the main loop of the stage.
	// It is meant to be used for returning an arbitrary data
	// that will be used by the HandleData method, running in a separate goroutine.
	// E.g. in the context of a TCP server, this function will listen
	// for new connections, and when the connection is established, it will return it,
	// so it can be used by the HandleData method in a separate goroutine.
	//
	// For quiting the main loop, return an error of type ErrQuitLoop.
	HandleSource(ctx context.Context) (Data, error)

	// OnRunContextDone is called when the running context is done,
	// meaning that the stage should be stopped.
	OnRunContextDone()

	// HandleData is called in a loop inside a separate goroutine
	// triggered by the HandleSource method.
	// This method shall return the message to be sent to the next stage.
	// If the method returns an error of type ErrQuitLoop, the
	// loop will be stopped and the separate goroutine will be closed.
	HandleData(ctx context.Context, data Data) (*msg[Out], error)

	// OnHandleDataContextDone is called when the HandleData context is done,
	// meaning that the separate goroutine should be closed.
	OnHandleDataContextDone()

	// Close is called once when the stage is closed.
	Close()

	// SetTelemetry sets the telemetry for the custom handler.
	// It can be used to add traces, logs, and metrics to the
	// user defined handler.
	SetTelemetry(tel *telemetry.Telemetry)
}

// GenericFanInHandlerBase is the base implementation for a generic fan-in
// ingress stage handler.
type GenericFanInHandlerBase[Data any, Out msgBody] struct {
	// Telemetry can be used to add traces, logs, and metrics to the custom handler.
	Telemetry *telemetry.Telemetry
}

// Init is a no-op implementation of the generic handler Init method.
func (hb *GenericFanInHandlerBase[Data, Out]) Init(_ context.Context) error {
	return nil
}

// OnRunContextDone is a no-op implementation of the generic handler OnRunContextDone method.
func (hb *GenericFanInHandlerBase[Data, Out]) OnRunContextDone() {}

// OnHandleDataContextDone is a no-op implementation of the generic handler OnHandleDataContextDone method.
func (hb *GenericFanInHandlerBase[Data, Out]) OnHandleDataContextDone() {}

// Close is a no-op implementation of the generic handler Close method.
func (hb *GenericFanInHandlerBase[Data, Out]) Close() {}

// SetTelemetry sets the telemetry for the generic handler.
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
		r.env.handler.OnRunContextDone()
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
			r.env.handler.OnHandleDataContextDone()
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

// GenericFanInStage is a generic ingress stage that uses an user defined handler
// for ingesting new messages into the pipeline.
// It will call the provided handler in a loop until the stage is stopped
// (either by the context or by the user) and spawn separate goroutines
// which will generate the messages. Similar to a TCP server,
// the source is the listener, while the connections are the generic
// data that will generate the messages.
type GenericFanInStage[Data any, Out msgBody] struct {
	*stage.IngressStage[Out, *genericFanInEnv[Data, Out]]
}

// NewGenericFanInStage returns a new GenericFanInStage.
func NewGenericFanInStage[Data any, Out msgBody](
	handler GenericFanInHandler[Data, Out], outConnector msgConn[Out], config *GenericFanInConfig,
) *GenericFanInStage[Data, Out] {

	return &GenericFanInStage[Data, Out]{
		IngressStage: stage.NewIngressStageFromRunner[Out](
			config.Name, newGenericFanInEnv(config, handler), newGenericFanInRunner[Data](outConnector),
		),
	}
}
