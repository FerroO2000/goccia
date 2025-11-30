package processor

import (
	"context"
	"fmt"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/pool"
	stageCommon "github.com/FerroO2000/goccia/internal/stage"
)

//////////////
//  CONFIG  //
//////////////

// CustomConfig structs contains the configuration for a custom processor stage.
type CustomConfig struct {
	Stage *stageCommon.Config

	// Name is the name of the stage.
	// It is used to identify the stage in the telemetry.
	//
	// Default: "custom"
	Name string
}

// DefaultCustomConfig returns the default configuration for a custom processor stage.
func DefaultCustomConfig(runningMode stageCommon.RunningMode) *CustomConfig {
	return &CustomConfig{
		Stage: stageCommon.DefaultConfig(runningMode),

		Name: "custom",
	}
}

///////////////
//  HANDLER  //
///////////////

// CustomHandler interface defines the methods that the handler for the
// processor processor must implement.
type CustomHandler[In msgEnv, T any, Out msgEnvPtr[T]] interface {
	// Init method is called once when the stage is initialized.
	Init(ctx context.Context) error

	// Handle method is called by one of the spawned workers
	// for each message received by the stage.
	Handle(ctx context.Context, msgIn In, msgOut Out) error

	// Close is called once when the stage is closed.
	Close()

	// SetTelemetry sets the telemetry for the custom handler.
	// It can be used to add traces, logs, and metrics to the
	// user defined handler.
	SetTelemetry(tel *internal.Telemetry)
}

// CustomHandlerBase is a base implementation of the CustomHandler interface.
// It provides a Telemetry field that can be used to add traces,
// logs, and metrics to the custom handler.
// It also provides a default implementation for the Init and Close methods,
// but not for the Handle method.
type CustomHandlerBase struct {
	Telemetry *internal.Telemetry
}

// Init is a no-op implementation of the custom handler Init method.
func (chb *CustomHandlerBase) Init(_ context.Context) error {
	return nil
}

// Close is a no-op implementation of the custom handler Close method.
func (chb *CustomHandlerBase) Close() {}

// SetTelemetry sets the telemetry for the custom handler.
func (chb *CustomHandlerBase) SetTelemetry(tel *internal.Telemetry) {
	chb.Telemetry = tel
}

//////////////
//  WORKER  //
//////////////

type customWorkerArgs[In msgEnv, T any, Out msgEnvPtr[T]] struct {
	name    string
	handler CustomHandler[In, T, Out]
}

func newCustomWorkerArgs[In msgEnv, T any, Out msgEnvPtr[T]](name string, handler CustomHandler[In, T, Out]) *customWorkerArgs[In, T, Out] {
	return &customWorkerArgs[In, T, Out]{
		name:    name,
		handler: handler,
	}
}

type customWorker[In msgEnv, T any, Out msgEnvPtr[T]] struct {
	pool.BaseWorker

	handler CustomHandler[In, T, Out]

	traceString string
}

func newCustomWorkerInstMaker[In msgEnv, T any, Out msgEnvPtr[T]]() workerInstanceMaker[*customWorkerArgs[In, T, Out], In, Out] {
	return func() workerInstance[*customWorkerArgs[In, T, Out], In, Out] {
		return &customWorker[In, T, Out]{}
	}
}

func (cw *customWorker[In, T, Out]) Init(_ context.Context, args *customWorkerArgs[In, T, Out]) error {
	cw.handler = args.handler
	cw.handler.SetTelemetry(cw.Tel)

	cw.traceString = fmt.Sprintf("handle %s message", args.name)

	return nil
}

func (cw *customWorker[In, T, Out]) Handle(ctx context.Context, msgIn *msg[In]) (*msg[Out], error) {
	ctx, span := cw.Tel.NewTrace(ctx, cw.traceString)
	defer span.End()

	// Create the generic output message
	var dummyMsgOutVal T
	msgOutVal := Out(&dummyMsgOutVal)

	// Call the provided handler
	if err := cw.handler.Handle(ctx, msgIn.GetEnvelope(), msgOutVal); err != nil {
		return &msg[Out]{}, err
	}

	msgOut := message.NewMessage(msgOutVal)
	msgOut.SaveSpan(span)

	return msgOut, nil
}

func (cw *customWorker[In, T, Out]) Close(_ context.Context) error {
	return nil
}

/////////////
//  STAGE  //
/////////////

// CustomStage is a processor stage that uses a custom handler to process messages.
type CustomStage[In msgEnv, T any, Out msgEnvPtr[T]] struct {
	stage[*customWorkerArgs[In, T, Out], In, Out]

	cfg *CustomConfig

	handler CustomHandler[In, T, Out]
}

// NewCustomStage returns a new custom processor stage.
func NewCustomStage[In msgEnv, T any, Out msgEnvPtr[T]](
	handler CustomHandler[In, T, Out], inputConnector msgConn[In], outputConnector msgConn[Out], cfg *CustomConfig,
) *CustomStage[In, T, Out] {

	return &CustomStage[In, T, Out]{
		stage: newStage(
			cfg.Name, inputConnector, outputConnector, newCustomWorkerInstMaker[In, T, Out](), cfg.Stage,
		),

		cfg: cfg,

		handler: handler,
	}
}

// Init initializes the stage.
func (cs *CustomStage[In, T, Out]) Init(ctx context.Context) error {
	// Initialize the handler
	if err := cs.handler.Init(ctx); err != nil {
		return err
	}

	return cs.stage.Init(ctx, newCustomWorkerArgs(cs.cfg.Name, cs.handler))
}
