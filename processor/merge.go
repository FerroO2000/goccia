package processor

import (
	"context"
	"errors"
	"sync"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/rb"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Default values for the merge stage configuration.
const (
	DefaultMergeConfigOutputQueueSize = 256
)

// MergeConfig structs contains the configuration for the merge stage.
type MergeConfig struct {
	// OutputQueueSize is the size of the fan-in ring buffer
	// placed between the input connectors and the output connector.
	OutputQueueSize int
}

// NewMergeConfig returns a new MergeConfig with default values.
func NewMergeConfig() *MergeConfig {
	return &MergeConfig{
		OutputQueueSize: DefaultMergeConfigOutputQueueSize,
	}
}

// Validate checks the configuration.
func (c *MergeConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotNegative(ac, "OutputQueueSize", &c.OutputQueueSize, DefaultMergeConfigOutputQueueSize)
	config.CheckNotZero(ac, "OutputQueueSize", &c.OutputQueueSize, DefaultMergeConfigOutputQueueSize)
}

// ─── Environment ────────────────────────────────────────────────────────────|

type mergeEnv struct {
	*env.BaseEnv[*MergeConfig, *metrics.EmptyMetrics]
}

func newMergeEnv(config *MergeConfig) *mergeEnv {
	return &mergeEnv{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewEmptyMetrics()),
	}
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*mergeEnv] = (*mergeRunner[msgBody])(nil)

type mergeRunner[T msgBody] struct {
	*mergeEnv

	inConnectors []msgConn[T]
	outConnector msgConn[T]

	fanIn *rb.RingBuffer[*msg[T]]

	readersWg *sync.WaitGroup

	runOutputBridgeDone chan struct{}
	runDone             chan struct{}
}

func newMergeRunner[T msgBody](inConnectors []msgConn[T], outConnector msgConn[T]) *mergeRunner[T] {
	return &mergeRunner[T]{
		inConnectors: inConnectors,
		outConnector: outConnector,

		readersWg: &sync.WaitGroup{},

		runOutputBridgeDone: make(chan struct{}),
		runDone:             make(chan struct{}),
	}
}

func (mr *mergeRunner[T]) SetEnvironment(env *mergeEnv) {
	mr.mergeEnv = env
}

func (mr *mergeRunner[T]) Init(_ context.Context) error {
	if len(mr.inConnectors) == 0 {
		return errors.New("no input connector specified")
	}

	fanInCapacity := uint64(mr.Config.OutputQueueSize)
	mr.fanIn = rb.NewRingBuffer[*msg[T]](fanInCapacity, rb.BufferKindMPSC)

	return nil
}

func (mr *mergeRunner[T]) Run(ctx context.Context) {
	defer close(mr.runDone)

	mr.readersWg.Add(len(mr.inConnectors))
	for _, inConnector := range mr.inConnectors {
		go mr.readInput(ctx, inConnector)
	}

	go mr.runOutputBridge(context.WithoutCancel(ctx))

	mr.readersWg.Wait()
	mr.fanIn.Close()
	<-mr.runOutputBridgeDone
}

func (mr *mergeRunner[T]) readInput(ctx context.Context, inConnector msgConn[T]) {
	defer mr.readersWg.Done()

	for {
		msgIn, err := inConnector.Read(ctx)
		if err != nil {
			// This means the input connector is closed
			// and there are no more messages in it
			return
		}

		mr.GetProcessorMetrics().IncrementProcessedMessages()
		if err := mr.fanIn.Write(msgIn); err != nil {
			msgIn.Destroy()
		}
	}
}

func (mr *mergeRunner[T]) runOutputBridge(ctx context.Context) {
	defer close(mr.runOutputBridgeDone)

	for {
		msg, err := mr.fanIn.Read(ctx)
		if err != nil {
			return
		}

		if err := mr.outConnector.Write(msg); err != nil {
			msg.Destroy()
		}
	}
}

func (mr *mergeRunner[T]) Close(_ context.Context) {
	<-mr.runDone
	mr.outConnector.Close()
}

func (mr *mergeRunner[T]) Inputs() []uintptr {
	inputs := make([]uintptr, 0, len(mr.inConnectors))

	for _, inConnector := range mr.inConnectors {
		inputs = append(inputs, connector.GetConnectorID(inConnector))
	}

	return inputs
}

func (mr *mergeRunner[T]) Outputs() []uintptr {
	return []uintptr{connector.GetConnectorID(mr.outConnector)}
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// MergeStage is a stage that merges multiple input connectors into a single
// output connector.
// It is the counterpart of the tee stage.
type MergeStage[T msgBody] struct {
	*stage.ProcessorStage[T, T, *mergeEnv]
}

// NewMergeStage returns a new merge processor stage.
func NewMergeStage[T msgBody](
	inConnectors []msgConn[T], outConnector msgConn[T], config *MergeConfig,
) *MergeStage[T] {

	return &MergeStage[T]{
		ProcessorStage: stage.NewProcessorStageFromRunner[T, T](
			"merge", newMergeEnv(config), newMergeRunner(inConnectors, outConnector),
		),
	}
}
