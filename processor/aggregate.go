package processor

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/processor/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Default configuration values for the aggregate stage.
const (
	DefaultAggregateConfigBatchSize = 64
	DefaultAggregateConfigTimeout   = 1 * time.Second
)

// AggregateConfig struct contains the configuration for the aggregate stage.
type AggregateConfig struct {
	// BatchSize is the number of messages to aggregate
	// before sending them to the next stage.
	BatchSize int

	// Timeout is the maximum duration to wait for a batch to be fullfilled.
	Timeout time.Duration
}

// NewAggregateConfig returns a new AggregateConfig with default values.
func NewAggregateConfig() *AggregateConfig {
	return &AggregateConfig{
		BatchSize: DefaultAggregateConfigBatchSize,
		Timeout:   DefaultAggregateConfigTimeout,
	}
}

// Validate checks the configuration.
func (c *AggregateConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotNegative(ac, "BatchSize", &c.BatchSize, DefaultAggregateConfigBatchSize)
	config.CheckNotZero(ac, "BatchSize", &c.BatchSize, DefaultAggregateConfigBatchSize)

	config.CheckNotNegative(ac, "Timeout", &c.Timeout, DefaultAggregateConfigTimeout)
	config.CheckNotZero(ac, "Timeout", &c.Timeout, DefaultAggregateConfigTimeout)
}

// ─── Message ────────────────────────────────────────────────────────────────|

// AggregateMessage is a message containing a batch of messages.
// It receive time will be the receive time of the first message in the batch,
// while the timestamp will be the timestamp of the last one.
// It will generate a new trace span linked to all the messages in the batch.
type AggregateMessage[T msgBody] struct {
	Batch []*msg[T]
}

func newAggregateMessage[T msgBody](batch []*msg[T]) *AggregateMessage[T] {
	return &AggregateMessage[T]{
		Batch: batch,
	}
}

// Destroy cleans up the messages in the batch.
func (m *AggregateMessage[T]) Destroy() {
	for _, msg := range m.Batch {
		msg.Destroy()
	}
	m.Batch = nil
}

// ─── Environment ────────────────────────────────────────────────────────────|

type aggregateEnv struct {
	*env.BaseEnv[*AggregateConfig, *metrics.AggregateStage]
}

func newAggregateEnv(config *AggregateConfig) *aggregateEnv {
	return &aggregateEnv{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewAggregateStage()),
	}
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*aggregateEnv] = (*aggregateRunner[msgBody])(nil)

type aggregateRunner[T msgBody] struct {
	*aggregateEnv

	inConnector  msgConn[T]
	outConnector msgConn[*AggregateMessage[T]]

	runDone chan struct{}
}

func newAggregateRunner[T msgBody](
	inConnector msgConn[T], outConnector msgConn[*AggregateMessage[T]],
) *aggregateRunner[T] {

	return &aggregateRunner[T]{
		inConnector:  inConnector,
		outConnector: outConnector,

		runDone: make(chan struct{}),
	}
}

func (r *aggregateRunner[T]) SetEnvironment(env *aggregateEnv) {
	r.aggregateEnv = env
}

func (r *aggregateRunner[T]) Init(_ context.Context) error {
	return nil
}

func (r *aggregateRunner[T]) Run(ctx context.Context) {
	defer close(r.runDone)

	accumulator := make([]*msg[T], 0, r.Config.BatchSize)

	for {
		timeoutCtx, cancelTimeoutCtx := context.WithTimeout(ctx, r.Config.Timeout)
		msgIn, err := r.inConnector.Read(timeoutCtx)
		cancelTimeoutCtx()

		if err != nil {
			if len(accumulator) > 0 {
				goto writeOutput
			}

			// The run context is done, quit
			if errors.Is(err, connector.ErrClosed) || ctx.Err() != nil {
				return
			}

			// Got a timeout and the accumulator is empty
			continue
		}

		accumulator = append(accumulator, msgIn)

		if len(accumulator) != r.Config.BatchSize {
			continue
		}

	writeOutput:
		r.GetProcessorMetrics().AddProcessedMessages(uint(len(accumulator)))

		msgOut := r.aggregate(ctx, accumulator)

		err = r.outConnector.Write(msgOut)
		if err != nil {
			r.Tel.LogError("failed to write message to output connector", err)
			msgOut.Destroy()
		} else {
			r.Metrics.IncrementAggregateMessages()
		}

		// Reset the accumulator
		clear(accumulator)
		accumulator = accumulator[:0]
	}
}

func (r *aggregateRunner[T]) aggregate(ctx context.Context, accumulator []*msg[T]) *msg[*AggregateMessage[T]] {
	batchSize := len(accumulator)

	links := make([]trace.Link, 0, batchSize)
	for _, msgIn := range accumulator {
		spanCtx := msgIn.GetSpanContext()
		if spanCtx.IsValid() {
			links = append(links, trace.Link{SpanContext: spanCtx})
		}
	}

	_, span := r.Tel.StartTrace(ctx, "aggregate messages", trace.WithNewRoot(), trace.WithLinks(links...))
	defer span.End()

	batch := slices.Clone(accumulator)
	aggregateMsg := newAggregateMessage(batch)

	msgOut := message.NewMessage(aggregateMsg)

	// Set the receive time of the first message
	msgOut.SetReceiveTime(accumulator[0].GetReceiveTime())

	// Set the timestamp of the last message
	msgOut.SetTimestamp(accumulator[batchSize-1].GetTimestamp())

	// Set the correlation ID of the first message
	msgOut.SetCorrelationID(accumulator[0].GetCorrelationID())

	// Telemetry
	span.SetAttributes(attribute.Int("batch_size", batchSize))
	msgOut.SaveSpan(span)

	return msgOut
}

func (r *aggregateRunner[T]) Close(_ context.Context) {
	<-r.runDone
	r.outConnector.Close()
}

func (r *aggregateRunner[T]) Inputs() []uintptr {
	return []uintptr{connector.GetConnectorID(r.inConnector)}
}

func (r *aggregateRunner[T]) Outputs() []uintptr {
	return []uintptr{connector.GetConnectorID(r.outConnector)}
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// AggregateStage is a processor stage that aggregates input messages into batches.
type AggregateStage[T msgBody] struct {
	*stage.ProcessorStage[T, *AggregateMessage[T], *aggregateEnv]
}

// NewAggregateStage returns a new aggregate processor stage.
func NewAggregateStage[T msgBody](
	inConnector msgConn[T], outConnector msgConn[*AggregateMessage[T]], config *AggregateConfig,
) *AggregateStage[T] {

	return &AggregateStage[T]{
		ProcessorStage: stage.NewProcessorStageFromRunner[T, *AggregateMessage[T]](
			"aggregate", newAggregateEnv(config), newAggregateRunner(inConnector, outConnector),
		),
	}
}
