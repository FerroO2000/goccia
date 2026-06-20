package processor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type aggregateTestBody struct {
	value        int
	destroyCount atomic.Int32
}

func (m *aggregateTestBody) Destroy() {
	m.destroyCount.Add(1)
}

func newAggregateTestMessage(value int, receiveTime, timestamp time.Time) *msg[*aggregateTestBody] {
	msg := message.NewMessage(&aggregateTestBody{value: value})
	msg.SetReceiveTime(receiveTime)
	msg.SetTimestamp(timestamp)
	return msg
}

func Test_AggregateStage_EmitsFullBatchInInputOrder(t *testing.T) {
	const batchSize = 3

	in := connector.NewRingBuffer[*aggregateTestBody](batchSize)
	out := connector.NewRingBuffer[*AggregateMessage[*aggregateTestBody]](1)

	firstReceiveTime := time.Now().Add(-time.Second)
	messages := make([]*msg[*aggregateTestBody], 0, batchSize)
	for idx := range batchSize {
		msgIn := newAggregateTestMessage(
			idx,
			firstReceiveTime.Add(time.Duration(idx)*time.Millisecond),
			firstReceiveTime.Add(time.Duration(idx+1)*time.Millisecond),
		)
		messages = append(messages, msgIn)
		require.NoError(t, in.Write(msgIn))
	}
	in.Close()

	cfg := NewAggregateConfig()
	cfg.BatchSize = batchSize
	cfg.Timeout = time.Hour

	stage := NewAggregateStage(in, out, cfg)
	require.NoError(t, stage.Init(t.Context()))

	stage.Run(t.Context())
	stage.Close(t.Context())

	msgOut, err := out.Read(t.Context())
	require.NoError(t, err)
	assert.Equal(t, messages, msgOut.GetBody().Batch)
	assert.Equal(t, messages[0].GetReceiveTime(), msgOut.GetReceiveTime())
	assert.Equal(t, messages[batchSize-1].GetTimestamp(), msgOut.GetTimestamp())

	for _, msgIn := range messages {
		assert.Zero(t, msgIn.GetBody().destroyCount.Load())
	}

	msgOut.Destroy()
	for _, msgIn := range messages {
		assert.Equal(t, int32(1), msgIn.GetBody().destroyCount.Load())
	}

	_, err = out.Read(t.Context())
	assert.ErrorIs(t, err, connector.ErrClosed)
}

func Test_AggregateStage_EmitsPartialBatchAfterTimeoutAndNoEmptyBatch(t *testing.T) {
	const batchSize = 3

	in := connector.NewRingBuffer[*aggregateTestBody](batchSize)
	out := connector.NewRingBuffer[*AggregateMessage[*aggregateTestBody]](1)

	cfg := NewAggregateConfig()
	cfg.BatchSize = batchSize
	cfg.Timeout = 20 * time.Millisecond

	stage := NewAggregateStage(in, out, cfg)
	require.NoError(t, stage.Init(t.Context()))

	for idx := range batchSize - 1 {
		require.NoError(t, in.Write(newAggregateTestMessage(idx, time.Now(), time.Now())))
	}

	runCtx, cancelRun := context.WithCancel(t.Context())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		stage.Run(runCtx)
	}()

	readCtx, cancelRead := context.WithTimeout(t.Context(), time.Second)
	msgOut, err := out.Read(readCtx)
	cancelRead()
	require.NoError(t, err)
	assert.Len(t, msgOut.GetBody().Batch, batchSize-1)
	msgOut.Destroy()

	emptyReadCtx, cancelEmptyRead := context.WithTimeout(t.Context(), 3*cfg.Timeout)
	_, err = out.Read(emptyReadCtx)
	cancelEmptyRead()
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	cancelRun()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("aggregate stage did not stop after cancellation")
	}
	stage.Close(t.Context())
}

func Test_AggregateStage_FlushesPartialBatchWhenInputCloses(t *testing.T) {
	in := connector.NewRingBuffer[*aggregateTestBody](2)
	out := connector.NewRingBuffer[*AggregateMessage[*aggregateTestBody]](1)

	for idx := range 2 {
		require.NoError(t, in.Write(newAggregateTestMessage(idx, time.Now(), time.Now())))
	}
	in.Close()

	cfg := NewAggregateConfig()
	cfg.BatchSize = 3
	cfg.Timeout = time.Hour

	stage := NewAggregateStage(in, out, cfg)
	require.NoError(t, stage.Init(t.Context()))

	stage.Run(t.Context())
	stage.Close(t.Context())

	msgOut, err := out.Read(t.Context())
	require.NoError(t, err)
	assert.Len(t, msgOut.GetBody().Batch, 2)
	msgOut.Destroy()
}

var errAggregateTestWrite = errors.New("aggregate test write failure")

type aggregateFailingOutput[T msgBody] struct {
	writes atomic.Int32
}

func (c *aggregateFailingOutput[T]) Write(*msg[*AggregateMessage[T]]) error {
	c.writes.Add(1)
	return errAggregateTestWrite
}

func (*aggregateFailingOutput[T]) Read(context.Context) (*msg[*AggregateMessage[T]], error) {
	return nil, connector.ErrClosed
}

func (*aggregateFailingOutput[T]) Close() {}

func Test_AggregateStage_DestroysBatchAfterOutputWriteFailure(t *testing.T) {
	in := connector.NewRingBuffer[*aggregateTestBody](1)
	out := &aggregateFailingOutput[*aggregateTestBody]{}

	msgIn := newAggregateTestMessage(1, time.Now(), time.Now())
	require.NoError(t, in.Write(msgIn))
	in.Close()

	cfg := NewAggregateConfig()
	cfg.BatchSize = 1

	stage := NewAggregateStage(in, out, cfg)
	require.NoError(t, stage.Init(t.Context()))

	stage.Run(t.Context())
	stage.Close(t.Context())

	assert.Equal(t, int32(1), out.writes.Load())
	assert.Equal(t, int32(1), msgIn.GetBody().destroyCount.Load())
}

func Test_AggregateConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		batchSize int
		timeout   time.Duration
	}{
		"zero": {
			batchSize: 0,
			timeout:   0,
		},
		"negative": {
			batchSize: -1,
			timeout:   -time.Second,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			in := connector.NewRingBuffer[*aggregateTestBody](1)
			out := connector.NewRingBuffer[*AggregateMessage[*aggregateTestBody]](1)
			cfg := &AggregateConfig{BatchSize: test.batchSize, Timeout: test.timeout}

			stage := NewAggregateStage(in, out, cfg)
			require.NoError(t, stage.Init(t.Context()))

			assert.Equal(t, DefaultAggregateConfigBatchSize, cfg.BatchSize)
			assert.Equal(t, DefaultAggregateConfigTimeout, cfg.Timeout)
		})
	}
}

type aggregateRecordingSpan struct {
	trace.Span

	spanCtx trace.SpanContext
	ended   atomic.Bool

	mux        sync.Mutex
	attributes []attribute.KeyValue
}

func (s *aggregateRecordingSpan) SpanContext() trace.SpanContext {
	return s.spanCtx
}

func (s *aggregateRecordingSpan) SetAttributes(attributes ...attribute.KeyValue) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.attributes = append(s.attributes, attributes...)
}

func (s *aggregateRecordingSpan) End(...trace.SpanEndOption) {
	s.ended.Store(true)
}

type aggregateRecordingTracer struct {
	trace.Tracer

	span        *aggregateRecordingSpan
	name        string
	startConfig trace.SpanConfig
}

func (t *aggregateRecordingTracer) Start(
	ctx context.Context, name string, options ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	t.name = name
	t.startConfig = trace.NewSpanStartConfig(options...)
	return trace.ContextWithSpan(ctx, t.span), t.span
}

type aggregateRecordingTracerProvider struct {
	trace.TracerProvider
	tracer *aggregateRecordingTracer
}

func (p *aggregateRecordingTracerProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	return p.tracer
}

func newAggregateRecordingTracerProvider(spanCtx trace.SpanContext) *aggregateRecordingTracerProvider {
	noopProvider := trace.NewNoopTracerProvider()
	noopTracer := noopProvider.Tracer("")
	noopSpan := trace.SpanFromContext(context.Background())

	return &aggregateRecordingTracerProvider{
		TracerProvider: noopProvider,
		tracer: &aggregateRecordingTracer{
			Tracer: noopTracer,
			span: &aggregateRecordingSpan{
				Span:    noopSpan,
				spanCtx: spanCtx,
			},
		},
	}
}

func Test_AggregateRunner_LinksInputSpansAndPropagatesAggregateSpan(t *testing.T) {
	aggregateSpanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{1},
		TraceFlags: trace.FlagsSampled,
	})
	provider := newAggregateRecordingTracerProvider(aggregateSpanCtx)

	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(previousProvider)

	testEnv := newAggregateEnv(NewAggregateConfig())
	testEnv.SetTelemetry(telemetry.NewTelemetry("processor", "aggregate"))
	runner := newAggregateRunner[*aggregateTestBody](nil, nil)
	runner.SetEnvironment(testEnv)

	inputSpanContexts := []trace.SpanContext{
		trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: trace.TraceID{2}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
		}),
		trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: trace.TraceID{3}, SpanID: trace.SpanID{3}, TraceFlags: trace.FlagsSampled,
		}),
	}

	inputs := make([]*msg[*aggregateTestBody], 0, len(inputSpanContexts))
	for idx, spanCtx := range inputSpanContexts {
		msgIn := newAggregateTestMessage(idx, time.Now(), time.Now())
		msgIn.SaveSpan(&aggregateRecordingSpan{
			Span:    trace.SpanFromContext(context.Background()),
			spanCtx: spanCtx,
		})
		inputs = append(inputs, msgIn)
	}

	msgOut := runner.aggregate(t.Context(), inputs)
	t.Cleanup(msgOut.Destroy)

	assert.Equal(t, "aggregate messages", provider.tracer.name)
	assert.True(t, provider.tracer.startConfig.NewRoot())
	require.Len(t, provider.tracer.startConfig.Links(), len(inputSpanContexts))
	for idx, link := range provider.tracer.startConfig.Links() {
		assert.Equal(t, inputSpanContexts[idx], link.SpanContext)
	}
	assert.Equal(t, aggregateSpanCtx, msgOut.GetSpanContext())
	assert.True(t, provider.tracer.span.ended.Load())
	assert.Contains(t, provider.tracer.span.attributes, attribute.Int("batch_size", len(inputs)))
}
