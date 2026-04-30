// Package worker contains tests for processorRunnerHandler and egressRunnerHandler.
//
// Covered scenarios:
//
// ┌─────────────────────────────────────────────────────────────────────────────────┐
// │ Handler                      │ Scenario                  │ Expected behaviour   │
// ├─────────────────────────────────────────────────────────────────────────────────┤
// │ processorRunnerHandler       │ GetWorker                 │ returns worker + ID  │
// │                              │ Read error                │ Handle/Write skipped │
// │                              │ Handle error              │ Write skipped        │
// │                              │ Output message dropped    │ Write skipped        │
// │                              │ Happy path                │ Write called;        │
// │                              │                           │ timestamps propagated│
// │                              │ Write error               │ no panic             │
// ├─────────────────────────────────────────────────────────────────────────────────┤
// │ egressRunnerHandler          │ GetWorker                 │ returns worker + ID  │
// │                              │ Read error                │ Deliver skipped      │
// │                              │ Deliver error             │ no panic             │
// │                              │ Happy path                │ Deliver called       │
// │                              │ Records processing time   │ no panic with stale  │
// │                              │                           │ ReceiveTime          │
// └─────────────────────────────────────────────────────────────────────────────────┘

package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/stage/metrics"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ─── Shared test body type ───────────────────────────────────────────────────

type testBody struct{ Value string }

// Destroy implements message.Body.
func (b *testBody) Destroy() {}

// ─── Mock: connector.MessageConnector ───────────────────────────────────────

type mockReader[T msgBody] struct{ mock.Mock }

func (m *mockReader[T]) Read(ctx context.Context) (*msg[T], error) {
	args := m.Called(ctx)
	v, _ := args.Get(0).(*msg[T])
	return v, args.Error(1)
}

func (m *mockReader[T]) Write(item *msg[T]) error {
	args := m.Called(item)
	return args.Error(0)
}

func (m *mockReader[T]) Close()                         {}
func (m *mockReader[T]) SetReadTimeout(_ time.Duration) {}

var _ connector.MessageConnector[*testBody] = (*mockReader[*testBody])(nil)

type mockWriter[T msgBody] struct{ mock.Mock }

func (m *mockWriter[T]) Read(ctx context.Context) (*msg[T], error) {
	args := m.Called(ctx)
	v, _ := args.Get(0).(*msg[T])
	return v, args.Error(1)
}

func (m *mockWriter[T]) Write(item *msg[T]) error {
	args := m.Called(item)
	return args.Error(0)
}

func (m *mockWriter[T]) Close()                         {}
func (m *mockWriter[T]) SetReadTimeout(_ time.Duration) {}

var _ connector.MessageConnector[*testBody] = (*mockWriter[*testBody])(nil)

// ─── Mock: Processor worker ──────────────────────────────────────────────────

type mockProcessor[WArgs any, In, Out msgBody] struct{ mock.Mock }

func (m *mockProcessor[WArgs, In, Out]) SetTelemetry(tel *telemetry.Telemetry) {
	m.Called(tel)
}

func (m *mockProcessor[WArgs, In, Out]) Init(ctx context.Context, args WArgs) error {
	return m.Called(ctx, args).Error(0)
}

func (m *mockProcessor[WArgs, In, Out]) Close(ctx context.Context) error {
	return m.Called(ctx).Error(0)
}

func (m *mockProcessor[WArgs, In, Out]) Handle(ctx context.Context, in *msg[In]) (*msg[Out], error) {
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*msg[Out])
	return out, args.Error(1)
}

var _ Processor[struct{}, *testBody, *testBody] = (*mockProcessor[struct{}, *testBody, *testBody])(nil)

// ─── Mock: Egress worker ─────────────────────────────────────────────────────

type mockEgress[WArgs any, In msgBody] struct{ mock.Mock }

func (m *mockEgress[WArgs, In]) SetTelemetry(tel *telemetry.Telemetry) {
	m.Called(tel)
}

func (m *mockEgress[WArgs, In]) Init(ctx context.Context, args WArgs) error {
	return m.Called(ctx, args).Error(0)
}

func (m *mockEgress[WArgs, In]) Close(ctx context.Context) error {
	return m.Called(ctx).Error(0)
}

func (m *mockEgress[WArgs, In]) Deliver(ctx context.Context, task *msg[In]) error {
	return m.Called(ctx, task).Error(0)
}

var _ Egress[struct{}, *testBody] = (*mockEgress[struct{}, *testBody])(nil)

// ─── Helpers ─────────────────────────────────────────────────────────────────

func noopTelemetry(t *testing.T) *telemetry.Telemetry {
	t.Helper()
	return telemetry.NewTelemetry("", "")
}

func noopProcessorMetrics(t *testing.T) *metrics.ProcessorStage {
	t.Helper()
	m := metrics.NewProcessorStage()
	m.InitMetrics(noopTelemetry(t))
	return m
}

func noopEgressMetrics(t *testing.T) *metrics.EgressStage {
	t.Helper()
	m := metrics.NewEgressStage()
	m.InitMetrics(noopTelemetry(t))
	return m
}

func newTestMsg(body *testBody) *msg[*testBody] {
	m := message.NewMessage(body)
	m.SetReceiveTime(time.Now())
	return m
}

// ─── processorRunnerHandler tests ────────────────────────────────────────────

func Test_ProcessorRunnerHandler_GetWorker(t *testing.T) {
	worker := &mockProcessor[struct{}, *testBody, *testBody]{}
	handler := newProcessorRunnerHandler[struct{}, *testBody, *testBody](
		noopTelemetry(t),
		noopProcessorMetrics(t),
		42,
		worker,
		&mockReader[*testBody]{},
		&mockWriter[*testBody]{},
	)

	gotWorker, gotID := handler.getWorker()
	assert.Equal(t, worker, gotWorker)
	assert.Equal(t, 42, gotID)
}

func Test_ProcessorRunnerHandler_Handle_ReadError(t *testing.T) {
	reader := &mockReader[*testBody]{}
	writer := &mockWriter[*testBody]{}
	worker := &mockProcessor[struct{}, *testBody, *testBody]{}

	reader.On("Read", mock.Anything).Return(nil, errors.New("read error"))

	handler := newProcessorRunnerHandler[struct{}, *testBody, *testBody](
		noopTelemetry(t),
		noopProcessorMetrics(t),
		1,
		worker,
		reader,
		writer,
	)

	handler.handle(context.Background())

	reader.AssertExpectations(t)
	worker.AssertNotCalled(t, "Handle", mock.Anything, mock.Anything)
	writer.AssertNotCalled(t, "Write", mock.Anything)
}

func Test_ProcessorRunnerHandler_Handle_WorkerError(t *testing.T) {
	msgIn := newTestMsg(&testBody{Value: "in"})

	reader := &mockReader[*testBody]{}
	writer := &mockWriter[*testBody]{}
	worker := &mockProcessor[struct{}, *testBody, *testBody]{}

	reader.On("Read", mock.Anything).Return(msgIn, nil)
	worker.On("Handle", mock.Anything, msgIn).Return(nil, errors.New("handle error"))

	handler := newProcessorRunnerHandler[struct{}, *testBody, *testBody](
		noopTelemetry(t),
		noopProcessorMetrics(t),
		1,
		worker,
		reader,
		writer,
	)

	handler.handle(context.Background())

	reader.AssertExpectations(t)
	worker.AssertExpectations(t)
	writer.AssertNotCalled(t, "Write", mock.Anything)
}

func Test_ProcessorRunnerHandler_Handle_DroppedMessage(t *testing.T) {
	msgIn := newTestMsg(&testBody{Value: "in"})
	msgOut := newTestMsg(&testBody{Value: "out"})
	msgOut.Drop()

	reader := &mockReader[*testBody]{}
	writer := &mockWriter[*testBody]{}
	worker := &mockProcessor[struct{}, *testBody, *testBody]{}

	reader.On("Read", mock.Anything).Return(msgIn, nil)
	worker.On("Handle", mock.Anything, msgIn).Return(msgOut, nil)

	handler := newProcessorRunnerHandler[struct{}, *testBody, *testBody](
		noopTelemetry(t),
		noopProcessorMetrics(t),
		1,
		worker,
		reader,
		writer,
	)

	handler.handle(context.Background())

	reader.AssertExpectations(t)
	worker.AssertExpectations(t)
	writer.AssertNotCalled(t, "Write", mock.Anything)
}

func Test_ProcessorRunnerHandler_Handle_HappyPath(t *testing.T) {
	msgIn := newTestMsg(&testBody{Value: "in"})
	msgOut := newTestMsg(&testBody{Value: "out"})

	reader := &mockReader[*testBody]{}
	writer := &mockWriter[*testBody]{}
	worker := &mockProcessor[struct{}, *testBody, *testBody]{}

	reader.On("Read", mock.Anything).Return(msgIn, nil)
	worker.On("Handle", mock.Anything, msgIn).Return(msgOut, nil)
	writer.On("Write", msgOut).Return(nil)

	handler := newProcessorRunnerHandler[struct{}, *testBody, *testBody](
		noopTelemetry(t),
		noopProcessorMetrics(t),
		1,
		worker,
		reader,
		writer,
	)

	handler.handle(context.Background())

	reader.AssertExpectations(t)
	worker.AssertExpectations(t)
	writer.AssertExpectations(t)

	assert.Equal(t, msgIn.GetReceiveTime(), msgOut.GetReceiveTime())
	assert.Equal(t, msgIn.GetTimestamp(), msgOut.GetTimestamp())
}

func Test_ProcessorRunnerHandler_Handle_WriteError(t *testing.T) {
	msgIn := newTestMsg(&testBody{Value: "in"})
	msgOut := newTestMsg(&testBody{Value: "out"})

	reader := &mockReader[*testBody]{}
	writer := &mockWriter[*testBody]{}
	worker := &mockProcessor[struct{}, *testBody, *testBody]{}

	reader.On("Read", mock.Anything).Return(msgIn, nil)
	worker.On("Handle", mock.Anything, msgIn).Return(msgOut, nil)
	writer.On("Write", msgOut).Return(errors.New("write error"))

	handler := newProcessorRunnerHandler[struct{}, *testBody, *testBody](
		noopTelemetry(t),
		noopProcessorMetrics(t),
		1,
		worker,
		reader,
		writer,
	)

	assert.NotPanics(t, func() {
		handler.handle(context.Background())
	})

	reader.AssertExpectations(t)
	worker.AssertExpectations(t)
	writer.AssertExpectations(t)
}

// ─── egressRunnerHandler tests ───────────────────────────────────────────────

func Test_EgressRunnerHandler_GetWorker(t *testing.T) {
	worker := &mockEgress[struct{}, *testBody]{}
	handler := newEgressRunnerHandler[struct{}, *testBody](
		noopTelemetry(t),
		noopEgressMetrics(t),
		99,
		worker,
		&mockReader[*testBody]{},
	)

	gotWorker, gotID := handler.getWorker()
	assert.Equal(t, worker, gotWorker)
	assert.Equal(t, 99, gotID)
}

func Test_EgressRunnerHandler_Handle_ReadError(t *testing.T) {
	reader := &mockReader[*testBody]{}
	worker := &mockEgress[struct{}, *testBody]{}

	reader.On("Read", mock.Anything).Return(nil, errors.New("read error"))

	handler := newEgressRunnerHandler[struct{}, *testBody](
		noopTelemetry(t),
		noopEgressMetrics(t),
		1,
		worker,
		reader,
	)

	handler.handle(context.Background())

	reader.AssertExpectations(t)
	worker.AssertNotCalled(t, "Deliver", mock.Anything, mock.Anything)
}

func Test_EgressRunnerHandler_Handle_DeliverError(t *testing.T) {
	msgIn := newTestMsg(&testBody{Value: "payload"})

	reader := &mockReader[*testBody]{}
	worker := &mockEgress[struct{}, *testBody]{}

	reader.On("Read", mock.Anything).Return(msgIn, nil)
	worker.On("Deliver", mock.Anything, msgIn).Return(errors.New("deliver error"))

	handler := newEgressRunnerHandler[struct{}, *testBody](
		noopTelemetry(t),
		noopEgressMetrics(t),
		1,
		worker,
		reader,
	)

	assert.NotPanics(t, func() {
		handler.handle(context.Background())
	})

	reader.AssertExpectations(t)
	worker.AssertExpectations(t)
}

func Test_EgressRunnerHandler_Handle_HappyPath(t *testing.T) {
	msgIn := newTestMsg(&testBody{Value: "payload"})

	reader := &mockReader[*testBody]{}
	worker := &mockEgress[struct{}, *testBody]{}

	reader.On("Read", mock.Anything).Return(msgIn, nil)
	worker.On("Deliver", mock.Anything, msgIn).Return(nil)

	handler := newEgressRunnerHandler[struct{}, *testBody](
		noopTelemetry(t),
		noopEgressMetrics(t),
		1,
		worker,
		reader,
	)

	handler.handle(context.Background())

	reader.AssertExpectations(t)
	worker.AssertExpectations(t)
}

func Test_EgressRunnerHandler_Handle_RecordsProcessingTime(t *testing.T) {
	msgIn := newTestMsg(&testBody{Value: "payload"})
	msgIn.SetReceiveTime(time.Now().Add(-50 * time.Millisecond))

	reader := &mockReader[*testBody]{}
	worker := &mockEgress[struct{}, *testBody]{}

	reader.On("Read", mock.Anything).Return(msgIn, nil)
	worker.On("Deliver", mock.Anything, msgIn).Return(nil)

	handler := &egressRunnerHandler[struct{}, *testBody]{
		tel:           noopTelemetry(t),
		metrics:       noopEgressMetrics(t),
		workerID:      1,
		worker:        worker,
		messageReader: reader,
	}

	assert.NotPanics(t, func() {
		handler.handle(context.Background())
	})

	reader.AssertExpectations(t)
	worker.AssertExpectations(t)
}
