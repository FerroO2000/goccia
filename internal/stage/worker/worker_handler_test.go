// Worker Handler test covered scenarios:
//
// ┌─────────────────────────────────────────────────────────────────────────────────┐
// │ Handler                      │ Scenario                  │ Expected behaviour   │
// ├─────────────────────────────────────────────────────────────────────────────────┤
// │ processorWorkerHandler       │ GetWorker                 │ returns worker + ID  │
// │                              │ Read error                │ Handle/Write skipped │
// │                              │ Handle error              │ Write skipped        │
// │                              │ Output message dropped    │ Write skipped        │
// │                              │ Happy path                │ Write called;        │
// │                              │                           │ timestamps propagated│
// │                              │ Write error               │ no panic             │
// ├─────────────────────────────────────────────────────────────────────────────────┤
// │ egressWorkerHandler          │ GetWorker                 │ returns worker + ID  │
// │                              │ Read error                │ Deliver skipped      │
// │                              │ Deliver error             │ no panic             │
// │                              │ Happy path                │ Deliver called       │
// │                              │ Records processing time   │ no panic with stale  │
// │                              │                           │ ReceiveTime          │
// └─────────────────────────────────────────────────────────────────────────────────┘

package worker

// import (
// 	"context"
// 	"errors"
// 	"testing"
// 	"time"

// 	"github.com/FerroO2000/goccia/connector"
// 	"github.com/FerroO2000/goccia/internal/message"
// 	"github.com/FerroO2000/goccia/internal/stage/metrics"
// 	"github.com/FerroO2000/goccia/internal/telemetry"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/mock"
// )

// // ─── Mock: Message Connector ────────────────────────────────────────────────|

// type messageBody struct{ Value string }

// func (b *messageBody) Destroy() {}

// var _ connector.MessageConnector[*messageBody] = (*mockReader)(nil)

// type mockReader struct{ mock.Mock }

// func newMockReader() *mockReader {
// 	return &mockReader{}
// }

// func (m *mockReader) Read(ctx context.Context) (*msg[*messageBody], error) {
// 	args := m.Called(ctx)
// 	v, _ := args.Get(0).(*msg[*messageBody])
// 	return v, args.Error(1)
// }

// func (m *mockReader) Write(item *msg[*messageBody]) error {
// 	args := m.Called(item)
// 	return args.Error(0)
// }

// func (m *mockReader) Close()                         {}
// func (m *mockReader) SetReadTimeout(_ time.Duration) {}

// var _ connector.MessageConnector[*messageBody] = (*mockWriter)(nil)

// type mockWriter struct{ mock.Mock }

// func newMockWriter() *mockWriter {
// 	return &mockWriter{}
// }

// func (m *mockWriter) Read(ctx context.Context) (*msg[*messageBody], error) {
// 	args := m.Called(ctx)
// 	v, _ := args.Get(0).(*msg[*messageBody])
// 	return v, args.Error(1)
// }

// func (m *mockWriter) Write(item *msg[*messageBody]) error {
// 	args := m.Called(item)
// 	return args.Error(0)
// }

// func (m *mockWriter) Close()                         {}
// func (m *mockWriter) SetReadTimeout(_ time.Duration) {}

// // ─── Mock: Processor Worker ─────────────────────────────────────────────────|

// type mockProcessor struct{ mock.Mock }

// func newMockProcessor() *mockProcessor {
// 	return &mockProcessor{}
// }

// func (m *mockProcessor) SetTelemetry(tel *telemetry.Telemetry) {
// 	m.Called(tel)
// }

// func (m *mockProcessor) Init(ctx context.Context, args struct{}) error {
// 	return m.Called(ctx, args).Error(0)
// }

// func (m *mockProcessor) Close(ctx context.Context) error {
// 	return m.Called(ctx).Error(0)
// }

// func (m *mockProcessor) Handle(ctx context.Context, in *msg[*messageBody]) (*msg[*messageBody], error) {
// 	args := m.Called(ctx, in)
// 	out, _ := args.Get(0).(*msg[*messageBody])
// 	return out, args.Error(1)
// }

// var _ Processor[struct{}, *messageBody, *messageBody] = (*mockProcessor)(nil)

// // ─── Mock: Egress Worker ────────────────────────────────────────────────────|

// type mockEgress struct{ mock.Mock }

// func newMockEgress() *mockEgress {
// 	return &mockEgress{}
// }

// func (m *mockEgress) SetTelemetry(tel *telemetry.Telemetry) {
// 	m.Called(tel)
// }

// func (m *mockEgress) Init(ctx context.Context, args struct{}) error {
// 	return m.Called(ctx, args).Error(0)
// }

// func (m *mockEgress) Close(ctx context.Context) error {
// 	return m.Called(ctx).Error(0)
// }

// func (m *mockEgress) Deliver(ctx context.Context, task *msg[*messageBody]) error {
// 	return m.Called(ctx, task).Error(0)
// }

// var _ Egress[struct{}, *messageBody] = (*mockEgress)(nil)

// // ─── Helpers ────────────────────────────────────────────────────────────────|

// func noopTelemetry(t *testing.T) *telemetry.Telemetry {
// 	t.Helper()
// 	return telemetry.NewTelemetry("", "")
// }

// func noopProcessorMetrics(t *testing.T) *metrics.ProcessorStage {
// 	t.Helper()
// 	m := metrics.NewProcessorStage()
// 	m.InitMetrics(noopTelemetry(t))
// 	return m
// }

// func noopEgressMetrics(t *testing.T) *metrics.EgressStage {
// 	t.Helper()
// 	m := metrics.NewEgressStage()
// 	m.InitMetrics(noopTelemetry(t))
// 	return m
// }

// func newTestMsg(body *messageBody) *msg[*messageBody] {
// 	m := message.NewMessage(body)
// 	m.SetReceiveTime(time.Now())
// 	return m
// }

// // ─── Processor Executor Handler ─────────────────────────────────────────────|

// func newTestProcessorWorkerHandler(
// 	t *testing.T,
// 	worker Processor[struct{}, *messageBody, *messageBody],
// 	workerID int,
// 	reader connector.MessageConnector[*messageBody],
// 	writer connector.MessageConnector[*messageBody],
// ) *processorWorkerHandler[struct{}, *messageBody, *messageBody, Processor[struct{}, *messageBody, *messageBody]] {
// 	return newProcessorWorkerHandler(
// 		noopTelemetry(t), noopProcessorMetrics(t), workerID, worker, reader, writer,
// 	)
// }

// func Test_ProcessorWorkerHandler_GetWorker(t *testing.T) {
// 	worker := newMockProcessor()
// 	reader := newMockReader()
// 	writer := newMockWriter()

// 	handler := newTestProcessorWorkerHandler(t, worker, 42, reader, writer)

// 	gotWorker, gotID := handler.getWorker()
// 	assert.Equal(t, worker, gotWorker)
// 	assert.Equal(t, 42, gotID)
// }

// func Test_ProcessorWorkerHandler_Handle_ReadError(t *testing.T) {
// 	worker := newMockProcessor()
// 	reader := newMockReader()
// 	writer := newMockWriter()

// 	reader.On("Read", mock.Anything).Return(nil, errors.New("read error"))

// 	handler := newTestProcessorWorkerHandler(t, worker, 1, reader, writer)

// 	handler.handle(t.Context())

// 	reader.AssertExpectations(t)
// 	worker.AssertNotCalled(t, "Handle", mock.Anything, mock.Anything)
// 	writer.AssertNotCalled(t, "Write", mock.Anything)
// }

// func Test_ProcessorWorkerHandler_Handle_WorkerError(t *testing.T) {
// 	worker := newMockProcessor()
// 	reader := newMockReader()
// 	writer := newMockWriter()

// 	msgIn := newTestMsg(&messageBody{Value: "in"})

// 	reader.On("Read", mock.Anything).Return(msgIn, nil)
// 	worker.On("Handle", mock.Anything, msgIn).Return(nil, errors.New("handle error"))

// 	handler := newTestProcessorWorkerHandler(t, worker, 1, reader, writer)

// 	handler.handle(t.Context())

// 	reader.AssertExpectations(t)
// 	worker.AssertExpectations(t)
// 	writer.AssertNotCalled(t, "Write", mock.Anything)
// }

// func Test_ProcessorWorkerHandler_Handle_DroppedMessage(t *testing.T) {
// 	worker := newMockProcessor()
// 	reader := newMockReader()
// 	writer := newMockWriter()

// 	msgIn := newTestMsg(&messageBody{Value: "in"})
// 	msgOut := newTestMsg(&messageBody{Value: "out"})
// 	msgOut.Drop()

// 	reader.On("Read", mock.Anything).Return(msgIn, nil)
// 	worker.On("Handle", mock.Anything, msgIn).Return(msgOut, nil)

// 	handler := newTestProcessorWorkerHandler(t, worker, 1, reader, writer)

// 	handler.handle(t.Context())

// 	reader.AssertExpectations(t)
// 	worker.AssertExpectations(t)
// 	writer.AssertNotCalled(t, "Write", mock.Anything)
// }

// func Test_ProcessorWorkerHandler_Handle_HappyPath(t *testing.T) {
// 	worker := newMockProcessor()
// 	reader := newMockReader()
// 	writer := newMockWriter()

// 	msgIn := newTestMsg(&messageBody{Value: "in"})
// 	msgOut := newTestMsg(&messageBody{Value: "out"})

// 	reader.On("Read", mock.Anything).Return(msgIn, nil)
// 	worker.On("Handle", mock.Anything, msgIn).Return(msgOut, nil)
// 	writer.On("Write", msgOut).Return(nil)

// 	handler := newTestProcessorWorkerHandler(t, worker, 1, reader, writer)

// 	handler.handle(t.Context())

// 	reader.AssertExpectations(t)
// 	worker.AssertExpectations(t)
// 	writer.AssertExpectations(t)

// 	assert.Equal(t, msgIn.GetReceiveTime(), msgOut.GetReceiveTime())
// 	assert.Equal(t, msgIn.GetTimestamp(), msgOut.GetTimestamp())
// }

// func Test_ProcessorWorkerHandler_Handle_WriteError(t *testing.T) {
// 	worker := newMockProcessor()
// 	reader := newMockReader()
// 	writer := newMockWriter()

// 	msgIn := newTestMsg(&messageBody{Value: "in"})
// 	msgOut := newTestMsg(&messageBody{Value: "out"})

// 	reader.On("Read", mock.Anything).Return(msgIn, nil)
// 	worker.On("Handle", mock.Anything, msgIn).Return(msgOut, nil)
// 	writer.On("Write", msgOut).Return(errors.New("write error"))

// 	handler := newTestProcessorWorkerHandler(t, worker, 1, reader, writer)

// 	assert.NotPanics(t, func() {
// 		handler.handle(t.Context())
// 	})

// 	reader.AssertExpectations(t)
// 	worker.AssertExpectations(t)
// 	writer.AssertExpectations(t)
// }

// // ─── Egress Executor Handler ────────────────────────────────────────────────|

// func newTestEgressWorkerHandler(
// 	t *testing.T,
// 	worker Egress[struct{}, *messageBody],
// 	workerID int,
// 	reader connector.MessageConnector[*messageBody],
// ) *egressWorkerHandler[struct{}, *messageBody, Egress[struct{}, *messageBody]] {
// 	return newEgressWorkerHandler(
// 		noopTelemetry(t), noopEgressMetrics(t), workerID, worker, reader,
// 	)
// }

// func Test_EgressWorkerHandler_GetWorker(t *testing.T) {
// 	worker := newMockEgress()
// 	reader := newMockReader()

// 	handler := newTestEgressWorkerHandler(t, worker, 99, reader)

// 	gotWorker, gotID := handler.getWorker()
// 	assert.Equal(t, worker, gotWorker)
// 	assert.Equal(t, 99, gotID)
// }

// func Test_EgressWorkerHandler_Handle_ReadError(t *testing.T) {
// 	worker := newMockEgress()
// 	reader := newMockReader()

// 	reader.On("Read", mock.Anything).Return(nil, errors.New("read error"))

// 	handler := newTestEgressWorkerHandler(t, worker, 1, reader)

// 	handler.handle(t.Context())

// 	reader.AssertExpectations(t)
// 	worker.AssertNotCalled(t, "Deliver", mock.Anything, mock.Anything)
// }

// func Test_EgressWorkerHandler_Handle_DeliverError(t *testing.T) {
// 	worker := newMockEgress()
// 	reader := newMockReader()

// 	msgIn := newTestMsg(&messageBody{Value: "payload"})

// 	reader.On("Read", mock.Anything).Return(msgIn, nil)
// 	worker.On("Deliver", mock.Anything, msgIn).Return(errors.New("deliver error"))

// 	handler := newTestEgressWorkerHandler(t, worker, 1, reader)

// 	assert.NotPanics(t, func() {
// 		handler.handle(t.Context())
// 	})

// 	reader.AssertExpectations(t)
// 	worker.AssertExpectations(t)
// }

// func Test_EgressWorkerHandler_Handle_HappyPath(t *testing.T) {
// 	worker := newMockEgress()
// 	reader := newMockReader()

// 	msgIn := newTestMsg(&messageBody{Value: "payload"})

// 	reader.On("Read", mock.Anything).Return(msgIn, nil)
// 	worker.On("Deliver", mock.Anything, msgIn).Return(nil)

// 	handler := newTestEgressWorkerHandler(t, worker, 1, reader)

// 	handler.handle(t.Context())

// 	reader.AssertExpectations(t)
// 	worker.AssertExpectations(t)
// }

// func Test_EgressWorkerHandler_Handle_RecordsProcessingTime(t *testing.T) {
// 	worker := newMockEgress()
// 	reader := newMockReader()

// 	msgIn := newTestMsg(&messageBody{Value: "payload"})
// 	msgIn.SetReceiveTime(time.Now().Add(-50 * time.Millisecond))

// 	reader.On("Read", mock.Anything).Return(msgIn, nil)
// 	worker.On("Deliver", mock.Anything, msgIn).Return(nil)

// 	handler := newTestEgressWorkerHandler(t, worker, 1, reader)

// 	assert.NotPanics(t, func() {
// 		handler.handle(t.Context())
// 	})

// 	reader.AssertExpectations(t)
// 	worker.AssertExpectations(t)
// }
