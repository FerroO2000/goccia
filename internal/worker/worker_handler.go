package worker

import (
	"context"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/stage/metrics"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

type workerHandler[WArgs any, W Worker[WArgs]] interface {
	getWorker() (worker W, workerID int)
	handle(ctx context.Context)
}

// ─── Processor ──────────────────────────────────────────────────────────────|

type processorWorkerHandler[WArgs any, In, Out msgBody] struct {
	tel          *telemetry.Telemetry
	stageMetrics *metrics.ProcessorStage

	workerID int
	worker   Processor[WArgs, In, Out]

	messageReader connector.MessageConnector[In]
	messageWriter connector.MessageConnector[Out]
}

func newProcessorWorkerHandler[WArgs any, In, Out msgBody](
	tel *telemetry.Telemetry, metrics *metrics.ProcessorStage,
	workerID int, worker Processor[WArgs, In, Out],
	messageReader connector.MessageConnector[In], messageWriter connector.MessageConnector[Out],
) *processorWorkerHandler[WArgs, In, Out] {

	return &processorWorkerHandler[WArgs, In, Out]{
		tel:          tel,
		stageMetrics: metrics,

		workerID: workerID,
		worker:   worker,

		messageReader: messageReader,
		messageWriter: messageWriter,
	}
}

func (pwh *processorWorkerHandler[WArgs, In, Out]) getWorker() (Processor[WArgs, In, Out], int) {
	return pwh.worker, pwh.workerID
}

func (pwh *processorWorkerHandler[WArgs, In, Out]) handle(ctx context.Context) {
	msgIn, err := pwh.messageReader.Read(ctx)
	if err != nil {
		return
	}

	defer msgIn.Destroy()

	pwh.stageMetrics.IncrementProcessedMessages()

	// Extract the span context from the input message
	ctx = msgIn.LoadSpanContext(ctx)

	msgOut, err := pwh.worker.Handle(ctx, msgIn)
	if err != nil {
		pwh.tel.LogError("failed to process message", err, "worker_id", pwh.workerID)
		pwh.stageMetrics.IncrementProcessingErrors()

		return
	}

	// Set the receive time and timestamp
	msgOut.SetReceiveTime(msgIn.GetReceiveTime())
	msgOut.SetTimestamp(msgIn.GetTimestamp())

	// Check if the output message has to be dropped
	if msgOut.IsDropped() {
		msgOut.Destroy()

		pwh.stageMetrics.IncrementDroppedMessages()

		return
	}

	// Inject the output message
	if err := pwh.messageWriter.Write(msgOut); err != nil {
		pwh.tel.LogError("failed to inject message", err, "worker_id", pwh.workerID)
	}
}

// ─── Egress ─────────────────────────────────────────────────────────────────|

type egressWorkerHandler[WArgs any, In msgBody] struct {
	tel          *telemetry.Telemetry
	stageMetrics *metrics.EgressStage

	workerID int
	worker   Egress[WArgs, In]

	messageReader connector.MessageConnector[In]
}

func newEgressWorkerHandler[WArgs any, In msgBody](
	tel *telemetry.Telemetry, metrics *metrics.EgressStage,
	workerID int, worker Egress[WArgs, In],
	messageReader connector.MessageConnector[In],
) *egressWorkerHandler[WArgs, In] {

	return &egressWorkerHandler[WArgs, In]{
		tel:          tel,
		stageMetrics: metrics,

		workerID: workerID,
		worker:   worker,

		messageReader: messageReader,
	}
}

func (ewh *egressWorkerHandler[WArgs, In]) getWorker() (Egress[WArgs, In], int) {
	return ewh.worker, ewh.workerID
}

func (ewh *egressWorkerHandler[WArgs, In]) handle(ctx context.Context) {
	msg, err := ewh.messageReader.Read(ctx)
	if err != nil {
		return
	}

	defer msg.Destroy()

	// Extract the span context from the input message
	ctx = msg.LoadSpanContext(ctx)

	if err := ewh.worker.Deliver(ctx, msg); err != nil {
		ewh.tel.LogError("failed to deliver message", err, "worker_id", ewh.workerID)
		ewh.stageMetrics.IncrementDeliveringErrors()
	}

	ewh.stageMetrics.IncrementDeliveredMessages()
	ewh.stageMetrics.RecordTotalMessageProcessingTime(ctx, int(time.Since(msg.GetReceiveTime()).Milliseconds()))
}
