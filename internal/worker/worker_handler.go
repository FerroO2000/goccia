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
	handle(ctx context.Context) error
}

// ─── Processor ──────────────────────────────────────────────────────────────|

type processorWorkerHandler[WArgs any, In, Out msgBody, W Processor[WArgs, In, Out]] struct {
	tel          *telemetry.Telemetry
	stageMetrics *metrics.ProcessorStage

	workerID int
	worker   W

	messageReader connector.MessageConnector[In]
	messageWriter connector.MessageConnector[Out]
}

func newProcessorWorkerHandler[WArgs any, In, Out msgBody, W Processor[WArgs, In, Out]](
	tel *telemetry.Telemetry, metrics *metrics.ProcessorStage,
	workerID int, worker W,
	messageReader connector.MessageConnector[In], messageWriter connector.MessageConnector[Out],
) *processorWorkerHandler[WArgs, In, Out, W] {

	return &processorWorkerHandler[WArgs, In, Out, W]{
		tel:          tel,
		stageMetrics: metrics,

		workerID: workerID,
		worker:   worker,

		messageReader: messageReader,
		messageWriter: messageWriter,
	}
}

func (pwh *processorWorkerHandler[WArgs, In, Out, W]) getWorker() (W, int) {
	return pwh.worker, pwh.workerID
}

func (pwh *processorWorkerHandler[WArgs, In, Out, W]) handle(ctx context.Context) error {
	msgIn, err := pwh.messageReader.Read(ctx)
	if err != nil {
		return err
	}

	defer msgIn.Destroy()

	pwh.stageMetrics.IncrementProcessedMessages()

	// Extract the span context from the input message
	ctx = msgIn.LoadSpanContext(ctx)

	msgOut, err := pwh.worker.Handle(ctx, msgIn)
	if err != nil {
		pwh.tel.LogError("failed to process message", err, "worker_id", pwh.workerID)
		pwh.stageMetrics.IncrementProcessingErrors()

		return nil
	}

	// Set the receive time and timestamp
	msgOut.SetReceiveTime(msgIn.GetReceiveTime())
	msgOut.SetTimestamp(msgIn.GetTimestamp())

	// Check if the output message has to be dropped
	if msgOut.IsDropped() {
		msgOut.Destroy()

		pwh.stageMetrics.IncrementDroppedMessages()

		return nil
	}

	return pwh.messageWriter.Write(msgOut)
}

// ─── Egress ─────────────────────────────────────────────────────────────────|

type egressWorkerHandler[WArgs any, In msgBody, W Egress[WArgs, In]] struct {
	tel          *telemetry.Telemetry
	stageMetrics *metrics.EgressStage

	workerID int
	worker   W

	messageReader connector.MessageConnector[In]
}

func newEgressWorkerHandler[WArgs any, In msgBody, W Egress[WArgs, In]](
	tel *telemetry.Telemetry, metrics *metrics.EgressStage,
	workerID int, worker W,
	messageReader connector.MessageConnector[In],
) *egressWorkerHandler[WArgs, In, W] {

	return &egressWorkerHandler[WArgs, In, W]{
		tel:          tel,
		stageMetrics: metrics,

		workerID: workerID,
		worker:   worker,

		messageReader: messageReader,
	}
}

func (ewh *egressWorkerHandler[WArgs, In, W]) getWorker() (W, int) {
	return ewh.worker, ewh.workerID
}

func (ewh *egressWorkerHandler[WArgs, In, W]) handle(ctx context.Context) error {
	msg, err := ewh.messageReader.Read(ctx)
	if err != nil {
		return err
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

	return nil
}
