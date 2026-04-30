package worker

import (
	"context"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/stage/metrics"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

type runnerHandler[WArgs any, W Worker[WArgs]] interface {
	getWorker() (worker W, workerID int)
	handle(ctx context.Context)
}

// ─── Processor ──────────────────────────────────────────────────────────────|

type processorRunnerHandler[WArgs any, In, Out msgBody] struct {
	tel     *telemetry.Telemetry
	metrics *metrics.ProcessorStage

	workerID int
	worker   Processor[WArgs, In, Out]

	messageReader connector.MessageConnector[In]
	messageWriter connector.MessageConnector[Out]
}

func newProcessorRunnerHandler[WArgs any, In, Out msgBody](
	tel *telemetry.Telemetry, metrics *metrics.ProcessorStage,
	workerID int, worker Processor[WArgs, In, Out],
	messageReader connector.MessageConnector[In], messageWriter connector.MessageConnector[Out],
) *processorRunnerHandler[WArgs, In, Out] {

	return &processorRunnerHandler[WArgs, In, Out]{
		tel:     tel,
		metrics: metrics,

		workerID: workerID,
		worker:   worker,

		messageReader: messageReader,
		messageWriter: messageWriter,
	}
}

func (prh *processorRunnerHandler[WArgs, In, Out]) getWorker() (Processor[WArgs, In, Out], int) {
	return prh.worker, prh.workerID
}

func (prh *processorRunnerHandler[WArgs, In, Out]) handle(ctx context.Context) {
	msgIn, err := prh.messageReader.Read(ctx)
	if err != nil {
		return
	}

	defer msgIn.Destroy()

	prh.metrics.IncrementProcessedMessages()

	// Extract the span context from the input message
	ctx = msgIn.LoadSpanContext(ctx)

	msgOut, err := prh.worker.Handle(ctx, msgIn)
	if err != nil {
		prh.tel.LogError("failed to process message", err, "worker_id", prh.workerID)
		prh.metrics.IncrementProcessingErrors()

		return
	}

	// Set the receive time and timestamp
	msgOut.SetReceiveTime(msgIn.GetReceiveTime())
	msgOut.SetTimestamp(msgIn.GetTimestamp())

	// Check if the output message has to be dropped
	if msgOut.IsDropped() {
		msgOut.Destroy()

		prh.metrics.IncrementDroppedMessages()

		return
	}

	// Inject the output message
	if err := prh.messageWriter.Write(msgOut); err != nil {
		prh.tel.LogError("failed to inject message", err, "worker_id", prh.workerID)
	}
}

// ─── Egress ─────────────────────────────────────────────────────────────────|

type egressRunnerHandler[WArgs any, In msgBody] struct {
	tel     *telemetry.Telemetry
	metrics *metrics.EgressStage

	workerID int
	worker   Egress[WArgs, In]

	messageReader connector.MessageConnector[In]
}

func newEgressRunnerHandler[WArgs any, In msgBody](
	tel *telemetry.Telemetry, metrics *metrics.EgressStage,
	workerID int, worker Egress[WArgs, In],
	messageReader connector.MessageConnector[In],
) *egressRunnerHandler[WArgs, In] {

	return &egressRunnerHandler[WArgs, In]{
		tel:     tel,
		metrics: metrics,

		workerID: workerID,
		worker:   worker,

		messageReader: messageReader,
	}
}

func (erh *egressRunnerHandler[WArgs, In]) getWorker() (Egress[WArgs, In], int) {
	return erh.worker, erh.workerID
}

func (erh *egressRunnerHandler[WArgs, In]) handle(ctx context.Context) {
	msg, err := erh.messageReader.Read(ctx)
	if err != nil {
		return
	}

	defer msg.Destroy()

	// Extract the span context from the input message
	ctx = msg.LoadSpanContext(ctx)

	if err := erh.worker.Deliver(ctx, msg); err != nil {
		erh.tel.LogError("failed to deliver message", err, "worker_id", erh.workerID)
		erh.metrics.IncrementDeliveringErrors()
	}

	erh.metrics.IncrementDeliveredMessages()
	erh.metrics.RecordTotalMessageProcessingTime(ctx, int(time.Since(msg.GetReceiveTime()).Milliseconds()))
}
