package worker

import (
	"context"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

type processorRunnerHandler[WArgs any, In, Out msgBody] struct {
	tel     *telemetry.Telemetry
	metrics *processorMetrics

	workerID int
	worker   Processor[WArgs, In, Out]

	messageReader connector.MessageConnector[In]
	messageWriter connector.MessageConnector[Out]
}

func newProcessorRunnerHandler[WArgs any, In, Out msgBody](
	tel *telemetry.Telemetry, metrics *processorMetrics,
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

func (prh *processorRunnerHandler[WArgs, In, Out]) handle(ctx context.Context) {
	msgIn, err := prh.messageReader.Read(ctx)
	if err != nil {
		return
	}

	defer msgIn.Destroy()

	prh.metrics.incrementProcessedMessages()

	// Extract the span context from the input message
	ctx = msgIn.LoadSpanContext(ctx)

	msgOut, err := prh.worker.Handle(ctx, msgIn)
	if err != nil {
		prh.tel.LogError("failed to process message", err, "worker_id", prh.workerID)
		prh.metrics.incrementProcessingErrors()

		return
	}

	// Set the receive time and timestamp
	msgOut.SetReceiveTime(msgIn.GetReceiveTime())
	msgOut.SetTimestamp(msgIn.GetTimestamp())

	// Check if the output message has to be dropped
	if msgOut.IsDropped() {
		msgOut.Destroy()

		prh.metrics.incrementDroppedMessages()

		return
	}

	// Inject the output message
	if err := prh.messageWriter.Write(msgOut); err != nil {
		prh.tel.LogError("failed to inject message", err, "worker_id", prh.workerID)
	}
}
