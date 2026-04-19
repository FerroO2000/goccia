package worker

import (
	"context"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/stage/metrics"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

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
