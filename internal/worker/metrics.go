package worker

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/FerroO2000/goccia/internal/telemetry"
	"go.opentelemetry.io/otel/metric"
)

type processorMetrics struct {
	tel *telemetry.Telemetry

	processedMessages atomic.Int64
	droppedMessages   atomic.Int64
	processingErrors  atomic.Int64
}

func NewProcessorMetrics(tel *telemetry.Telemetry) *processorMetrics {
	return &processorMetrics{
		tel: tel,
	}
}

func (wm *processorMetrics) init() {
	wm.tel.NewCounterMetric("processed_messages", func() int64 { return wm.processedMessages.Load() })
	wm.tel.NewCounterMetric("dropped_messages", func() int64 { return wm.droppedMessages.Load() })
	wm.tel.NewCounterMetric("processing_errors", func() int64 { return wm.processingErrors.Load() })
}

func (wm *processorMetrics) incrementProcessedMessages() {
	wm.processedMessages.Add(1)
}

func (wm *processorMetrics) incrementDroppedMessages() {
	wm.droppedMessages.Add(1)
}

func (wm *processorMetrics) incrementProcessingErrors() {
	wm.processingErrors.Add(1)
}

type egressMetrics struct {
	tel *telemetry.Telemetry

	deliveredMessages atomic.Int64
	deliveringErrors  atomic.Int64

	totMsgProcessingTime *telemetry.Histogram
}

func NewEgressMetrics(tel *telemetry.Telemetry) *egressMetrics {
	return &egressMetrics{
		tel: tel,
	}
}

func (wm *egressMetrics) init() {
	wm.tel.NewCounterMetric("delivered_messages", func() int64 { return wm.deliveredMessages.Load() })
	wm.tel.NewCounterMetric("delivering_errors", func() int64 { return wm.deliveringErrors.Load() })

	totMsgProcessingTime, err := wm.tel.NewHistogramMetric("total_message_processing_time", metric.WithUnit("ms"))
	if err != nil {
		wm.tel.LogErrorCtx(context.Background(), "unable to create histogram metric", err)
	}
	wm.totMsgProcessingTime = totMsgProcessingTime
}

func (wm *egressMetrics) incrementDeliveredMessages() {
	wm.deliveredMessages.Add(1)
}

func (wm *egressMetrics) incrementDeliveringErrors() {
	wm.deliveringErrors.Add(1)
}

func (wm *egressMetrics) recordTotalMessageProcessingTime(ctx context.Context, recvTime time.Time) {
	wm.totMsgProcessingTime.Record(ctx, time.Since(recvTime).Milliseconds())
}
