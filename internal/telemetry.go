package internal

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const providerName = "goccia"

type Telemetry struct {
	stageKind string
	stageName string

	l *Logger

	tracer          trace.Tracer
	tracePropagator propagation.TextMapPropagator

	meter metric.Meter
}

func NewTelemetry(stageKind, stageName string) *Telemetry {
	return &Telemetry{
		stageKind: stageKind,
		stageName: stageName,

		l: NewLogger(stageKind, stageName),

		tracer:          otel.GetTracerProvider().Tracer(providerName),
		tracePropagator: otel.GetTextMapPropagator(),

		meter: otel.GetMeterProvider().Meter(providerName),
	}
}

func (t *Telemetry) Logger() *Logger {
	return t.l
}

func (t *Telemetry) LogInfo(msg string, args ...any) {
	t.l.Info(msg, args...)
}

func (t *Telemetry) LogWarn(msg string, args ...any) {
	t.l.Warn(msg, args...)
}

func (t *Telemetry) LogError(msg string, err error, args ...any) {
	t.l.Error(msg, err, args...)
}

func (t *Telemetry) setSpanDefaultAttributes(span trace.Span) {
	span.SetAttributes(
		attribute.String("goccia.stage_kind", t.stageKind),
		attribute.String("goccia.stage_name", t.stageName),
	)
}

func (t *Telemetry) NewTrace(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	ctx, span := t.tracer.Start(ctx, spanName, opts...)
	t.setSpanDefaultAttributes(span)
	return ctx, span
}

func (t *Telemetry) getMetricDefaultAttributes() metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.String("goccia.stage_kind", t.stageKind),
		attribute.String("goccia.stage_name", t.stageName),
	)
}

func (t *Telemetry) InjectTrace(ctx context.Context, carrier propagation.TextMapCarrier) {
	t.tracePropagator.Inject(ctx, carrier)
}

func (t *Telemetry) ExtractTraceContext(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return t.tracePropagator.Extract(ctx, carrier)
}

func (t *Telemetry) NewCounter(name string, getter func() int64, opts ...metric.Int64ObservableCounterOption) {
	counter, err := t.meter.Int64ObservableCounter(name, opts...)
	if err != nil {
		t.LogError("failed to create async counter", err, "name", name)
	}

	_, err = t.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(counter, getter(), t.getMetricDefaultAttributes())
		return nil
	}, counter)

	if err != nil {
		t.LogError("failed to register callback", err, "name", name)
	}

	t.LogInfo("created async counter", "name", name)
}

func (t *Telemetry) NewUpDownCounter(name string, getter func() int64, opts ...metric.Int64ObservableUpDownCounterOption) {
	counter, err := t.meter.Int64ObservableUpDownCounter(name, opts...)
	if err != nil {
		t.LogError("failed to create async up/down counter", err, "name", name)
	}

	_, err = t.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(counter, getter(), t.getMetricDefaultAttributes())
		return nil
	}, counter)

	if err != nil {
		t.LogError("failed to register callback", err, "name", name)
	}

	t.LogInfo("created async up/down counter", "name", name)
}

type Histogram struct {
	histogram    metric.Int64Histogram
	attibutesOpt metric.MeasurementOption
}

func newHistogram(histogram metric.Int64Histogram, attibutesOpt metric.MeasurementOption) *Histogram {
	return &Histogram{
		histogram:    histogram,
		attibutesOpt: attibutesOpt,
	}
}

func (h *Histogram) Record(ctx context.Context, value int64) {
	h.histogram.Record(ctx, value, h.attibutesOpt)
}

func (t *Telemetry) NewHistogram(name string, opts ...metric.Int64HistogramOption) *Histogram {
	histogram, err := t.meter.Int64Histogram(name, opts...)
	if err != nil {
		t.LogError("failed to create histogram", err, "name", name)
	}
	t.LogInfo("created histogram", "name", name)
	return newHistogram(histogram, t.getMetricDefaultAttributes())
}
