package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Histogram represents a histogram metric.
type Histogram struct {
	histogram      metric.Int64Histogram
	measurementOpt metric.MeasurementOption
}

func newHistogram(histogram metric.Int64Histogram, measurementOpt metric.MeasurementOption) *Histogram {
	return &Histogram{
		histogram:      histogram,
		measurementOpt: measurementOpt,
	}
}

// Record records a value into the histogram.
func (h *Histogram) Record(ctx context.Context, value int64) {
	h.histogram.Record(ctx, value, h.measurementOpt)
}

type meter struct {
	m metric.Meter

	measurementOpt metric.MeasurementOption
}

func newMeter(attributes []attribute.KeyValue) *meter {
	mp := otel.GetMeterProvider()

	m := mp.Meter(
		libName,
		metric.WithInstrumentationVersion(libVersion),
		metric.WithInstrumentationAttributes(attributes...),
	)

	return &meter{
		m: m,

		measurementOpt: metric.WithAttributes(attributes...),
	}
}

// NewCouterMetric creates a new counter metric.
func (m *meter) NewCouterMetric(name string, getter func() int64, opts ...metric.Int64ObservableCounterOption) error {
	counter, err := m.m.Int64ObservableCounter(name, opts...)
	if err != nil {
		return err
	}

	measurementOpt := m.measurementOpt
	_, err = m.m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(counter, getter(), measurementOpt)
		return nil
	}, counter)

	return err
}

// NewUpDownCounterMetric creates a new up/down counter metric.
func (m *meter) NewUpDownCounterMetric(name string, getter func() int64, opts ...metric.Int64ObservableUpDownCounterOption) error {
	counter, err := m.m.Int64ObservableUpDownCounter(name, opts...)
	if err != nil {
		return err
	}

	measurementOpt := m.measurementOpt
	_, err = m.m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(counter, getter(), measurementOpt)
		return nil
	}, counter)

	return err
}

// NewHistogramMetric creates a new histogram metric.
func (m *meter) NewHistogramMetric(name string, opts ...metric.Int64HistogramOption) (*Histogram, error) {
	histogram, err := m.m.Int64Histogram(name, opts...)
	if err != nil {
		return nil, err
	}

	return newHistogram(histogram, m.measurementOpt), nil
}
