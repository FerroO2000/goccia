package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// IntHistogram represents an integer histogram metric.
type IntHistogram struct {
	histogram      metric.Int64Histogram
	measurementOpt metric.MeasurementOption
}

func newIntHistogram(histogram metric.Int64Histogram, measurementOpt metric.MeasurementOption) *IntHistogram {
	return &IntHistogram{
		histogram:      histogram,
		measurementOpt: measurementOpt,
	}
}

// Record records a value into the histogram.
func (h *IntHistogram) Record(ctx context.Context, value int64) {
	h.histogram.Record(ctx, value, h.measurementOpt)
}

// FloatHistogram represents a float histogram metric.
type FloatHistogram struct {
	histogram      metric.Float64Histogram
	measurementOpt metric.MeasurementOption
}

func newFloatHistogram(histogram metric.Float64Histogram, measurementOpt metric.MeasurementOption) *FloatHistogram {
	return &FloatHistogram{
		histogram:      histogram,
		measurementOpt: measurementOpt,
	}
}

// Record records a value into the histogram.
func (h *FloatHistogram) Record(ctx context.Context, value float64) {
	h.histogram.Record(ctx, value, h.measurementOpt)
}

// CounterMetricDataPoint defines a single observable counter data point.
type CounterMetricDataPoint struct {
	Getter     func() int64
	Attributes []attribute.KeyValue
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

// NewCounterMetric creates a new counter metric.
func (m *meter) NewCounterMetric(name string, getter func() int64, opts ...metric.Int64ObservableCounterOption) error {
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

// NewCounterMetricSet creates a new counter metric with multiple observable data points.
func (m *meter) NewCounterMetricSet(name string, dataPoints []CounterMetricDataPoint, opts ...metric.Int64ObservableCounterOption) error {
	counter, err := m.m.Int64ObservableCounter(name, opts...)
	if err != nil {
		return err
	}

	measurementOpt := m.measurementOpt
	attributeOpts := make([]metric.MeasurementOption, 0, len(dataPoints))
	for _, dataPoint := range dataPoints {
		attributeOpts = append(attributeOpts, metric.WithAttributes(dataPoint.Attributes...))
	}

	_, err = m.m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for id, dataPoint := range dataPoints {
			o.ObserveInt64(counter, dataPoint.Getter(), measurementOpt, attributeOpts[id])
		}

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

// NewGaugeMetric creates a new observable integer gauge metric.
func (m *meter) NewGaugeMetric(name string, getter func() int64, opts ...metric.Int64ObservableGaugeOption) error {
	gauge, err := m.m.Int64ObservableGauge(name, opts...)
	if err != nil {
		return err
	}

	measurementOpt := m.measurementOpt
	_, err = m.m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(gauge, getter(), measurementOpt)
		return nil
	})

	return err
}

// NewIntHistogramMetric creates a new integer histogram metric.
func (m *meter) NewIntHistogramMetric(name string, opts ...metric.Int64HistogramOption) (*IntHistogram, error) {
	histogram, err := m.m.Int64Histogram(name, opts...)
	if err != nil {
		return nil, err
	}

	return newIntHistogram(histogram, m.measurementOpt), nil
}

// NewFloatHistogramMetric creates a new float histogram metric.
func (m *meter) NewFloatHistogramMetric(name string, opts ...metric.Float64HistogramOption) (*FloatHistogram, error) {
	histogram, err := m.m.Float64Histogram(name, opts...)
	if err != nil {
		return nil, err
	}

	return newFloatHistogram(histogram, m.measurementOpt), nil
}
