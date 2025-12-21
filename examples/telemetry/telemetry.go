package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

var (
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
)

func Init(ctx context.Context, serviceName string) {
	// Resource
	resource := newResource(serviceName)

	// Trace
	traceExporter := newTraceExporter(ctx)
	traceProvider := newTraceProvider(resource, traceExporter)
	otel.SetTracerProvider(traceProvider)

	// Trace Propagator
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Meter
	meterExporter := newMeterExporter(ctx)
	meterProvider := newMeterProvider(resource, meterExporter)
	otel.SetMeterProvider(meterProvider)

	// Runtime
	if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second)); err != nil {
		panic(err)
	}
}

func Close() {
	ctx := context.Background()

	if err := tracerProvider.Shutdown(ctx); err != nil {
		panic(err)
	}

	if err := meterProvider.Shutdown(ctx); err != nil {
		panic(err)
	}
}

func newResource(serviceName string) *resource.Resource {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("0.1.0"),
		),
	)

	if err != nil {
		panic(err)
	}

	return res
}

func newTraceExporter(ctx context.Context) *otlptrace.Exporter {
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	return exporter
}

func newTraceProvider(resource *resource.Resource, exporter sdktrace.SpanExporter) *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(0.05)),
	)
}

func newMeterExporter(ctx context.Context) *otlpmetrichttp.Exporter {
	exporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithInsecure())
	if err != nil {
		panic(err)
	}
	return exporter
}

func newMeterProvider(resource *resource.Resource, exporter sdkmetric.Exporter) *sdkmetric.MeterProvider {
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(resource),
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Second)),
		),
	)
}
