// Package telemetry provides OpenTelemetry initialization for the examples.
package telemetry

import (
	"context"
	"log"
	"net"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const otelCollectorEndpoint = "localhost:4317"

var (
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider

	traceRatio = 0.05
)

// isCollectorReachable checks if the OTLP collector port is reachable
func isCollectorReachable(endpoint string) bool {
	conn, err := net.DialTimeout("tcp", endpoint, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// Init initializes OpenTelemetry.
// It prints out a warning if the connection to the OpenTelemetry collector fails.
func Init(ctx context.Context, serviceName string) {
	// Check if collector is healthy using gRPC health check
	if !isCollectorReachable(otelCollectorEndpoint) {
		log.Print("WARNING: OpenTelemetry collector is not healthy or not reachable")
		return
	}

	// Create gRPC connection
	grpcTransport := grpc.WithTransportCredentials(insecure.NewCredentials())
	grcpConn, err := grpc.NewClient(otelCollectorEndpoint, grpcTransport)
	if err != nil {
		panic(err)
	}

	// Resource
	resource := newResource(serviceName)

	// Trace
	traceExporter := newTraceExporter(ctx, grcpConn)
	traceProvider := newTraceProvider(resource, traceExporter)
	otel.SetTracerProvider(traceProvider)

	// Trace Propagator
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Meter
	meterExporter := newMeterExporter(ctx, grcpConn)
	meterProvider := newMeterProvider(resource, meterExporter)
	otel.SetMeterProvider(meterProvider)

	// Runtime
	if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second)); err != nil {
		panic(err)
	}
}

// Close shut downs OpenTelemetry providers.
func Close() {
	ctx := context.Background()

	if err := tracerProvider.Shutdown(ctx); err != nil {
		panic(err)
	}

	if err := meterProvider.Shutdown(ctx); err != nil {
		panic(err)
	}
}

// SetTraceRatio sets the sampling ratio for traces.
func SetTraceRatio(ratio float64) {
	traceRatio = ratio
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

func newTraceExporter(ctx context.Context, conn *grpc.ClientConn) *otlptrace.Exporter {
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		panic(err)
	}
	return exporter
}

func newTraceProvider(resource *resource.Resource, exporter sdktrace.SpanExporter) *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(traceRatio)),
	)
}

func newMeterExporter(ctx context.Context, conn *grpc.ClientConn) *otlpmetricgrpc.Exporter {
	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
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
