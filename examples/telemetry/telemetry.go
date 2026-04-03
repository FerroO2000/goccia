// Package telemetry provides OpenTelemetry initialization for the examples.
package telemetry

import (
	"context"
	"log"
	"net"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const otelCollectorEndpoint = "localhost:4317"

var (
	globalTracerProvider *sdktrace.TracerProvider
	globalMeterProvider  *sdkmetric.MeterProvider
	globalLoggerProvider *sdklog.LoggerProvider

	traceRatio     = 0.05
	logMinSeverity = otellog.SeverityInfo
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
	// Resource
	resource := newResource(serviceName)

	// Check if collector is healthy using gRPC health check
	if !isCollectorReachable(otelCollectorEndpoint) {
		log.Print("WARNING: OpenTelemetry collector is not healthy or not reachable")

		// Use the console exporter for logging
		loggerProvider := newLoggerProvider(resource, newConsoleExporter(), nil)
		globalLoggerProvider = loggerProvider
		global.SetLoggerProvider(loggerProvider)

		return
	}

	// Create gRPC connection
	grpcTransport := grpc.WithTransportCredentials(insecure.NewCredentials())
	grcpConn, err := grpc.NewClient(otelCollectorEndpoint, grpcTransport)
	if err != nil {
		panic(err)
	}

	// Tracer
	traceExporter := newTraceExporter(ctx, grcpConn)
	traceProvider := newTraceProvider(resource, traceExporter)
	globalTracerProvider = traceProvider
	otel.SetTracerProvider(traceProvider)

	// Trace Propagator
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Meter
	meterExporter := newMeterExporter(ctx, grcpConn)
	meterProvider := newMeterProvider(resource, meterExporter)
	globalMeterProvider = meterProvider
	otel.SetMeterProvider(meterProvider)

	// Logger
	loggerProvider := newLoggerProvider(resource, newConsoleExporter(), newLoggerExporter(ctx, grcpConn))
	globalLoggerProvider = loggerProvider
	global.SetLoggerProvider(loggerProvider)

	// Runtime
	if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second)); err != nil {
		panic(err)
	}
}

// Close shut downs OpenTelemetry providers.
func Close() {
	ctx := context.Background()

	if globalTracerProvider != nil {
		if err := globalTracerProvider.Shutdown(ctx); err != nil {
			panic(err)
		}
	}

	if globalMeterProvider != nil {
		if err := globalMeterProvider.Shutdown(ctx); err != nil {
			panic(err)
		}
	}

	if globalLoggerProvider != nil {
		if err := globalLoggerProvider.Shutdown(ctx); err != nil {
			panic(err)
		}
	}
}

// SetTraceRatio sets the sampling ratio for traces.
func SetTraceRatio(ratio float64) {
	traceRatio = ratio
}

// SetLogMinSeverity sets the severity for logs.
func SetLogMinSeverity(severity otellog.Severity) {
	logMinSeverity = severity
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

func newLoggerExporter(ctx context.Context, conn *grpc.ClientConn) *otlploggrpc.Exporter {
	exporter, err := otlploggrpc.New(ctx, otlploggrpc.WithGRPCConn(conn))
	if err != nil {
		panic(err)
	}
	return exporter
}

func newLoggerProvider(resource *resource.Resource, consoleExporter *consoleExporter, exporter sdklog.Exporter) *sdklog.LoggerProvider {
	consoleProcessor := newSeverityProcessor(logMinSeverity, sdklog.NewSimpleProcessor(consoleExporter))

	providerOpts := []sdklog.LoggerProviderOption{
		sdklog.WithResource(resource),
		sdklog.WithProcessor(consoleProcessor),
	}

	if exporter != nil {
		processor := newSeverityProcessor(logMinSeverity, sdklog.NewBatchProcessor(exporter))
		providerOpts = append(providerOpts, sdklog.WithProcessor(processor))
	}

	return sdklog.NewLoggerProvider(providerOpts...)
}
