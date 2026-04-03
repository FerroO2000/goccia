package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type tracer struct {
	t          trace.Tracer
	propagator propagation.TextMapPropagator

	spanStartOpt trace.SpanStartOption
}

func newTracer(attributes []attribute.KeyValue) *tracer {
	tp := otel.GetTracerProvider()

	t := tp.Tracer(
		libName,
		trace.WithInstrumentationVersion(libVersion),
		trace.WithInstrumentationAttributes(attributes...),
	)

	return &tracer{
		t:          t,
		propagator: otel.GetTextMapPropagator(),

		spanStartOpt: trace.WithAttributes(attributes...),
	}
}

// StartTrace starts a trace with the provided name and options.
func (t *tracer) StartTrace(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if len(opts) == 0 {
		return t.t.Start(ctx, name, t.spanStartOpt)
	}

	allOpts := make([]trace.SpanStartOption, 0, len(opts)+1)
	allOpts = append(allOpts, t.spanStartOpt)
	allOpts = append(allOpts, opts...)

	return t.t.Start(ctx, name, allOpts...)
}

// InjectTrace injects the trace context into the provided carrier.
// It shall be used for propagating traces between services.
func (t *tracer) InjectTrace(ctx context.Context, carrier propagation.TextMapCarrier) {
	t.propagator.Inject(ctx, carrier)
}

// ExtractTraceContext extracts the trace context from the provided carrier.
// It shall be used for propagating traces between services.
func (t *tracer) ExtractTraceContext(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return t.propagator.Extract(ctx, carrier)
}
