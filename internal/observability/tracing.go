package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// SetupTracing configures OpenTelemetry. With an empty endpoint tracing stays
// a no-op, so tests and local runs need no collector. The returned function
// flushes pending spans on shutdown.
func SetupTracing(ctx context.Context, service, endpoint string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	// The exporter reads the standard OTEL_EXPORTER_OTLP_* variables itself,
	// which appends /v1/traces to the base endpoint and handles http vs https.
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	// Schemaless avoids a schema-URL clash with resource.Default(), whose
	// semconv version moves with the SDK.
	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", service)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// InjectTraceParent serialises the current span context (W3C traceparent) so
// it can be stored with a notification and resumed by the worker later.
func InjectTraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// ExtractTraceParent restores a span context stored by InjectTraceParent.
func ExtractTraceParent(ctx context.Context, traceParent string) context.Context {
	if traceParent == "" {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": traceParent})
}
