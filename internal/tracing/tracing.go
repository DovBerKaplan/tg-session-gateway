// Package tracing wires OpenTelemetry distributed tracing into both
// planes. Opt-in via GW_OTEL_ENDPOINT (OTLP/HTTP): empty means a
// no-op tracer provider — zero spans, zero exporter, effectively
// zero overhead for deployments that do not trace.
package tracing

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/DovBerKaplan/tg-session-gateway/internal/version"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// ShutdownFunc flushes and releases the provider (called on graceful
// shutdown). For the no-op setup it does nothing.
type ShutdownFunc func(context.Context) error

// Setup configures the global tracer provider. endpoint is the OTLP
// HTTP endpoint (e.g. http://otel-collector:4318); empty disables
// tracing entirely.
func Setup(ctx context.Context, endpoint, plane string) (ShutdownFunc, error) {
	if endpoint == "" {
		// explicit no-op: spans are non-recording, no exporter runs
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithInsecure()) // local collectors; TLS via env if needed
	if err != nil {
		return nil, fmt.Errorf("otel exporter: %w", err)
	}
	res, err := sdkresource.New(ctx,
		sdkresource.WithAttributes(
			semconv.ServiceName("tggw-"+plane),
			semconv.ServiceVersion(version.Version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	slog.Info("otel tracing enabled", "plane", plane, "endpoint", endpoint)
	return provider.Shutdown, nil
}

// Tracer returns the named tracer for a component.
func Tracer(component string) trace.Tracer {
	return otel.Tracer("github.com/DovBerKaplan/tg-session-gateway/" + component)
}
