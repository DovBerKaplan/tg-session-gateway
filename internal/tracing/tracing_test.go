package tracing

import (
	"context"
	"testing"
)

func TestSetupNoopWithoutEndpoint(t *testing.T) {
	shutdown, err := Setup(context.Background(), "", "botapi")
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("noop shutdown must be nil-error: %v", err)
	}
	// the global provider is the no-op one: spans are non-recording
	_, span := Tracer("botapi").Start(context.Background(), "test")
	if span.SpanContext().IsValid() {
		t.Fatal("no-op spans must not carry a valid span context")
	}
	span.End()
}
