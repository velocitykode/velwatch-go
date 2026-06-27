package velwatch

import (
	"encoding/hex"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// attrMap collapses an OTLP attribute list into a key->value map for assertions.
func attrMap(kvs []*commonpb.KeyValue) map[string]*commonpb.AnyValue {
	m := make(map[string]*commonpb.AnyValue, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value
	}
	return m
}

func TestEventToSpan_RequestSemconv(t *testing.T) {
	traceID := "0123456789abcdef0123456789abcdef" // 16 bytes hex
	spanID := "0123456789abcdef"                  // 8 bytes hex
	e := NewRequestEvent("GET", "/users", 200, 12.5).WithTraceID(traceID)
	e.SpanID = spanID

	span := eventToSpan(e)

	if span.Kind != tracepb.Span_SPAN_KIND_SERVER {
		t.Errorf("Kind = %v, want SERVER", span.Kind)
	}
	if span.Name != "GET /users" {
		t.Errorf("Name = %q, want %q", span.Name, "GET /users")
	}
	if got := hex.EncodeToString(span.TraceId); got != traceID {
		t.Errorf("TraceId = %q, want %q", got, traceID)
	}
	if got := hex.EncodeToString(span.SpanId); got != spanID {
		t.Errorf("SpanId = %q, want %q", got, spanID)
	}

	m := attrMap(span.Attributes)
	if v := m["http.request.method"].GetStringValue(); v != "GET" {
		t.Errorf("http.request.method = %q, want GET", v)
	}
	if v := m["url.path"].GetStringValue(); v != "/users" {
		t.Errorf("url.path = %q, want /users", v)
	}
	if v := m["http.response.status_code"].GetIntValue(); v != 200 {
		t.Errorf("http.response.status_code = %d, want 200", v)
	}
	if _, ok := m["duration_ms"]; ok {
		t.Error("duration_ms should not be an attribute (it is the span duration)")
	}

	// duration_ms drives the span end time
	wantNanos := uint64(12_500_000)
	if span.EndTimeUnixNano-span.StartTimeUnixNano != wantNanos {
		t.Errorf("span duration = %d ns, want %d", span.EndTimeUnixNano-span.StartTimeUnixNano, wantNanos)
	}
}

func TestEventToSpan_QueryKindAndText(t *testing.T) {
	e := NewQueryEvent("SELECT 1", 3, 1)
	span := eventToSpan(e)
	if span.Kind != tracepb.Span_SPAN_KIND_CLIENT {
		t.Errorf("Kind = %v, want CLIENT", span.Kind)
	}
	m := attrMap(span.Attributes)
	if v := m["db.query.text"].GetStringValue(); v != "SELECT 1" {
		t.Errorf("db.query.text = %q, want %q", v, "SELECT 1")
	}
	if v := m["db.response.returned_rows"].GetIntValue(); v != 1 {
		t.Errorf("db.response.returned_rows = %d, want 1", v)
	}
}

func TestEventToSpan_ExceptionSpanEvent(t *testing.T) {
	e := NewExceptionEvent("RuntimeError", "boom", "goroutine 1 [running]:\nmain.main()")
	span := eventToSpan(e)

	if span.Status == nil || span.Status.Code != tracepb.Status_STATUS_CODE_ERROR {
		t.Fatalf("Status = %v, want ERROR", span.Status)
	}
	if len(span.Events) != 1 {
		t.Fatalf("got %d span events, want 1", len(span.Events))
	}
	ev := span.Events[0]
	if ev.Name != "exception" {
		t.Errorf("event name = %q, want exception", ev.Name)
	}
	m := attrMap(ev.Attributes)
	if v := m["exception.type"].GetStringValue(); v != "RuntimeError" {
		t.Errorf("exception.type = %q, want RuntimeError", v)
	}
	if v := m["exception.message"].GetStringValue(); v != "boom" {
		t.Errorf("exception.message = %q, want boom", v)
	}
	if m["exception.stacktrace"].GetStringValue() == "" {
		t.Error("exception.stacktrace should be set")
	}
	// raw exception fields must not leak onto the span attributes
	if _, ok := attrMap(span.Attributes)["type"]; ok {
		t.Error("raw 'type' attribute should not appear on the span")
	}
}

func TestEventToSpan_TagsBecomeAttributes(t *testing.T) {
	e := NewJobEvent("*queue.EmailJob", "emails", "processed", 10).WithTag("service", "api")
	span := eventToSpan(e)
	if span.Kind != tracepb.Span_SPAN_KIND_CONSUMER {
		t.Errorf("Kind = %v, want CONSUMER", span.Kind)
	}
	m := attrMap(span.Attributes)
	if v := m["service"].GetStringValue(); v != "api" {
		t.Errorf("service tag = %q, want api", v)
	}
	if v := m["messaging.destination.name"].GetStringValue(); v != "emails" {
		t.Errorf("messaging.destination.name = %q, want emails", v)
	}
}

func TestDecodeID_InvalidFallsBackToRandom(t *testing.T) {
	// wrong length / non-hex must still yield a valid-width ID, not panic
	if got := len(decodeID("not-hex", 16)); got != 16 {
		t.Errorf("decodeID len = %d, want 16", got)
	}
	if got := len(decodeID("", 8)); got != 8 {
		t.Errorf("decodeID len = %d, want 8", got)
	}
}

func TestEventToSpan_ParentID(t *testing.T) {
	e := NewRequestEvent("GET", "/", 200, 1)
	e.TraceID = "0123456789abcdef0123456789abcdef"
	e.SpanID = "0123456789abcdef"
	e.WithParent("fedcba9876543210")

	span := eventToSpan(e)
	if got := hex.EncodeToString(span.ParentSpanId); got != "fedcba9876543210" {
		t.Errorf("ParentSpanId = %q, want fedcba9876543210", got)
	}
}
