package velwatch

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/protobuf/proto"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

func TestNormalizeTracesURL(t *testing.T) {
	cases := []struct {
		endpoint string
		insecure bool
		want     string
	}{
		{"ingest.velwatch.com", false, "https://ingest.velwatch.com/v1/traces"},
		{"localhost:4318", true, "http://localhost:4318/v1/traces"},
		{"https://ingest.velwatch.com/", false, "https://ingest.velwatch.com/v1/traces"},
		{"https://ingest.velwatch.com/v1/traces", false, "https://ingest.velwatch.com/v1/traces"},
	}
	for _, c := range cases {
		if got := normalizeTracesURL(c.endpoint, c.insecure); got != c.want {
			t.Errorf("normalizeTracesURL(%q, %v) = %q, want %q", c.endpoint, c.insecure, got, c.want)
		}
	}
}

func TestOTLPHTTPExporter_PostsProtobuf(t *testing.T) {
	var gotAuth, gotType string
	var gotReq coltracepb.ExportTraceServiceRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		_ = proto.Unmarshal(body, &gotReq)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp, err := NewOTLPHTTPExporter(srv.URL, "tok-123", "api", true)
	if err != nil {
		t.Fatalf("NewOTLPHTTPExporter: %v", err)
	}

	e := NewRequestEvent("GET", "/health", 200, 4).WithTag("team", "billing")
	e.TraceID = "0123456789abcdef0123456789abcdef"
	e.SpanID = "0123456789abcdef"
	if err := exp.Export([]*Event{e}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if gotAuth != "Bearer tok-123" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer tok-123")
	}
	if gotType != "application/x-protobuf" {
		t.Errorf("Content-Type = %q, want application/x-protobuf", gotType)
	}
	if len(gotReq.ResourceSpans) != 1 {
		t.Fatalf("got %d resource spans, want 1", len(gotReq.ResourceSpans))
	}
	spans := gotReq.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 1 || spans[0].Name != "GET /health" {
		t.Fatalf("unexpected spans: %+v", spans)
	}
	// The HTTP exporter shares the span builder, so tags cross the wire
	// prefixed and with no flat copy.
	m := attrMap(spans[0].Attributes)
	if v := m["velwatch.tag.team"].GetStringValue(); v != "billing" {
		t.Errorf("velwatch.tag.team = %q, want billing", v)
	}
	if _, ok := m["team"]; ok {
		t.Error("flat 'team' attribute should not cross the HTTP wire")
	}
}

func TestOTLPHTTPExporter_ErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	exp, _ := NewOTLPHTTPExporter(srv.URL, "bad", "api", true)
	if err := exp.Export([]*Event{NewRequestEvent("GET", "/", 200, 1)}); err == nil {
		t.Error("expected error on non-2xx status, got nil")
	}
}
