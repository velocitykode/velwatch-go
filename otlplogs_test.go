package velwatch

import (
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

// fakeCollector is an in-process OTLP receiver speaking both signals. It
// stands in for the platform so a test can assert what actually crosses the
// wire, including that spans and their log records arrive together.
type fakeCollector struct {
	coltracepb.UnimplementedTraceServiceServer
	collogspb.UnimplementedLogsServiceServer

	mu        sync.Mutex
	traceReqs []*coltracepb.ExportTraceServiceRequest
	logReqs   []*collogspb.ExportLogsServiceRequest
	authority []string
}

func (f *fakeCollector) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.traceReqs = append(f.traceReqs, req)
	f.authority = append(f.authority, bearerFrom(ctx))
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (f *fakeCollector) ExportLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logReqs = append(f.logReqs, req)
	f.authority = append(f.authority, bearerFrom(ctx))
	return &collogspb.ExportLogsServiceResponse{}, nil
}

// logsServiceAdapter exposes the collector's ExportLogs under the Export name
// the generated LogsServiceServer interface requires, so one struct can serve
// both services without two methods called Export.
type logsServiceAdapter struct {
	collogspb.UnimplementedLogsServiceServer
	inner *fakeCollector
}

func (a logsServiceAdapter) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	return a.inner.ExportLogs(ctx, req)
}

func bearerFrom(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (f *fakeCollector) counts() (traces, logs int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.traceReqs), len(f.logReqs)
}

func (f *fakeCollector) spans() []*coltracepb.ExportTraceServiceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*coltracepb.ExportTraceServiceRequest, len(f.traceReqs))
	copy(out, f.traceReqs)
	return out
}

func (f *fakeCollector) logs() []*collogspb.ExportLogsServiceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*collogspb.ExportLogsServiceRequest, len(f.logReqs))
	copy(out, f.logReqs)
	return out
}

func (f *fakeCollector) tokens() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.authority))
	copy(out, f.authority)
	return out
}

// startFakeCollector serves both OTLP services on a local port and returns the
// receiver and its address.
func startFakeCollector(t *testing.T) (*fakeCollector, string) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	fake := &fakeCollector{}
	server := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(server, fake)
	collogspb.RegisterLogsServiceServer(server, logsServiceAdapter{inner: fake})

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	return fake, listener.Addr().String()
}

// logEventFor builds a log event bound to the given span, the shape
// SpanLogs.Events produces.
func logEventFor(traceID, spanID, message string, level slog.Level, attrs map[string]any) *Event {
	event := NewLogEvent(LogLine{
		Time:    time.Now(),
		Level:   level,
		Message: message,
		Attrs:   attrs,
	})
	event.TraceID = traceID
	event.SpanID = spanID
	return event
}

func TestOTLPExporterSendsSpansAndLogsInOneFlush(t *testing.T) {
	fake, addr := startFakeCollector(t)

	exporter, err := NewOTLPExporter(addr, "tok-logs", "api", true)
	if err != nil {
		t.Fatalf("NewOTLPExporter: %v", err)
	}
	t.Cleanup(func() { _ = exporter.Close() })
	exporter.release, exporter.commitSHA = "1.4.2", "abc123"

	traceID := "0123456789abcdef0123456789abcdef"
	spanID := "0123456789abcdef"

	// A batch big enough that nothing flushes until Flush is called, so the
	// spans and the log records below leave in one tick.
	collector := NewCollector(exporter, 100, time.Hour)
	request := NewRequestEvent("GET", "/orders", 200, 4)
	request.TraceID, request.SpanID = traceID, spanID
	collector.Add(request)
	collector.Add(logEventFor(traceID, spanID, "handling request", slog.LevelInfo, map[string]any{"path": "/orders"}))
	collector.Add(logEventFor(traceID, spanID, "slow dependency", slog.LevelWarn, nil))

	collector.Flush()

	waitFor(t, func() bool {
		traces, logs := fake.counts()
		return traces == 1 && logs == 1
	}, "one trace request and one logs request from a single flush")

	// Both signals authenticate the same way.
	for _, token := range fake.tokens() {
		if token != "Bearer tok-logs" {
			t.Errorf("authorization = %q, want %q", token, "Bearer tok-logs")
		}
	}

	spanResource := attrMap(fake.spans()[0].ResourceSpans[0].Resource.Attributes)
	logResource := attrMap(fake.logs()[0].ResourceLogs[0].Resource.Attributes)
	for _, key := range []string{"service.name", "service.version", "telemetry.sdk.name", "vcs.ref.head.revision"} {
		want := spanResource[key].GetStringValue()
		if want == "" {
			t.Fatalf("span resource is missing %s", key)
		}
		if got := logResource[key].GetStringValue(); got != want {
			t.Errorf("log resource %s = %q, want %q (the span's value)", key, got, want)
		}
	}

	records := fake.logs()[0].ResourceLogs[0].ScopeLogs[0].LogRecords
	if len(records) != 2 {
		t.Fatalf("got %d log records, want 2", len(records))
	}
	if got := records[0].Body.GetStringValue(); got != "handling request" {
		t.Errorf("body = %q, want %q", got, "handling request")
	}
	if got := records[0].SeverityText; got != "info" {
		t.Errorf("severity text = %q, want info", got)
	}
	if got := records[0].SeverityNumber; got != logspb.SeverityNumber(severityNumberInfo) {
		t.Errorf("severity number = %v, want %d", got, severityNumberInfo)
	}
	if got := hex.EncodeToString(records[0].TraceId); got != traceID {
		t.Errorf("log trace id = %q, want %q (the span's)", got, traceID)
	}
	if got := hex.EncodeToString(records[0].SpanId); got != spanID {
		t.Errorf("log span id = %q, want %q (the span's)", got, spanID)
	}
	if records[0].TimeUnixNano == 0 || records[0].ObservedTimeUnixNano != records[0].TimeUnixNano {
		t.Errorf("times = %d/%d, want a non-zero pair", records[0].TimeUnixNano, records[0].ObservedTimeUnixNano)
	}

	attrs := attrMap(records[0].Attributes)
	if got := attrs["path"].GetStringValue(); got != "/orders" {
		t.Errorf("path attribute = %q, want /orders", got)
	}
	for _, reserved := range []string{logAttrMessage, logAttrLevel, logAttrSeverityNumber} {
		if _, ok := attrs[reserved]; ok {
			t.Errorf("%q has an OTLP field of its own and must not be repeated as an attribute", reserved)
		}
	}
	if records[1].SeverityText != "warn" {
		t.Errorf("second record severity = %q, want warn", records[1].SeverityText)
	}
}

func TestOTLPExporterSplitsOversizeLogBatch(t *testing.T) {
	fake, addr := startFakeCollector(t)

	exporter, err := NewOTLPExporter(addr, "tok", "api", true)
	if err != nil {
		t.Fatalf("NewOTLPExporter: %v", err)
	}
	t.Cleanup(func() { _ = exporter.Close() })

	events := make([]*Event, maxRecordsPerExport+1)
	for i := range events {
		events[i] = logEventFor("", "", "line", slog.LevelInfo, nil)
	}
	if err := exporter.ExportLogRecords(events); err != nil {
		t.Fatalf("ExportLogRecords: %v", err)
	}

	requests := fake.logs()
	if len(requests) != 2 {
		t.Fatalf("got %d logs requests, want 2 (the batch exceeds the per-export cap)", len(requests))
	}
	if got := len(requests[0].ResourceLogs[0].ScopeLogs[0].LogRecords); got != maxRecordsPerExport {
		t.Errorf("first request carried %d records, want %d", got, maxRecordsPerExport)
	}
	if got := len(requests[1].ResourceLogs[0].ScopeLogs[0].LogRecords); got != 1 {
		t.Errorf("second request carried %d records, want 1", got)
	}
}

func TestOTLPHTTPExporterSendsSpansAndLogsInOneFlush(t *testing.T) {
	var mu sync.Mutex
	var traceReq coltracepb.ExportTraceServiceRequest
	var logReq collogspb.ExportLogsServiceRequest
	var tracePosts, logPosts int
	headers := map[string]http.Header{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		headers[r.URL.Path] = r.Header.Clone()
		switch r.URL.Path {
		case otlpTracesPath:
			tracePosts++
			_ = proto.Unmarshal(body, &traceReq)
		case otlpLogsPath:
			logPosts++
			_ = proto.Unmarshal(body, &logReq)
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exporter, err := NewOTLPHTTPExporter(srv.URL, "tok-http", "api", true)
	if err != nil {
		t.Fatalf("NewOTLPHTTPExporter: %v", err)
	}
	exporter.release = "2.0.0"

	traceID := "abcdefabcdefabcdefabcdefabcdefab"
	spanID := "abcdefabcdefabcd"

	collector := NewCollector(exporter, 100, time.Hour)
	request := NewRequestEvent("GET", "/health", 200, 1)
	request.TraceID, request.SpanID = traceID, spanID
	collector.Add(request)
	collector.Add(logEventFor(traceID, spanID, "checked", slog.LevelError, nil))
	collector.Flush()

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return tracePosts == 1 && logPosts == 1
	}, "one POST to each OTLP route from a single flush")

	mu.Lock()
	defer mu.Unlock()

	for path, header := range headers {
		if got := header.Get("Content-Type"); got != "application/x-protobuf" {
			t.Errorf("%s Content-Type = %q, want application/x-protobuf", path, got)
		}
		if got := header.Get("Authorization"); got != "Bearer tok-http" {
			t.Errorf("%s Authorization = %q, want Bearer tok-http", path, got)
		}
	}

	spanResource := attrMap(traceReq.ResourceSpans[0].Resource.Attributes)
	logResource := attrMap(logReq.ResourceLogs[0].Resource.Attributes)
	for _, key := range []string{"service.name", "service.version"} {
		if got, want := logResource[key].GetStringValue(), spanResource[key].GetStringValue(); got != want || want == "" {
			t.Errorf("log resource %s = %q, want %q (the span's value)", key, got, want)
		}
	}

	records := logReq.ResourceLogs[0].ScopeLogs[0].LogRecords
	if len(records) != 1 {
		t.Fatalf("got %d log records, want 1", len(records))
	}
	if records[0].SeverityText != "error" {
		t.Errorf("severity text = %q, want error", records[0].SeverityText)
	}
	if got := hex.EncodeToString(records[0].SpanId); got != spanID {
		t.Errorf("log span id = %q, want %q (the span's)", got, spanID)
	}
}

func TestNormalizeLogsURL(t *testing.T) {
	cases := []struct {
		endpoint string
		insecure bool
		want     string
	}{
		{"ingest.velwatch.com", false, "https://ingest.velwatch.com/v1/logs"},
		{"localhost:4318", true, "http://localhost:4318/v1/logs"},
		{"https://ingest.velwatch.com/", false, "https://ingest.velwatch.com/v1/logs"},
		// An endpoint configured as a full traces URL still resolves the
		// logs route beside it.
		{"https://ingest.velwatch.com/v1/traces", false, "https://ingest.velwatch.com/v1/logs"},
		{"https://ingest.velwatch.com/v1/logs", false, "https://ingest.velwatch.com/v1/logs"},
	}
	for _, c := range cases {
		if got := normalizeLogsURL(c.endpoint, c.insecure); got != c.want {
			t.Errorf("normalizeLogsURL(%q, %v) = %q, want %q", c.endpoint, c.insecure, got, c.want)
		}
	}
}

func TestBothOTLPExportersShipLogRecords(t *testing.T) {
	grpcExporter, err := NewOTLPExporter("localhost:4317", "tok", "api", true)
	if err != nil {
		t.Fatalf("NewOTLPExporter: %v", err)
	}
	t.Cleanup(func() { _ = grpcExporter.Close() })
	httpExporter, err := NewOTLPHTTPExporter("localhost:4318", "tok", "api", true)
	if err != nil {
		t.Fatalf("NewOTLPHTTPExporter: %v", err)
	}

	for _, exporter := range []Exporter{grpcExporter, httpExporter} {
		if _, ok := exporter.(LogRecordExporter); !ok {
			t.Errorf("%T does not implement LogRecordExporter, so its log records would be dropped", exporter)
		}
	}
}

func TestEventToLogRecordZeroTimestamp(t *testing.T) {
	event := NewLogEvent(LogLine{Level: slog.LevelInfo, Message: "no time"})
	event.Timestamp = time.Time{}

	record := eventToLogRecord(event)
	if record.TimeUnixNano != 0 || record.ObservedTimeUnixNano != 0 {
		t.Errorf("times = %d/%d, want 0 for a zero timestamp", record.TimeUnixNano, record.ObservedTimeUnixNano)
	}
}

func TestLogRecordCarriesEventTags(t *testing.T) {
	event := logEventFor("", "", "tagged", slog.LevelInfo, nil)
	event.setDefaultTag("service", "api")
	event.setDefaultTag(tagRelease, "1.4.2")

	attrs := attrMap(eventToLogRecord(event).Attributes)
	if got := attrs["velwatch.tag.service"].GetStringValue(); got != "api" {
		t.Errorf("velwatch.tag.service = %q, want api", got)
	}
	if got := attrs["velwatch.tag.release"].GetStringValue(); got != "1.4.2" {
		t.Errorf("velwatch.tag.release = %q, want 1.4.2", got)
	}
}

func TestChunkEvents(t *testing.T) {
	events := make([]*Event, 5)
	for i := range events {
		events[i] = NewEvent(EventTypeLog)
	}

	if got := chunkEvents(events, 5); len(got) != 1 || len(got[0]) != 5 {
		t.Errorf("a batch at the cap should stay one chunk, got %d chunks", len(got))
	}
	chunks := chunkEvents(events, 2)
	if len(chunks) != 3 {
		t.Fatalf("got %d chunks, want 3", len(chunks))
	}
	if len(chunks[0]) != 2 || len(chunks[1]) != 2 || len(chunks[2]) != 1 {
		t.Errorf("chunk sizes = %d/%d/%d, want 2/2/1", len(chunks[0]), len(chunks[1]), len(chunks[2]))
	}
}

func TestAnyValueIntegerKinds(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  int64
	}{
		{"int", 7, 7},
		{"int32", int32(7), 7},
		{"int64", int64(7), 7},
		{"uint", uint(7), 7},
		{"uint32", uint32(7), 7},
		{"uint64", uint64(7), 7},
	}
	for _, c := range cases {
		got := anyValue(c.value)
		if _, ok := got.Value.(*commonpb.AnyValue_IntValue); !ok {
			t.Errorf("anyValue(%s) = %T, want an int value", c.name, got.Value)
			continue
		}
		if got.GetIntValue() != c.want {
			t.Errorf("anyValue(%s) = %d, want %d", c.name, got.GetIntValue(), c.want)
		}
	}

	// Too large for an int64: string is the only lossless form OTLP has.
	huge := anyValue(uint64(1) << 63)
	if huge.GetStringValue() != "9223372036854775808" {
		t.Errorf("oversize uint64 = %q, want its decimal string", huge.GetStringValue())
	}
}
