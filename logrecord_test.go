package velwatch

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// tracedContextWithParent returns a context carrying a trace, a parent span
// and a bound log buffer.
func tracedContextWithParent(t *testing.T) (context.Context, *SpanLogs) {
	t.Helper()

	ctx := WithTraceContext(context.Background(), GenerateTraceID(), GenerateSpanID())
	ctx = WithParentSpan(ctx, GenerateSpanID())
	ctx, logs := StartSpanLogs(ctx)
	if logs == nil {
		t.Fatal("StartSpanLogs returned no buffer for a traced context")
	}
	return ctx, logs
}

func TestSpanLogsEventsCarrySpanIDs(t *testing.T) {
	captureForTest(t)
	ctx, logs := tracedContextWithParent(t)

	slog.InfoContext(ctx, "first")
	slog.WarnContext(ctx, "second")

	events := logs.Events()
	if len(events) != 2 {
		t.Fatalf("converted %d events, want 2", len(events))
	}
	for i, event := range events {
		if event.Type != EventTypeLog {
			t.Errorf("event %d Type = %q, want %q", i, event.Type, EventTypeLog)
		}
		if event.TraceID == "" || event.TraceID != logs.TraceID() {
			t.Errorf("event %d TraceID = %q, want %q", i, event.TraceID, logs.TraceID())
		}
		if event.SpanID == "" || event.SpanID != logs.SpanID() {
			t.Errorf("event %d SpanID = %q, want %q", i, event.SpanID, logs.SpanID())
		}
		if event.ParentID == nil || *event.ParentID != logs.ParentID() {
			t.Errorf("event %d ParentID = %v, want %q", i, event.ParentID, logs.ParentID())
		}
	}
	if events[0].Attributes["message"] != "first" || events[1].Attributes["message"] != "second" {
		t.Errorf("messages = %v / %v, want first / second",
			events[0].Attributes["message"], events[1].Attributes["message"])
	}
}

func TestSpanLogsEventsOmitAbsentParent(t *testing.T) {
	captureForTest(t)
	ctx, logs := tracedContext(t)

	slog.InfoContext(ctx, "root span line")

	events := logs.Events()
	if len(events) != 1 {
		t.Fatalf("converted %d events, want 1", len(events))
	}
	if events[0].ParentID != nil {
		t.Errorf("ParentID = %q, want none for a root span", *events[0].ParentID)
	}
}

func TestSpanLogsEventsEmptyBuffer(t *testing.T) {
	captureForTest(t)
	_, logs := tracedContext(t)

	if events := logs.Events(); len(events) != 0 {
		t.Errorf("converted %d events from an empty buffer, want 0", len(events))
	}
	var nilLogs *SpanLogs
	if events := nilLogs.Events(); events != nil {
		t.Errorf("nil buffer converted to %v, want nil", events)
	}
}

func TestSpanLogsEventsFlattenNestedGroups(t *testing.T) {
	captureForTest(t)
	ctx, logs := tracedContext(t)

	// Groups arrive two ways: opened on the logger with WithGroup, and
	// passed inline as a group-valued record attribute. Both flatten to
	// dotted keys, and inline groups nest.
	slog.Default().WithGroup("db").InfoContext(ctx, "query executed",
		slog.Group("query",
			slog.String("table", "users"),
			slog.Group("timing", slog.Int("ms", 12)),
		),
		slog.String("driver", "postgres"),
	)

	events := logs.Events()
	if len(events) != 1 {
		t.Fatalf("converted %d events, want 1", len(events))
	}
	attrs := events[0].Attributes

	want := map[string]any{
		"db.query.table":     "users",
		"db.query.timing.ms": int64(12),
		"db.driver":          "postgres",
		"message":            "query executed",
		"level":              "info",
		"severity_number":    severityNumberInfo,
	}
	for key, value := range want {
		got, ok := attrs[key]
		if !ok {
			t.Errorf("Attributes is missing %q (have %v)", key, attrs)
			continue
		}
		if got != value {
			t.Errorf("Attributes[%q] = %v (%T), want %v (%T)", key, got, got, value, value)
		}
	}
}

func TestSpanLogsEventsKeepRecordTimestamp(t *testing.T) {
	captureForTest(t)
	ctx, logs := tracedContext(t)

	slog.InfoContext(ctx, "emitted early")
	emitted := logs.Lines()[0].Time
	if emitted.IsZero() {
		t.Fatal("captured line has no timestamp")
	}

	// Convert well after the line was emitted: the record timestamp must
	// survive the wait rather than being replaced by the conversion time.
	time.Sleep(25 * time.Millisecond)
	converted := time.Now()

	events := logs.Events()
	if len(events) != 1 {
		t.Fatalf("converted %d events, want 1", len(events))
	}
	if !events[0].Timestamp.Equal(emitted) {
		t.Errorf("Timestamp = %v, want the record time %v", events[0].Timestamp, emitted)
	}
	if !events[0].Timestamp.Before(converted) {
		t.Errorf("Timestamp = %v, want a time before the conversion at %v",
			events[0].Timestamp, converted)
	}
}

func TestNewLogEventFallsBackToBuildTimeWithoutRecordTime(t *testing.T) {
	before := time.Now()
	event := NewLogEvent(LogLine{Level: slog.LevelInfo, Message: "no timestamp"})

	if event.Timestamp.Before(before) {
		t.Errorf("Timestamp = %v, want at or after %v", event.Timestamp, before)
	}
}

func TestNewLogEventReservedKeysWinOverLineAttrs(t *testing.T) {
	event := NewLogEvent(LogLine{
		Level:   slog.LevelWarn,
		Message: "the message",
		Attrs: map[string]any{
			"message": "shadowed",
			"level":   "shadowed",
			"kept":    "yes",
		},
	})

	if event.Attributes["message"] != "the message" {
		t.Errorf("message = %v, want the message", event.Attributes["message"])
	}
	if event.Attributes["level"] != "warn" {
		t.Errorf("level = %v, want warn", event.Attributes["level"])
	}
	if event.Attributes["kept"] != "yes" {
		t.Errorf("kept = %v, want yes", event.Attributes["kept"])
	}
}

func TestLogLevelMapping(t *testing.T) {
	cases := []struct {
		level    slog.Level
		name     string
		severity int
	}{
		{slog.LevelDebug, "debug", severityNumberDebug},
		{slog.LevelInfo, "info", severityNumberInfo},
		{slog.LevelWarn, "warn", severityNumberWarn},
		{slog.LevelError, "error", severityNumberError},
		// Custom levels land in the nearest bucket at or below them.
		{slog.LevelDebug - 4, "debug", severityNumberDebug},
		{slog.LevelInfo + 2, "info", severityNumberInfo},
		{slog.LevelWarn + 3, "warn", severityNumberWarn},
		{slog.LevelError + 4, "error", severityNumberError},
	}
	for _, tc := range cases {
		if got := logLevelName(tc.level); got != tc.name {
			t.Errorf("logLevelName(%v) = %q, want %q", tc.level, got, tc.name)
		}
		if got := logSeverityNumber(tc.level); got != tc.severity {
			t.Errorf("logSeverityNumber(%v) = %d, want %d", tc.level, got, tc.severity)
		}
	}
}

// capturingExporter records the batches it is handed. It implements
// LogRecordExporter, so the collector routes log records to it.
type capturingExporter struct {
	mu      sync.Mutex
	spans   []*Event
	records []*Event
}

func (e *capturingExporter) Export(events []*Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, events...)
	return nil
}

func (e *capturingExporter) ExportLogRecords(events []*Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.records = append(e.records, events...)
	return nil
}

func (e *capturingExporter) Close() error { return nil }

func (e *capturingExporter) counts() (spans, records int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.spans), len(e.records)
}

// spanOnlyExporter ships spans and cannot ship log records.
type spanOnlyExporter struct {
	mu    sync.Mutex
	spans []*Event
}

func (e *spanOnlyExporter) Export(events []*Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, events...)
	return nil
}

func (e *spanOnlyExporter) Close() error { return nil }

func (e *spanOnlyExporter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.spans)
}

func TestCollectorRoutesLogRecordsToLogExporter(t *testing.T) {
	exporter := &capturingExporter{}
	collector := NewCollector(exporter, 3, time.Hour)

	collector.Add(NewRequestEvent("GET", "/orders", 200, 4))
	collector.Add(NewLogEvent(LogLine{Time: time.Now(), Level: slog.LevelInfo, Message: "one"}))
	collector.Add(NewLogEvent(LogLine{Time: time.Now(), Level: slog.LevelError, Message: "two"}))

	// The batch flushes on a background goroutine once it is full.
	waitFor(t, func() bool {
		spans, records := exporter.counts()
		return spans == 1 && records == 2
	}, "exporter to receive 1 span and 2 log records")
}

func TestCollectorCountsLogRecordsAnExporterCannotShip(t *testing.T) {
	exporter := &spanOnlyExporter{}
	collector := NewCollector(exporter, 2, time.Hour)

	before := LogRecordsDropped()
	collector.Add(NewRequestEvent("GET", "/orders", 200, 4))
	collector.Add(NewLogEvent(LogLine{Time: time.Now(), Level: slog.LevelInfo, Message: "unshippable"}))

	waitFor(t, func() bool { return exporter.count() == 1 }, "exporter to receive the span")
	waitFor(t, func() bool { return LogRecordsDropped()-before == 1 }, "the log record to be counted as dropped")
}

func TestMiddlewareQueuesLogRecordsWithTheRequest(t *testing.T) {
	captureForTest(t)

	config := testConfig()
	config.LogCapture = true
	config.BatchSize = 1000
	config.FlushInterval = time.Hour
	sdk, err := initForTest(t, config)
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		slog.InfoContext(ctx, "handling request", "path", r.URL.Path)
		slog.WarnContext(ctx, "slow dependency", slog.Group("db", slog.String("host", "primary")))
		w.WriteHeader(http.StatusOK)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/orders", nil))

	var requests, logs []*Event
	for _, event := range getEvents(sdk.collector) {
		if event.Type == EventTypeLog {
			logs = append(logs, event)
			continue
		}
		requests = append(requests, event)
	}

	if len(requests) != 1 {
		t.Fatalf("queued %d request events, want 1", len(requests))
	}
	if len(logs) != 2 {
		t.Fatalf("queued %d log records, want 2", len(logs))
	}

	traceID := requests[0].TraceID
	spanID := requests[0].SpanID
	if traceID == "" || spanID == "" {
		t.Fatalf("request event carries %q/%q, want a trace and span id", traceID, spanID)
	}
	for i, event := range logs {
		if event.TraceID != traceID {
			t.Errorf("log record %d TraceID = %q, want the request's %q", i, event.TraceID, traceID)
		}
		if event.SpanID != spanID {
			t.Errorf("log record %d SpanID = %q, want the request's %q", i, event.SpanID, spanID)
		}
		if event.Tags["service"] != config.ServiceName {
			t.Errorf("log record %d service tag = %q, want %q", i, event.Tags["service"], config.ServiceName)
		}
	}
	if logs[0].Attributes["message"] != "handling request" || logs[0].Attributes["level"] != "info" {
		t.Errorf("first record = %v, want the info line", logs[0].Attributes)
	}
	if logs[1].Attributes["level"] != "warn" || logs[1].Attributes["db.host"] != "primary" {
		t.Errorf("second record = %v, want the warn line with db.host", logs[1].Attributes)
	}
}

// waitFor polls condition until it holds or the test times out.
func waitFor(t *testing.T, condition func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
