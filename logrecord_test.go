package velwatch

import (
	"log/slog"
	"sync"
	"testing"
	"time"
)

// TestNewLogEventIsTraceless pins the traceless model: a log event carries no
// trace, span or parent id, so a line is stored and searched on its own.
func TestNewLogEventIsTraceless(t *testing.T) {
	event := NewLogEvent(LogLine{Time: time.Now(), Level: slog.LevelInfo, Message: "standalone"})

	if event.TraceID != "" || event.SpanID != "" {
		t.Errorf("trace/span = %q/%q, want both empty", event.TraceID, event.SpanID)
	}
	if event.ParentID != nil {
		t.Errorf("ParentID = %v, want nil", *event.ParentID)
	}
	if event.Type != EventTypeLog {
		t.Errorf("Type = %q, want %q", event.Type, EventTypeLog)
	}
}

// TestNewLogEventKeepsLineTimestamp keeps a line at the moment it was
// written, however long it waits in a batch.
func TestNewLogEventKeepsLineTimestamp(t *testing.T) {
	written := time.Now().Add(-30 * time.Second)
	event := NewLogEvent(LogLine{Time: written, Level: slog.LevelInfo, Message: "earlier"})

	if !event.Timestamp.Equal(written) {
		t.Errorf("Timestamp = %v, want %v", event.Timestamp, written)
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
