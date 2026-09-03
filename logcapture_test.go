package velwatch

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

// recordingHandler is a slog.Handler that keeps every record it is given, so
// a test can assert the handler capture wrapped still receives records.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
	level   slog.Level
	attrs   []slog.Attr
	groups  []string
}

func (h *recordingHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.attrs = append(h.attrs, attrs...)
	return h
}

func (h *recordingHandler) WithGroup(name string) slog.Handler {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.groups = append(h.groups, name)
	return h
}

func (h *recordingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

func (h *recordingHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.records))
	for i, r := range h.records {
		out[i] = r.Message
	}
	return out
}

// captureForTest installs log capture in front of a recording handler and
// restores the previous slog default when the test ends.
func captureForTest(t *testing.T) *recordingHandler {
	t.Helper()

	previous := slog.Default()
	sink := &recordingHandler{level: slog.LevelInfo}
	slog.SetDefault(slog.New(sink))

	installLogCapture()
	t.Cleanup(func() {
		uninstallLogCapture()
		slog.SetDefault(previous)
	})
	return sink
}

// tracedContext returns a context carrying a trace and a bound log buffer.
func tracedContext(t *testing.T) (context.Context, *SpanLogs) {
	t.Helper()

	ctx := WithTraceContext(context.Background(), GenerateTraceID(), GenerateSpanID())
	ctx, logs := StartSpanLogs(ctx)
	if logs == nil {
		t.Fatal("StartSpanLogs returned no buffer for a traced context")
	}
	return ctx, logs
}

func TestLogCaptureBuffersLineInsideSpan(t *testing.T) {
	captureForTest(t)
	ctx, logs := tracedContext(t)

	slog.InfoContext(ctx, "user created", "user_id", 42)

	lines := logs.Lines()
	if len(lines) != 1 {
		t.Fatalf("buffered %d lines, want 1", len(lines))
	}
	line := lines[0]
	if line.Message != "user created" {
		t.Errorf("Message = %q, want %q", line.Message, "user created")
	}
	if line.Level != slog.LevelInfo {
		t.Errorf("Level = %v, want %v", line.Level, slog.LevelInfo)
	}
	if line.Time.IsZero() {
		t.Error("Time is zero, want the slog record timestamp")
	}
	if got := line.Attrs["user_id"]; got != int64(42) {
		t.Errorf("Attrs[user_id] = %v (%T), want 42", got, got)
	}
	if logs.TraceID() != GetTraceID(ctx) || logs.SpanID() != GetSpanID(ctx) {
		t.Errorf("buffer bound to %s/%s, want %s/%s",
			logs.TraceID(), logs.SpanID(), GetTraceID(ctx), GetSpanID(ctx))
	}
}

func TestLogCaptureDropsLineOutsideSpan(t *testing.T) {
	captureForTest(t)

	before := LogsDroppedOutsideSpan()
	slog.Info("no span here")
	slog.InfoContext(context.Background(), "still no span")

	if got := LogsDroppedOutsideSpan() - before; got != 2 {
		t.Errorf("dropped %d lines outside a span, want 2", got)
	}
}

func TestLogCaptureDroppedLineStillReachesPreviousHandler(t *testing.T) {
	sink := captureForTest(t)

	slog.Info("outside the span")

	if got := sink.count(); got != 1 {
		t.Fatalf("previous handler saw %d records, want 1", got)
	}
}

func TestLogCaptureWithAttrsAndGroups(t *testing.T) {
	captureForTest(t)
	ctx, logs := tracedContext(t)

	logger := slog.Default().
		With("service", "api").
		WithGroup("db").
		WithGroup("query").
		With("table", "users")
	logger.InfoContext(ctx, "query executed", "rows", 3, slog.Group("timing", "ms", 12))

	lines := logs.Lines()
	if len(lines) != 1 {
		t.Fatalf("buffered %d lines, want 1", len(lines))
	}
	attrs := lines[0].Attrs

	want := map[string]any{
		"service":            "api",
		"db.query.table":     "users",
		"db.query.rows":      int64(3),
		"db.query.timing.ms": int64(12),
	}
	for key, value := range want {
		got, ok := attrs[key]
		if !ok {
			t.Errorf("Attrs is missing %q (have %v)", key, attrs)
			continue
		}
		if got != value {
			t.Errorf("Attrs[%q] = %v (%T), want %v", key, got, got, value)
		}
	}
}

func TestLogCaptureEmptyAttrsAndGroupsAreSkipped(t *testing.T) {
	captureForTest(t)
	ctx, logs := tracedContext(t)

	// An empty group name leaves the prefix alone; an empty group value and
	// an empty attribute are dropped, as slog specifies.
	slog.Default().WithGroup("").InfoContext(ctx, "edge cases",
		slog.Group("empty"), slog.Attr{}, slog.String("kept", "yes"))

	lines := logs.Lines()
	if len(lines) != 1 {
		t.Fatalf("buffered %d lines, want 1", len(lines))
	}
	attrs := lines[0].Attrs
	if len(attrs) != 1 || attrs["kept"] != "yes" {
		t.Errorf("Attrs = %v, want only kept=yes", attrs)
	}
}

func TestLogCaptureDerivedHandlersDoNotShareAttrs(t *testing.T) {
	captureForTest(t)
	ctx, logs := tracedContext(t)

	base := slog.Default()
	base.With("branch", "a").InfoContext(ctx, "first")
	base.With("branch", "b").InfoContext(ctx, "second")
	base.InfoContext(ctx, "third")

	lines := logs.Lines()
	if len(lines) != 3 {
		t.Fatalf("buffered %d lines, want 3", len(lines))
	}
	if lines[0].Attrs["branch"] != "a" {
		t.Errorf("first branch = %v, want a", lines[0].Attrs["branch"])
	}
	if lines[1].Attrs["branch"] != "b" {
		t.Errorf("second branch = %v, want b", lines[1].Attrs["branch"])
	}
	if _, ok := lines[2].Attrs["branch"]; ok {
		t.Errorf("third line inherited branch = %v, want none", lines[2].Attrs["branch"])
	}
}

func TestLogCaptureConcurrentHandleOnOneSpan(t *testing.T) {
	captureForTest(t)
	ctx, logs := tracedContext(t)

	const goroutines, perGoroutine = 8, 25
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				slog.InfoContext(ctx, "concurrent", "worker", worker)
			}
		}(i)
	}
	wg.Wait()

	if got := logs.Len(); got != goroutines*perGoroutine {
		t.Errorf("buffered %d lines, want %d", got, goroutines*perGoroutine)
	}
}

func TestStartSpanLogsNoOpWithoutCapture(t *testing.T) {
	ctx := WithTraceContext(context.Background(), GenerateTraceID(), GenerateSpanID())

	got, logs := StartSpanLogs(ctx)
	if logs != nil {
		t.Error("StartSpanLogs returned a buffer with capture off, want nil")
	}
	if got != ctx {
		t.Error("StartSpanLogs modified the context with capture off")
	}
}

func TestStartSpanLogsNoOpWithoutTrace(t *testing.T) {
	captureForTest(t)

	ctx := context.Background()
	got, logs := StartSpanLogs(ctx)
	if logs != nil {
		t.Error("StartSpanLogs returned a buffer for an untraced context, want nil")
	}
	if got != ctx {
		t.Error("StartSpanLogs modified an untraced context")
	}
}

func TestLogHandlerNilWhenCaptureOff(t *testing.T) {
	if h := LogHandler(); h != nil {
		t.Errorf("LogHandler() = %v with capture off, want nil", h)
	}
}

func TestLogHandlerReturnsInstalledHandler(t *testing.T) {
	captureForTest(t)

	if LogHandler() == nil {
		t.Fatal("LogHandler() = nil with capture on, want the handler")
	}
}

func TestInitDefaultLeavesSlogUnchanged(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	if _, err := initForTest(t, testConfig()); err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	if slog.Default() != previous {
		t.Error("default config replaced the slog default logger")
	}
	if h := LogHandler(); h != nil {
		t.Errorf("LogHandler() = %v with capture off, want nil", h)
	}
}

func TestInitWithLogCaptureInstallsTee(t *testing.T) {
	previous := slog.Default()
	sink := &recordingHandler{level: slog.LevelInfo}
	slog.SetDefault(slog.New(sink))
	t.Cleanup(func() { slog.SetDefault(previous) })

	config := testConfig()
	config.LogCapture = true
	if _, err := initForTest(t, config); err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	handler := LogHandler()
	if handler == nil {
		t.Fatal("LogHandler() = nil after init with LogCapture, want the handler")
	}
	if _, ok := slog.Default().Handler().(*logHandler); !ok {
		t.Fatalf("slog default handler = %T, want *logHandler", slog.Default().Handler())
	}

	ctx, logs := tracedContext(t)
	slog.InfoContext(ctx, "teed line")

	if got := logs.Len(); got != 1 {
		t.Errorf("buffered %d lines, want 1", got)
	}
	if msgs := sink.messages(); len(msgs) != 1 || msgs[0] != "teed line" {
		t.Errorf("previous handler saw %v, want [teed line]", msgs)
	}
}

func TestShutdownRestoresPreviousLogger(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	config := testConfig()
	config.LogCapture = true
	if _, err := initForTest(t, config); err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}
	if err := Shutdown(); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	if slog.Default() != previous {
		t.Error("Shutdown did not restore the previous slog default logger")
	}
	if h := LogHandler(); h != nil {
		t.Errorf("LogHandler() = %v after Shutdown, want nil", h)
	}
}

func TestMiddlewareCapturesRequestLogs(t *testing.T) {
	captureForTest(t)

	config := testConfig()
	config.LogCapture = true
	if _, err := initForTest(t, config); err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	var logs *SpanLogs
	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logs = SpanLogsFrom(ctx)
		slog.InfoContext(ctx, "handling request", "path", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/orders", nil))

	if logs == nil {
		t.Fatal("no log buffer was bound to the request span")
	}
	lines := logs.Lines()
	if len(lines) != 1 {
		t.Fatalf("buffered %d lines, want 1", len(lines))
	}
	if lines[0].Message != "handling request" {
		t.Errorf("Message = %q, want %q", lines[0].Message, "handling request")
	}
	if lines[0].Attrs["path"] != "/orders" {
		t.Errorf("Attrs[path] = %v, want /orders", lines[0].Attrs["path"])
	}
	if logs.TraceID() == "" || logs.SpanID() == "" {
		t.Errorf("buffer bound to %q/%q, want a trace and span id", logs.TraceID(), logs.SpanID())
	}
}

func TestConfigFromEnvLogCapture(t *testing.T) {
	if cfg := configFromEnv(); cfg.LogCapture {
		t.Error("LogCapture = true with VELWATCH_LOG_CAPTURE unset, want false")
	}

	t.Setenv("VELWATCH_LOG_CAPTURE", "true")
	if cfg := configFromEnv(); !cfg.LogCapture {
		t.Error("LogCapture = false with VELWATCH_LOG_CAPTURE=true, want true")
	}

	if err := os.Setenv("VELWATCH_LOG_CAPTURE", "1"); err != nil {
		t.Fatalf("Setenv returned error: %v", err)
	}
	if cfg := configFromEnv(); cfg.LogCapture {
		t.Error(`LogCapture = true with VELWATCH_LOG_CAPTURE=1, want false (only "true" enables it)`)
	}
}

func TestLogCaptureNormalizesAttributeValues(t *testing.T) {
	captureForTest(t)
	ctx, logs := tracedContext(t)

	stamp := time.Date(2026, 3, 1, 12, 30, 45, 123456789, time.UTC)
	slog.InfoContext(ctx, "value kinds",
		"text", "hello",
		"ok", true,
		"count", 7,
		"ratio", 1.5,
		"took", 250*time.Millisecond,
		"at", stamp,
		"err", errors.New("boom"),
		"other", []string{"a", "b"},
	)

	lines := logs.Lines()
	if len(lines) != 1 {
		t.Fatalf("buffered %d lines, want 1", len(lines))
	}
	want := map[string]any{
		"text":  "hello",
		"ok":    true,
		"count": int64(7),
		"ratio": 1.5,
		"took":  "250ms",
		"at":    stamp.Format(time.RFC3339Nano),
		"err":   "boom",
		"other": "[a b]",
	}
	for key, value := range want {
		if got := lines[0].Attrs[key]; got != value {
			t.Errorf("Attrs[%q] = %v (%T), want %v (%T)", key, got, got, value, value)
		}
	}
}
