package velwatch

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	vlog "github.com/velocitykode/velocity/log"
	"github.com/velocitykode/velocity/log/logtest"

	// The stack driver lives in its own leaf and self-registers from that
	// package's init, so a stack channel needs this blank import (or the
	// log/standard aggregator). The README says the same to applications.
	_ "github.com/velocitykode/velocity/log/stack"
)

// shippingSDK initializes the SDK with log shipping on and returns it. The
// collector is given a batch large enough and an interval long enough that
// nothing leaves during the test, so the queued events can be read back.
func shippingSDK(t *testing.T, adjust func(*Config)) *SDK {
	t.Helper()

	config := testConfig()
	config.LogCapture = true
	config.BatchSize = 1000
	config.FlushInterval = time.Hour
	if adjust != nil {
		adjust(&config)
	}

	sdk, err := initForTest(t, config)
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}
	return sdk
}

// logEvents returns the log events queued on the collector.
func logEvents(c *Collector) []*Event {
	var out []*Event
	for _, event := range getEvents(c) {
		if event.Type == EventTypeLog {
			out = append(out, event)
		}
	}
	return out
}

func TestLogDriverIsRegisteredUnderItsName(t *testing.T) {
	logger, err := vlog.Drivers().Resolve(context.Background(), LogDriverName,
		vlog.LogConfig{Driver: LogDriverName})
	if err != nil {
		t.Fatalf("Resolve(%q) = %v, want the SDK's driver", LogDriverName, err)
	}
	if logger == nil {
		t.Fatalf("Resolve(%q) returned a nil logger", LogDriverName)
	}
}

func TestLogDriverSatisfiesTheVelocityLoggerContract(t *testing.T) {
	shippingSDK(t, nil)

	logtest.RunLoggerContractTests(t, func(t *testing.T) vlog.Logger {
		logger, err := vlog.Drivers().Resolve(context.Background(), LogDriverName,
			vlog.LogConfig{Driver: LogDriverName})
		if err != nil {
			t.Fatalf("Resolve(%q): %v", LogDriverName, err)
		}
		return logger
	})
}

func TestLogDriverQueuesOneTracelessEventPerLine(t *testing.T) {
	sdk := shippingSDK(t, nil)

	before := time.Now()
	logDriver{}.Info("Request completed", "method", "GET", "path", "/api/ping", "status", 200)

	events := logEvents(sdk.collector)
	if len(events) != 1 {
		t.Fatalf("queued %d log events, want 1", len(events))
	}
	event := events[0]

	if event.TraceID != "" || event.SpanID != "" || event.ParentID != nil {
		t.Errorf("event carries trace context %q/%q/%v, want a traceless event",
			event.TraceID, event.SpanID, event.ParentID)
	}
	if event.Attributes["message"] != "Request completed" {
		t.Errorf("message = %v, want Request completed", event.Attributes["message"])
	}
	if event.Attributes["level"] != "info" {
		t.Errorf("level = %v, want info", event.Attributes["level"])
	}
	if event.Attributes["severity_number"] != severityNumberInfo {
		t.Errorf("severity_number = %v, want %d", event.Attributes["severity_number"], severityNumberInfo)
	}
	if event.Attributes["method"] != "GET" || event.Attributes["path"] != "/api/ping" {
		t.Errorf("attributes = %v, want the method and path kvs", event.Attributes)
	}
	if event.Attributes["status"] != int64(200) {
		t.Errorf("status = %#v, want int64(200)", event.Attributes["status"])
	}
	if event.Tags["service"] != "test-service" {
		t.Errorf("service tag = %q, want test-service", event.Tags["service"])
	}
	if event.Timestamp.Before(before) {
		t.Errorf("Timestamp = %v, want at or after %v", event.Timestamp, before)
	}
}

func TestLogDriverMapsLevelsAndFlattensGroups(t *testing.T) {
	sdk := shippingSDK(t, func(c *Config) { c.LogLevel = slog.LevelDebug })

	logDriver{}.Debug("d")
	logDriver{}.Info("i")
	logDriver{}.Warn("w")
	logDriver{}.Error("e")
	// Fatal is recorded at error severity and must not exit the process.
	logDriver{}.Fatal("f")
	logDriver{}.Info("grouped", slog.Group("db", slog.String("host", "primary")),
		"took", 250*time.Millisecond)

	events := logEvents(sdk.collector)
	if len(events) != 6 {
		t.Fatalf("queued %d log events, want 6", len(events))
	}
	want := []string{"debug", "info", "warn", "error", "error", "info"}
	for i, level := range want {
		if events[i].Attributes["level"] != level {
			t.Errorf("event %d level = %v, want %v", i, events[i].Attributes["level"], level)
		}
	}
	grouped := events[5].Attributes
	if grouped["db.host"] != "primary" {
		t.Errorf("db.host = %v, want primary", grouped["db.host"])
	}
	if grouped["took"] != "250ms" {
		t.Errorf("took = %v, want 250ms", grouped["took"])
	}
}

func TestLogDriverHonoursTheLevelFloor(t *testing.T) {
	sdk := shippingSDK(t, func(c *Config) { c.LogLevel = slog.LevelWarn })

	logDriver{}.Debug("below")
	logDriver{}.Info("below")
	logDriver{}.Warn("at the floor")
	logDriver{}.Error("above")

	events := logEvents(sdk.collector)
	if len(events) != 2 {
		t.Fatalf("queued %d log events, want 2 (warn and error only)", len(events))
	}
	if events[0].Attributes["message"] != "at the floor" || events[1].Attributes["message"] != "above" {
		t.Errorf("queued %v, want the warn and error lines",
			[]any{events[0].Attributes["message"], events[1].Attributes["message"]})
	}
}

func TestLogDriverCapsLinesPerSecond(t *testing.T) {
	sdk := shippingSDK(t, func(c *Config) { c.LogMaxPerSecond = 2 })

	before := LogsDroppedRate()
	for i := 0; i < 5; i++ {
		logDriver{}.Info("hot loop")
	}

	if got := len(logEvents(sdk.collector)); got != 2 {
		t.Errorf("queued %d log events, want 2 (the per-second cap)", got)
	}
	if dropped := LogsDroppedRate() - before; dropped != 3 {
		t.Errorf("LogsDroppedRate grew by %d, want 3", dropped)
	}
}

func TestLogRateLimiterRefillsEachWindow(t *testing.T) {
	limiter := newLogRateLimiter(2)
	start := time.Now()

	if !limiter.allow(start) || !limiter.allow(start) {
		t.Fatal("the first two lines in a window should be allowed")
	}
	if limiter.allow(start) {
		t.Error("the third line in the same window should be refused")
	}
	if !limiter.allow(start.Add(time.Second)) {
		t.Error("the next window should allow again")
	}
}

func TestLogDriverIsANoOpWhileTheSDKIsDormant(t *testing.T) {
	// No SDK initialized: the registered driver must return immediately,
	// drop the line, and count nothing as rate-dropped.
	activeLogDriver.Store(nil)

	before := LogsDroppedRate()
	logDriver{}.Info("nobody is listening", "k", "v")
	logDriver{}.Error("still nobody")

	if got := LogsDroppedRate() - before; got != 0 {
		t.Errorf("LogsDroppedRate grew by %d while dormant, want 0", got)
	}
}

func TestLogDriverShipsNothingWithLogCaptureOff(t *testing.T) {
	config := testConfig()
	config.BatchSize = 1000
	config.FlushInterval = time.Hour
	sdk, err := initForTest(t, config) // LogCapture left off
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	logDriver{}.Error("should not ship")

	if got := len(logEvents(sdk.collector)); got != 0 {
		t.Errorf("queued %d log events with LogCapture off, want 0", got)
	}
}

func TestLogDriverStopsShippingAfterShutdown(t *testing.T) {
	sdk := shippingSDK(t, nil)

	if err := Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	logDriver{}.Info("after shutdown")

	if got := len(logEvents(sdk.collector)); got != 0 {
		t.Errorf("queued %d log events after shutdown, want 0", got)
	}
}

// TestVelocityStackReachesConsoleAndVelwatch is the proof an application
// wants: with LOG_DRIVER=stack and LOG_STACK=console,velwatch, one ordinary
// log call prints on the console and arrives at Velwatch, with no call-site
// change of any kind.
func TestVelocityStackReachesConsoleAndVelwatch(t *testing.T) {
	sdk := shippingSDK(t, nil)

	logger, err := vlog.NewLogger(vlog.LogConfig{
		Driver: "stack",
		Config: map[string]any{
			"level":    "debug",
			"channels": []string{"console", LogDriverName},
		},
	})
	if err != nil {
		t.Fatalf("NewLogger(stack console+velwatch): %v", err)
	}

	printed := captureStdout(t, func() {
		logger.Info("Request | method=GET path=/api/ping")
	})

	if !strings.Contains(printed, "Request | method=GET path=/api/ping") {
		t.Errorf("console output = %q, want the log line", printed)
	}
	events := logEvents(sdk.collector)
	if len(events) != 1 {
		t.Fatalf("queued %d log events, want 1", len(events))
	}
	if events[0].Attributes["message"] != "Request | method=GET path=/api/ping" {
		t.Errorf("shipped message = %v, want the log line", events[0].Attributes["message"])
	}
}

func TestVelocityStackFailsLoudlyOnAnUnknownChannel(t *testing.T) {
	if _, err := vlog.NewLogger(vlog.LogConfig{
		Driver: "stack",
		Config: map[string]any{"channels": []string{"console", "velwtach"}},
	}); err == nil {
		t.Error("a stack naming a misspelled channel should fail to build")
	}
}

// captureStdout runs fn with os.Stdout redirected to a pipe and returns what
// was written. The console driver prints with fmt.Println, which resolves
// os.Stdout at call time, so the swap is enough.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	previous := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = previous }()

	fn()

	if err := writer.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}
	_ = reader.Close()
	return string(out)
}

func TestFlattenKVsSurvivesDegenerateArguments(t *testing.T) {
	attrs := flattenKVs([]any{42, "answer", "nil", nil, "lonely"})

	if attrs["42"] != "answer" {
		t.Errorf("42 = %v, want answer (a non-string key is rendered with %%v)", attrs["42"])
	}
	if attrs["nil"] != "<nil>" {
		t.Errorf("nil = %#v, want the rendered nil", attrs["nil"])
	}
	if attrs[badKVKey] != "lonely" {
		t.Errorf("%s = %v, want lonely", badKVKey, attrs[badKVKey])
	}
	if flattenKVs(nil) != nil {
		t.Error("flattenKVs(nil) should allocate nothing")
	}
}
