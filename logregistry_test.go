package velwatch

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/velocitykode/velocity/queue"
	"github.com/velocitykode/velocity/router"
	"github.com/velocitykode/velocity/scheduler"
)

// frameworkContext returns a context carrying only the trace ids velocity's
// router, queue worker and scheduler put on theirs: a trace, and no log
// buffer, since the framework owns the context and the SDK never sees it
// before the first log line.
func frameworkContext(t *testing.T) (ctx context.Context, traceID, spanID string) {
	t.Helper()
	traceID, spanID = GenerateTraceID(), GenerateSpanID()
	return WithTraceContext(context.Background(), traceID, spanID), traceID, spanID
}

func requestHandled(ctx context.Context, traceID, spanID string, status int, duration time.Duration) *router.RequestHandled {
	return &router.RequestHandled{
		Context:    ctx,
		RequestID:  "req-1",
		Method:     http.MethodGet,
		Path:       "/orders",
		Route:      "/orders",
		StatusCode: status,
		Duration:   duration,
		TraceID:    traceID,
		SpanID:     spanID,
	}
}

func TestFrameworkRequestLogsAreClosedByRequestHandled(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		duration time.Duration
		want     string
	}{
		{"500 keeps every line", http.StatusInternalServerError, 5 * time.Millisecond, "handling request|cache miss"},
		{"slow 200 keeps every line", http.StatusOK, 3 * time.Second, "handling request|cache miss"},
		{"fast 200 on an unsampled trace keeps only the warn line", http.StatusOK, 5 * time.Millisecond, "cache miss"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			captureForTest(t)
			sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
			if err != nil {
				t.Fatalf("initLocked returned error: %v", err)
			}

			ctx, traceID, spanID := frameworkContext(t)
			droppedBefore := LogsDroppedOutsideSpan()
			slog.InfoContext(ctx, "handling request")
			slog.WarnContext(ctx, "cache miss")
			if got := activeSpanLogs.len(); got != 1 {
				t.Fatalf("registry holds %d buffers after logging under a framework span, want 1", got)
			}
			if got := LogsDroppedOutsideSpan() - droppedBefore; got != 0 {
				t.Fatalf("LogsDroppedOutsideSpan grew by %d, want 0: the lines carried a trace id", got)
			}

			if err := sdk.listeners.onRequestHandled(requestHandled(ctx, traceID, spanID, tc.status, tc.duration)); err != nil {
				t.Fatalf("onRequestHandled returned error: %v", err)
			}

			records := logEventsIn(sdk.collector)
			if sent := strings.Join(messagesOf(records), "|"); sent != tc.want {
				t.Errorf("sent %q, want %q", sent, tc.want)
			}
			for _, record := range records {
				if record.TraceID != traceID || record.SpanID != spanID {
					t.Errorf("log record carries %s/%s, want the request's %s/%s", record.TraceID, record.SpanID, traceID, spanID)
				}
			}
			if got := activeSpanLogs.len(); got != 0 {
				t.Errorf("registry still holds %d buffers after request.handled, want 0", got)
			}
		})
	}
}

func TestContextBoundBufferWinsOverRegistry(t *testing.T) {
	captureForTest(t)
	sdk, err := initForTest(t, keepRulesConfig(1.0))
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	ctx, logs := tracedContext(t)
	slog.InfoContext(ctx, "bound line")

	if got := activeSpanLogs.len(); got != 0 {
		t.Fatalf("registry holds %d buffers for a context-bound span, want 0", got)
	}
	if logs.Len() != 1 {
		t.Fatalf("context-bound buffer holds %d lines, want 1", logs.Len())
	}

	// The router reporting the same span ended must not ship the line: the
	// code that opened the buffer records it, once.
	if err := sdk.listeners.onRequestHandled(requestHandled(ctx, GetTraceID(ctx), GetSpanID(ctx), http.StatusInternalServerError, time.Millisecond)); err != nil {
		t.Fatalf("onRequestHandled returned error: %v", err)
	}
	if n := len(logEventsIn(sdk.collector)); n != 0 {
		t.Fatalf("request.handled recorded %d log records from a context-bound buffer, want 0", n)
	}

	RecordSpanLogs(logs)
	if n := len(logEventsIn(sdk.collector)); n != 1 {
		t.Errorf("opener recorded %d log records, want 1", n)
	}
}

func TestRequestFailedMarksTheRegisteredBufferFailed(t *testing.T) {
	captureForTest(t)
	sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	ctx, traceID, spanID := frameworkContext(t)
	slog.InfoContext(ctx, "validating payload")

	failed := &router.RequestFailed{
		Context: ctx, RequestID: "req-1", Method: http.MethodPost, Path: "/orders",
		Error: errors.New("bad payload"), TraceID: traceID, SpanID: spanID,
	}
	if err := sdk.listeners.onRequestFailed(failed); err != nil {
		t.Fatalf("onRequestFailed returned error: %v", err)
	}
	// The error was rendered as a 422, which the status rule alone would
	// call healthy.
	if err := sdk.listeners.onRequestHandled(requestHandled(ctx, traceID, spanID, http.StatusUnprocessableEntity, time.Millisecond)); err != nil {
		t.Fatalf("onRequestHandled returned error: %v", err)
	}

	if sent := strings.Join(messagesOf(logEventsIn(sdk.collector)), "|"); sent != "validating payload" {
		t.Errorf("sent %q, want the info line kept by the failed rule", sent)
	}
}

func TestRecordExceptionMarksTheRegisteredBufferFailed(t *testing.T) {
	captureForTest(t)
	sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	ctx, traceID, spanID := frameworkContext(t)
	slog.InfoContext(ctx, "charging card")
	RecordException(ctx, "PaymentError", "declined", "")

	if err := sdk.listeners.onRequestHandled(requestHandled(ctx, traceID, spanID, http.StatusOK, time.Millisecond)); err != nil {
		t.Fatalf("onRequestHandled returned error: %v", err)
	}
	if sent := strings.Join(messagesOf(logEventsIn(sdk.collector)), "|"); sent != "charging card" {
		t.Errorf("sent %q, want the info line kept by the failed rule", sent)
	}
}

func TestFrameworkJobLogsAreClosedByJobEvents(t *testing.T) {
	logLines := func(ctx context.Context) {
		slog.InfoContext(ctx, "fetching rows")
		slog.WarnContext(ctx, "retrying upstream")
	}

	t.Run("healthy fast job on an unsampled trace keeps only the warn line", func(t *testing.T) {
		captureForTest(t)
		sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
		if err != nil {
			t.Fatalf("initLocked returned error: %v", err)
		}
		ctx, traceID, spanID := frameworkContext(t)
		logLines(ctx)

		err = sdk.listeners.onJobProcessed(&queue.JobProcessed{
			Context: ctx, JobType: "*jobs.Sync", Queue: "default", DurationMs: 5, TraceID: traceID, SpanID: spanID,
		})
		if err != nil {
			t.Fatalf("onJobProcessed returned error: %v", err)
		}
		if sent := strings.Join(messagesOf(logEventsIn(sdk.collector)), "|"); sent != "retrying upstream" {
			t.Errorf("sent %q, want only the warn line", sent)
		}
		if got := activeSpanLogs.len(); got != 0 {
			t.Errorf("registry still holds %d buffers after job.processed, want 0", got)
		}
	})

	t.Run("failed job keeps every line", func(t *testing.T) {
		captureForTest(t)
		sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
		if err != nil {
			t.Fatalf("initLocked returned error: %v", err)
		}
		ctx, traceID, spanID := frameworkContext(t)
		logLines(ctx)

		err = sdk.listeners.onJobFailed(&queue.JobFailed{
			Context: ctx, JobType: "*jobs.Sync", Queue: "default", Error: "upstream down", DurationMs: 5, TraceID: traceID, SpanID: spanID,
		})
		if err != nil {
			t.Fatalf("onJobFailed returned error: %v", err)
		}
		if sent := strings.Join(messagesOf(logEventsIn(sdk.collector)), "|"); sent != "fetching rows|retrying upstream" {
			t.Errorf("sent %q, want both lines", sent)
		}
	})

	t.Run("slow job keeps every line", func(t *testing.T) {
		captureForTest(t)
		sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
		if err != nil {
			t.Fatalf("initLocked returned error: %v", err)
		}
		ctx, traceID, spanID := frameworkContext(t)
		logLines(ctx)

		err = sdk.listeners.onJobProcessed(&queue.JobProcessed{
			Context: ctx, JobType: "*jobs.Sync", Queue: "default", DurationMs: 4000, TraceID: traceID, SpanID: spanID,
		})
		if err != nil {
			t.Fatalf("onJobProcessed returned error: %v", err)
		}
		if sent := strings.Join(messagesOf(logEventsIn(sdk.collector)), "|"); sent != "fetching rows|retrying upstream" {
			t.Errorf("sent %q, want both lines", sent)
		}
	})
}

func TestFrameworkTaskLogsAreClosedByTaskEvents(t *testing.T) {
	t.Run("failed task keeps every line", func(t *testing.T) {
		captureForTest(t)
		sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
		if err != nil {
			t.Fatalf("initLocked returned error: %v", err)
		}
		ctx, traceID, spanID := frameworkContext(t)
		slog.InfoContext(ctx, "pruning sessions")

		err = sdk.listeners.onScheduledTaskFailed(&scheduler.ScheduledTaskFailed{
			Context: ctx, TaskName: "prune", Error: "lock held", DurationMs: 5, TraceID: traceID, SpanID: spanID,
		})
		if err != nil {
			t.Fatalf("onScheduledTaskFailed returned error: %v", err)
		}
		if sent := strings.Join(messagesOf(logEventsIn(sdk.collector)), "|"); sent != "pruning sessions" {
			t.Errorf("sent %q, want the info line", sent)
		}
	})

	t.Run("healthy fast task on an unsampled trace sends nothing", func(t *testing.T) {
		captureForTest(t)
		sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
		if err != nil {
			t.Fatalf("initLocked returned error: %v", err)
		}
		ctx, traceID, spanID := frameworkContext(t)
		slog.InfoContext(ctx, "pruning sessions")

		err = sdk.listeners.onScheduledTaskFinished(&scheduler.ScheduledTaskFinished{
			Context: ctx, TaskName: "prune", DurationMs: 5, TraceID: traceID, SpanID: spanID,
		})
		if err != nil {
			t.Fatalf("onScheduledTaskFinished returned error: %v", err)
		}
		if n := len(logEventsIn(sdk.collector)); n != 0 {
			t.Errorf("sent %d log records, want 0", n)
		}
		if got := activeSpanLogs.len(); got != 0 {
			t.Errorf("registry still holds %d buffers after scheduled.finished, want 0", got)
		}
	})
}

func TestFrameworkRecordsReportDroppedLogLines(t *testing.T) {
	// The sink accepts debug so the debug record is built and reaches the
	// floor, rather than being skipped by Enabled before it exists.
	captureForTestWith(t, Config{LogMaxLines: 1}, slog.LevelDebug)
	sdk, err := initForTest(t, keepRulesConfig(1.0))
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	ctx, traceID, spanID := frameworkContext(t)
	for i := 0; i < 3; i++ {
		slog.InfoContext(ctx, "chatter")
	}
	slog.DebugContext(ctx, "noise")

	if err := sdk.listeners.onRequestHandled(requestHandled(ctx, traceID, spanID, http.StatusOK, time.Millisecond)); err != nil {
		t.Fatalf("onRequestHandled returned error: %v", err)
	}

	got := requestEventIn(t, sdk.collector).Attributes["log.dropped"]
	if got != uint64(3) {
		t.Errorf("log.dropped = %v (%T), want 3: two past the cap plus one below the floor", got, got)
	}
}

func TestStaleRegisteredBuffersAreSweptAndRecorded(t *testing.T) {
	captureForTest(t)
	sdk, err := initForTest(t, keepRulesConfig(1.0))
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	ctx, _, _ := frameworkContext(t)
	slog.InfoContext(ctx, "orphan line")

	recordStaleSpanLogs(time.Now())
	if n := len(logEventsIn(sdk.collector)); n != 0 {
		t.Fatalf("a fresh buffer was swept: %d log records", n)
	}

	recordStaleSpanLogs(time.Now().Add(registryTTL + time.Second))
	if sent := strings.Join(messagesOf(logEventsIn(sdk.collector)), "|"); sent != "orphan line" {
		t.Errorf("sent %q after the sweep, want the orphan line", sent)
	}
	if got := activeSpanLogs.len(); got != 0 {
		t.Errorf("registry still holds %d buffers after the sweep, want 0", got)
	}
}

func TestRegistryRefusesSpansPastItsLimit(t *testing.T) {
	r := newSpanLogRegistry(2, time.Minute)
	ctxA, traceA, spanA := frameworkContext(t)
	ctxB, _, _ := frameworkContext(t)
	ctxC, _, _ := frameworkContext(t)

	before := LogsDroppedSpanLimit()
	a := r.attach(ctxA)
	if a == nil || r.attach(ctxB) == nil {
		t.Fatal("registry refused a span under its limit")
	}
	if r.attach(ctxC) != nil {
		t.Error("registry accepted a third span past a limit of two")
	}
	if got := LogsDroppedSpanLimit() - before; got != 1 {
		t.Errorf("LogsDroppedSpanLimit grew by %d, want 1", got)
	}
	if r.attach(ctxA) != a {
		t.Error("a second line under a registered span did not join its buffer")
	}
	if r.attach(context.Background()) != nil {
		t.Error("registry opened a buffer for a context with no trace")
	}
	if got := LogsDroppedSpanLimit() - before; got != 1 {
		t.Errorf("a traceless context counted against the span limit: grew by %d", got)
	}

	if r.detach(traceA, spanA) != a {
		t.Error("detach did not return the registered buffer")
	}
	if r.detach(traceA, spanA) != nil {
		t.Error("detach returned a buffer twice")
	}
	if r.attach(ctxC) == nil {
		t.Error("registry stayed full after a detach")
	}
}

func TestUninstallDropsRegisteredBuffers(t *testing.T) {
	captureForTest(t)
	ctx, _, _ := frameworkContext(t)
	slog.InfoContext(ctx, "line")
	if got := activeSpanLogs.len(); got != 1 {
		t.Fatalf("registry holds %d buffers, want 1", got)
	}

	uninstallLogCapture()
	if got := activeSpanLogs.len(); got != 0 {
		t.Errorf("registry holds %d buffers after uninstall, want 0", got)
	}
}

func TestRegistryTakesConcurrentLinesForOneSpan(t *testing.T) {
	captureForTest(t)
	sdk, err := initForTest(t, keepRulesConfig(1.0))
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	ctx, traceID, spanID := frameworkContext(t)
	const lines = 32
	var wg sync.WaitGroup
	for i := 0; i < lines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.InfoContext(ctx, "line")
		}()
	}
	wg.Wait()

	if err := sdk.listeners.onRequestHandled(requestHandled(ctx, traceID, spanID, http.StatusOK, time.Millisecond)); err != nil {
		t.Fatalf("onRequestHandled returned error: %v", err)
	}
	if n := len(logEventsIn(sdk.collector)); n != lines {
		t.Errorf("sent %d log records, want %d", n, lines)
	}
}
