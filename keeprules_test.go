package velwatch

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/velocitykode/velocity/events"
	"github.com/velocitykode/velocity/router"
)

// unsampledRate is a sample rate low enough that no trace id these tests
// generate wins its deterministic verdict, while staying above zero:
// initialization treats a zero rate as "unset" and resets it to 1.0. Every
// test using it asserts the verdict rather than trusting the odds.
const unsampledRate = 1e-9

// logEventsIn returns the log records queued on the collector, in order.
func logEventsIn(c *Collector) []*Event {
	var logs []*Event
	for _, event := range getEvents(c) {
		if event.Type == EventTypeLog {
			logs = append(logs, event)
		}
	}
	return logs
}

// messagesOf returns the message attribute of each log record.
func messagesOf(records []*Event) []string {
	out := make([]string, len(records))
	for i, record := range records {
		out[i], _ = record.Attributes["message"].(string)
	}
	return out
}

// keepRulesConfig is a config with log capture on, batching effectively
// disabled so queued records stay on the collector, and a one second slow
// threshold.
func keepRulesConfig(sampleRate float64) Config {
	config := testConfig()
	config.LogCapture = true
	config.BatchSize = 1000
	config.FlushInterval = time.Hour
	config.SampleRate = sampleRate
	config.LogSlowThreshold = time.Second
	return config
}

func TestSpanLogsKeepRules(t *testing.T) {
	type line struct {
		level   slog.Level
		message string
	}
	healthy := []line{
		{slog.LevelInfo, "loading order"},
		{slog.LevelInfo, "order loaded"},
		{slog.LevelWarn, "cache miss"},
	}

	cases := []struct {
		name        string
		sampleRate  float64
		lines       []line
		outcome     SpanOutcome
		wantSent    []string
		wantDropped uint64
	}{
		{
			name:       "errored span keeps every line",
			sampleRate: unsampledRate,
			lines:      healthy,
			outcome:    SpanOutcome{Failed: true, Duration: 5 * time.Millisecond},
			wantSent:   []string{"loading order", "order loaded", "cache miss"},
		},
		{
			name:       "slow span keeps every line",
			sampleRate: unsampledRate,
			lines:      healthy,
			outcome:    SpanOutcome{Duration: 2 * time.Second},
			wantSent:   []string{"loading order", "order loaded", "cache miss"},
		},
		{
			name:        "healthy unsampled span keeps warn and above",
			sampleRate:  unsampledRate,
			lines:       healthy,
			outcome:     SpanOutcome{Duration: 5 * time.Millisecond},
			wantSent:    []string{"cache miss"},
			wantDropped: 2,
		},
		{
			// Every line here is at or above the capture floor, so the
			// keep rules see all three. Lines below the floor never reach
			// a buffer at all; that is the floor's job, tested separately.
			name:       "healthy unsampled span with no warn sends nothing",
			sampleRate: unsampledRate,
			lines: []line{
				{slog.LevelInfo, "cache lookup"},
				{slog.LevelInfo, "loading order"},
				{slog.LevelInfo, "order loaded"},
			},
			outcome:     SpanOutcome{Duration: 5 * time.Millisecond},
			wantSent:    nil,
			wantDropped: 3,
		},
		{
			name:       "healthy sampled span keeps every line",
			sampleRate: 1.0,
			lines:      healthy,
			outcome:    SpanOutcome{Duration: 5 * time.Millisecond},
			wantSent:   []string{"loading order", "order loaded", "cache miss"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			captureForTest(t)
			sdk, err := initForTest(t, keepRulesConfig(tc.sampleRate))
			if err != nil {
				t.Fatalf("initLocked returned error: %v", err)
			}

			ctx, logs := tracedContext(t)
			if got := traceSampled(logs.TraceID(), tc.sampleRate); got != (tc.sampleRate >= 1.0) {
				t.Fatalf("traceSampled(%q, %v) = %v, want %v",
					logs.TraceID(), tc.sampleRate, got, tc.sampleRate >= 1.0)
			}
			for _, l := range tc.lines {
				slog.Log(ctx, l.level, l.message)
			}

			logs.SetOutcome(tc.outcome)
			RecordSpanLogs(logs)

			sent := messagesOf(logEventsIn(sdk.collector))
			if strings.Join(sent, "|") != strings.Join(tc.wantSent, "|") {
				t.Errorf("sent %v, want %v", sent, tc.wantSent)
			}
			if got := logs.DroppedByKeepRule(); got != tc.wantDropped {
				t.Errorf("DroppedByKeepRule() = %d, want %d", got, tc.wantDropped)
			}
			if got := logs.Dropped(); got != tc.wantDropped {
				t.Errorf("Dropped() = %d, want %d", got, tc.wantDropped)
			}
			if got := logs.DroppedByCap(); got != 0 {
				t.Errorf("DroppedByCap() = %d, want 0", got)
			}
			if got := logs.DroppedByFloor(); got != 0 {
				t.Errorf("DroppedByFloor() = %d, want 0", got)
			}
		})
	}
}

func TestSpanLogsMarkFailedKeepsEveryLine(t *testing.T) {
	captureForTest(t)
	sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	ctx, logs := tracedContext(t)
	slog.InfoContext(ctx, "starting job")
	logs.MarkFailed()
	logs.SetOutcome(SpanOutcome{Failed: logs.Outcome().Failed, Duration: time.Millisecond})
	RecordSpanLogs(logs)

	if got := len(logEventsIn(sdk.collector)); got != 1 {
		t.Errorf("sent %d records, want 1", got)
	}
}

func TestApplyKeepRulesSlowThresholdBoundary(t *testing.T) {
	lines := []LogLine{{Level: slog.LevelInfo, Message: "tick"}}
	threshold := time.Second

	kept, dropped := applyKeepRules(lines, SpanOutcome{Duration: threshold}, "trace-a", threshold, unsampledRate)
	if len(kept) != 0 || dropped != 1 {
		t.Errorf("a span exactly at the threshold kept %d / dropped %d, want 0 / 1", len(kept), dropped)
	}

	kept, dropped = applyKeepRules(lines, SpanOutcome{Duration: threshold + time.Nanosecond}, "trace-a", threshold, unsampledRate)
	if len(kept) != 1 || dropped != 0 {
		t.Errorf("a span past the threshold kept %d / dropped %d, want 1 / 0", len(kept), dropped)
	}
}

func TestTraceSampledIsDeterministicAndProportional(t *testing.T) {
	if !traceSampled("any-trace", 1.0) {
		t.Error("rate 1.0 dropped a trace, want every trace kept")
	}
	if traceSampled("any-trace", 0) {
		t.Error("rate 0 kept a trace, want every trace dropped")
	}
	if !traceSampled("", 0.5) {
		t.Error("a span with no trace id was dropped, want fail open")
	}

	first := traceSampled("stable-trace-id", 0.5)
	for i := 0; i < 100; i++ {
		if traceSampled("stable-trace-id", 0.5) != first {
			t.Fatal("traceSampled is not deterministic for one trace id")
		}
	}

	var kept int
	const total = 5000
	for i := 0; i < total; i++ {
		if traceSampled(GenerateTraceID(), 0.25) {
			kept++
		}
	}
	if kept < total/8 || kept > total*3/8 {
		t.Errorf("kept %d of %d traces at rate 0.25, want roughly a quarter", kept, total)
	}
}

// TestExceptionsMarkTheSpanFailed pins the "a recorded exception or panic
// fails the span" half of the keep rules: neither RecordException nor the
// request.failed listener needs the middleware's status code to keep the
// span's lines, and the mark lands even when the exception event itself is
// sampled out.
func TestExceptionsMarkTheSpanFailed(t *testing.T) {
	captureForTest(t)
	if _, err := initForTest(t, keepRulesConfig(unsampledRate)); err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}

	t.Run("RecordException", func(t *testing.T) {
		ctx, logs := tracedContext(t)
		RecordException(ctx, "BoomError", "boom", "")
		if !logs.Outcome().Failed {
			t.Error("RecordException left the span healthy, want it marked failed")
		}
	})

	t.Run("request.failed listener", func(t *testing.T) {
		ctx, logs := tracedContext(t)
		unsampled := NewListeners(testCollector(), events.NewDispatcher(), "test-service", 0.0)
		if err := unsampled.onRequestFailed(&router.RequestFailed{
			Context: ctx,
			Error:   errors.New("boom"),
		}); err != nil {
			t.Fatalf("onRequestFailed returned error: %v", err)
		}
		if !logs.Outcome().Failed {
			t.Error("request.failed left the span healthy, want it marked failed even when the event is sampled out")
		}
	})

	t.Run("nil buffer is a no-op", func(t *testing.T) {
		RecordException(context.Background(), "BoomError", "boom", "")
	})
}

func TestSetOutcomeKeepsAnEarlierFailure(t *testing.T) {
	logs := &SpanLogs{}
	logs.MarkFailed()
	logs.SetOutcome(SpanOutcome{Duration: time.Millisecond})

	outcome := logs.Outcome()
	if !outcome.Failed {
		t.Error("SetOutcome cleared a failure MarkFailed had recorded")
	}
	if outcome.Duration != time.Millisecond {
		t.Errorf("Duration = %s, want 1ms", outcome.Duration)
	}
}
