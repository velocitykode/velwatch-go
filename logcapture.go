package velwatch

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// LogLine is a single log record captured while a span was active. It holds
// everything an OTLP LogRecord needs: the timestamp slog stamped on the
// record, the severity, the message, and the record's attributes flattened to
// dotted keys. The span it belongs to supplies the trace and span IDs.
type LogLine struct {
	// Time is the timestamp from the slog record. It can be the zero time
	// when the caller built the record without one.
	Time time.Time

	// Level is the slog severity of the record.
	Level slog.Level

	// Message is the log message.
	Message string

	// Attrs holds the record's attributes plus any handler attributes,
	// flattened to dotted keys ("db.query.table"). Never nil.
	Attrs map[string]any
}

// SpanLogs buffers the log lines captured while one span is active. A buffer
// is bound to a span by StartSpanLogs and carried on the context, so log
// lines emitted anywhere under that context land in this buffer.
//
// It is safe for concurrent use: a handler may be called from several
// goroutines running under the same span.
type SpanLogs struct {
	traceID  string
	spanID   string
	parentID string

	mu      sync.Mutex
	lines   []LogLine
	outcome SpanOutcome

	// droppedByKeepRule counts lines the tail-based keep rules discarded
	// when the span ended; droppedByCap counts lines a per-span cap refused
	// to buffer in the first place. They are kept apart because they mean
	// different things: the first is the SDK working as configured, the
	// second is a span logging more than the SDK will hold.
	droppedByKeepRule uint64
	droppedByCap      uint64
}

// TraceID returns the trace this buffer belongs to.
func (s *SpanLogs) TraceID() string { return s.traceID }

// SpanID returns the span this buffer belongs to.
func (s *SpanLogs) SpanID() string { return s.spanID }

// ParentID returns the parent of the span this buffer belongs to, empty when
// the span is the root of its trace.
func (s *SpanLogs) ParentID() string { return s.parentID }

// append adds a line to the buffer. It reports whether the line was kept.
// Keep rules and a per-span cap hook in here: a rejected line increments the
// dropped counter instead of growing the buffer.
func (s *SpanLogs) append(line LogLine) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, line)
	return true
}

// Lines returns a copy of the buffered lines in capture order.
func (s *SpanLogs) Lines() []LogLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LogLine, len(s.lines))
	copy(out, s.lines)
	return out
}

// Len returns the number of buffered lines.
func (s *SpanLogs) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.lines)
}

// Dropped returns the number of lines this span produced that were not sent:
// those the keep rules discarded when the span ended plus those a per-span cap
// refused to buffer.
func (s *SpanLogs) Dropped() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.droppedByKeepRule + s.droppedByCap
}

// DroppedByKeepRule returns how many buffered lines the keep rules discarded
// when the span ended.
func (s *SpanLogs) DroppedByKeepRule() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.droppedByKeepRule
}

// DroppedByCap returns how many lines were never buffered because the span
// had already reached its per-span cap.
func (s *SpanLogs) DroppedByCap() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.droppedByCap
}

// drop records a line that was produced under this span but not buffered.
func (s *SpanLogs) drop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.droppedByCap++
}

// dropByKeepRule records n buffered lines the keep rules discarded at span end.
func (s *SpanLogs) dropByKeepRule(n uint64) {
	if n == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.droppedByKeepRule += n
}

// spanLogsKey is the context key the per-span buffer is carried under.
type spanLogsKey struct{}

// logsDroppedOutsideSpan counts log records the capture handler saw with no
// active span on their context. Those lines are dropped: log capture exists to
// attach lines to a trace, and a line with no span has nothing to attach to.
var logsDroppedOutsideSpan atomic.Uint64

// LogsDroppedOutsideSpan returns how many log records were dropped because no
// span was active on their context since the process started.
func LogsDroppedOutsideSpan() uint64 {
	return logsDroppedOutsideSpan.Load()
}

// StartSpanLogs binds a fresh log buffer to the span active on ctx and
// returns a context carrying it. Log records emitted with that context (or a
// context derived from it) are buffered on the returned SpanLogs.
//
// It returns ctx unchanged and a nil buffer when log capture is off
// (VELWATCH_LOG_CAPTURE is not "true") or when ctx carries no trace context,
// so callers pay nothing for it in the default configuration.
//
// Middleware calls this for every instrumented request. Call it directly at
// the top of a job or console command to capture the log lines it produces.
func StartSpanLogs(ctx context.Context) (context.Context, *SpanLogs) {
	if ctx == nil || !logCaptureEnabled() {
		return ctx, nil
	}
	traceID := GetTraceID(ctx)
	spanID := GetSpanID(ctx)
	if traceID == "" && spanID == "" {
		return ctx, nil
	}
	logs := &SpanLogs{traceID: traceID, spanID: spanID, parentID: GetParentID(ctx)}
	return context.WithValue(ctx, spanLogsKey{}, logs), logs
}

// SpanLogsFrom returns the log buffer bound to ctx, or nil when no span is
// active on it. This is the same "is a span active" lookup the sensors and
// middleware use, narrowed to the buffer the span carries.
func SpanLogsFrom(ctx context.Context) *SpanLogs {
	if ctx == nil {
		return nil
	}
	logs, _ := ctx.Value(spanLogsKey{}).(*SpanLogs)
	return logs
}
