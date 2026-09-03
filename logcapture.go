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

	// maxLines is the most lines this span will buffer, zero for no cap.
	// It is fixed when the buffer is created, so a configuration change
	// cannot move the ceiling out from under a span that is already running.
	maxLines int

	mu      sync.Mutex
	lines   []LogLine
	outcome SpanOutcome

	// droppedByKeepRule counts lines the tail-based keep rules discarded
	// when the span ended; droppedByCap counts lines a per-span cap refused
	// to buffer in the first place, and droppedByFloor lines the capture
	// level floor refused. They are kept apart because they mean different
	// things: the first is the SDK working as configured, the other two are
	// a span offering more than the SDK was told to hold.
	droppedByKeepRule uint64
	droppedByCap      uint64
	droppedByFloor    uint64
}

// TraceID returns the trace this buffer belongs to.
func (s *SpanLogs) TraceID() string { return s.traceID }

// SpanID returns the span this buffer belongs to.
func (s *SpanLogs) SpanID() string { return s.spanID }

// ParentID returns the parent of the span this buffer belongs to, empty when
// the span is the root of its trace.
func (s *SpanLogs) ParentID() string { return s.parentID }

// append adds a line to the buffer. It reports whether the line was kept; a
// rejected line increments the dropped counter instead of growing the buffer.
//
// The per-span cap is enforced here, at capture time, so one span cannot hold
// unbounded memory however much it logs. Once the buffer holds maxLines, only
// error lines are still taken: a span that emits five hundred info lines and
// then fails still records the error that explains it. Error lines get their
// own headroom of another maxLines on top of the cap, after which they are
// refused too, so a span that logs an error per loop iteration is bounded at
// twice the cap rather than not at all. Refused lines are counted on
// DroppedByCap.
func (s *SpanLogs) append(line LogLine) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maxLines > 0 && len(s.lines) >= s.maxLines {
		if line.Level < slog.LevelError || len(s.lines) >= s.hardCap() {
			return false
		}
	}
	s.lines = append(s.lines, line)
	return true
}

// hardCap is the ceiling no line, error or not, is buffered past: the cap plus
// an equal headroom reserved for error lines. Callers must hold s.mu.
func (s *SpanLogs) hardCap() int {
	return s.maxLines * 2
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
// those the keep rules discarded when the span ended plus those the per-span
// cap and the capture level floor refused to buffer.
func (s *SpanLogs) Dropped() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.droppedByKeepRule + s.droppedByCap + s.droppedByFloor
}

// DroppedAtCapture returns the lines this span offered that never reached the
// buffer: those refused by the per-span cap plus those below the capture level
// floor. It excludes the keep rules, which run later and on lines that were
// buffered, so the number is final as soon as the span's work is done.
//
// This is the quantity reported on the span's own record as "log.dropped".
// Middleware attaches it to every request record; a job or console command
// that brackets its own span can attach the same value to its record. It is
// a no-op returning zero on a nil buffer.
func (s *SpanLogs) DroppedAtCapture() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.droppedByCap + s.droppedByFloor
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

// DroppedByFloor returns how many lines were never buffered because they were
// below the configured capture level floor.
func (s *SpanLogs) DroppedByFloor() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.droppedByFloor
}

// dropByCap records a line the per-span cap refused to buffer.
func (s *SpanLogs) dropByCap() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.droppedByCap++
}

// dropByFloor records a line the capture level floor refused to buffer.
func (s *SpanLogs) dropByFloor() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.droppedByFloor++
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
// The buffer holds at most VELWATCH_LOG_MAX_LINES lines, plus any error lines
// emitted after that ceiling is reached.
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
	logs := &SpanLogs{
		traceID:  traceID,
		spanID:   spanID,
		parentID: GetParentID(ctx),
		maxLines: int(logMaxLines.Load()),
	}
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
