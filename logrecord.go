package velwatch

import "log/slog"

// OTLP severity numbers for the levels slog defines. OTLP groups severities
// into ranges (debug 5-8, info 9-12, warn 13-16, error 17-20); a level maps to
// the base of its range, so the numbers stay stable and comparable.
const (
	severityNumberDebug = 5
	severityNumberInfo  = 9
	severityNumberWarn  = 13
	severityNumberError = 17
)

// logLevelName returns the lowercase level name a log record is stored under.
// A custom level (slog.LevelInfo+2 for a "notice", say) falls into the nearest
// bucket at or below it, which is how slog itself renders custom levels.
func logLevelName(level slog.Level) string {
	switch {
	case level < slog.LevelInfo:
		return "debug"
	case level < slog.LevelWarn:
		return "info"
	case level < slog.LevelError:
		return "warn"
	default:
		return "error"
	}
}

// logSeverityNumber returns the OTLP severity number for a slog level, using
// the same buckets as logLevelName.
func logSeverityNumber(level slog.Level) int {
	switch {
	case level < slog.LevelInfo:
		return severityNumberDebug
	case level < slog.LevelWarn:
		return severityNumberInfo
	case level < slog.LevelError:
		return severityNumberWarn
	default:
		return severityNumberError
	}
}

// Events converts the buffered lines into log events, in capture order. Every
// event carries the trace, span and parent ids of the span the lines were
// emitted in, and keeps the timestamp slog stamped on the record.
//
// The buffer is left intact: converting it twice yields two equivalent sets of
// events. A nil buffer converts to no events, so a caller need not check
// whether log capture is on.
func (s *SpanLogs) Events() []*Event {
	if s == nil {
		return nil
	}
	lines := s.Lines()
	if len(lines) == 0 {
		return nil
	}

	events := make([]*Event, 0, len(lines))
	for _, line := range lines {
		event := NewLogEvent(line)
		event.TraceID = s.traceID
		event.SpanID = s.spanID
		if s.parentID != "" {
			parentID := s.parentID
			event.ParentID = &parentID
		}
		events = append(events, event)
	}
	return events
}

// RecordSpanLogs converts the lines buffered on logs into log records and
// queues them on the SDK pipeline, so they are batched and flushed with the
// span they belong to. Call it once, where the span ends:
//
//	ctx, logs := velwatch.StartSpanLogs(ctx)
//	defer velwatch.RecordSpanLogs(logs)
//
// Middleware does this for every instrumented request. A job, a console
// command or any other span brackets itself the same way.
//
// It is a no-op on a nil buffer and while the SDK is dormant or disabled, so
// the call costs nothing in a build that never turns log capture on.
func RecordSpanLogs(logs *SpanLogs) {
	if logs == nil {
		return
	}

	mu.Lock()
	sdk := instance
	mu.Unlock()
	if sdk == nil || sdk.config.Disabled || sdk.collector == nil {
		return
	}

	for _, event := range logs.Events() {
		event.setDefaultTag("service", sdk.config.ServiceName)
		sdk.collector.Add(event)
	}
}
