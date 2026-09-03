package velwatch

import (
	"log/slog"
	"time"
)

// OTLP severity numbers for the levels slog defines. OTLP groups severities
// into ranges (debug 5-8, info 9-12, warn 13-16, error 17-20); a level maps to
// the base of its range, so the numbers stay stable and comparable.
const (
	severityNumberDebug = 5
	severityNumberInfo  = 9
	severityNumberWarn  = 13
	severityNumberError = 17
)

// LogLine is a single log line on its way to Velwatch. It holds everything an
// OTLP LogRecord needs: the time the line was written, its severity, the
// message, and its key/value arguments flattened to dotted keys.
//
// A log line is traceless. It carries no trace, span or parent id, and is
// searchable by service, level, message and time.
type LogLine struct {
	// Time is when the line was written. It can be the zero time when the
	// caller built the line without one.
	Time time.Time

	// Level is the slog severity the line was written at. Velocity's log
	// levels map onto the four slog defines; Fatal is recorded as error.
	Level slog.Level

	// Message is the log message.
	Message string

	// Attrs holds the line's key/value arguments flattened to dotted keys
	// ("db.query.table"). It may be nil for a bare message.
	Attrs map[string]any
}

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
