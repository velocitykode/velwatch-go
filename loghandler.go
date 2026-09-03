package velwatch

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// flatAttr is one handler attribute already flattened to its dotted key.
type flatAttr struct {
	key   string
	value any
}

// logHandler is a slog.Handler that captures log records onto the span active
// on the record's context. It tees: every record is first offered to the
// handler that was installed before capture, so the application's own logging
// is untouched, and is then buffered on the active span.
//
// A record whose context carries no active span is dropped and counted in
// LogsDroppedOutsideSpan. The handler never logs and never panics: a log
// handler that fails loudly would take the application down with it.
type logHandler struct {
	// next is the handler capture was installed in front of. It may be nil
	// when the handler was built without one.
	next slog.Handler

	// attrs holds the WithAttrs attributes accumulated so far, already
	// flattened with the group prefix that was open when they were added.
	attrs []flatAttr

	// prefix is the dotted prefix built by WithGroup calls, e.g. "db.query.".
	prefix string
}

var _ slog.Handler = (*logHandler)(nil)

// newLogHandler returns a capture handler that forwards to next. next may be
// nil, in which case records are only captured.
func newLogHandler(next slog.Handler) *logHandler {
	return &logHandler{next: next}
}

// Enabled reports whether a record at this level should be built. Capture
// takes every level, so the answer is always yes: the level a line is kept at
// is decided when the buffered lines are filtered, not here. The tee still
// respects the wrapped handler's own level, so the application's output does
// not change.
func (h *logHandler) Enabled(context.Context, slog.Level) bool { return true }

// Handle forwards the record to the wrapped handler, then buffers it on the
// span active on ctx.
func (h *logHandler) Handle(ctx context.Context, r slog.Record) error {
	var err error
	if h.next != nil && h.next.Enabled(ctx, r.Level) {
		// Clone so a handler that retains or extends the record cannot
		// disturb the copy we are about to read.
		err = h.next.Handle(ctx, r.Clone())
	}

	logs := SpanLogsFrom(ctx)
	if logs == nil {
		logsDroppedOutsideSpan.Add(1)
		return err
	}

	attrs := make(map[string]any, len(h.attrs)+r.NumAttrs())
	for _, a := range h.attrs {
		attrs[a.key] = a.value
	}
	var recordAttrs []flatAttr
	r.Attrs(func(a slog.Attr) bool {
		flattenAttr(h.prefix, a, &recordAttrs)
		return true
	})
	for _, a := range recordAttrs {
		attrs[a.key] = a.value
	}

	if !logs.append(LogLine{
		Time:    r.Time,
		Level:   r.Level,
		Message: r.Message,
		Attrs:   attrs,
	}) {
		logs.drop()
	}
	return err
}

// WithAttrs returns a handler whose captured lines all carry attrs. The
// attributes are flattened with the group prefix open at this point, so
// WithGroup("db").WithAttrs(slog.String("table", "users")) yields "db.table".
func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	next := h.clone()
	if next.next != nil {
		next.next = next.next.WithAttrs(attrs)
	}
	for _, a := range attrs {
		flattenAttr(h.prefix, a, &next.attrs)
	}
	return next
}

// WithGroup returns a handler that qualifies every later attribute with name,
// flattened to a dotted key.
func (h *logHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	next := h.clone()
	if next.next != nil {
		next.next = next.next.WithGroup(name)
	}
	next.prefix = h.prefix + name + "."
	return next
}

// clone copies the handler, including its attribute slice, so a derived
// handler never writes through to its parent.
func (h *logHandler) clone() *logHandler {
	attrs := make([]flatAttr, len(h.attrs), len(h.attrs)+4)
	copy(attrs, h.attrs)
	return &logHandler{next: h.next, attrs: attrs, prefix: h.prefix}
}

// flattenAttr appends a to out under prefix, expanding group values to dotted
// keys and resolving slog.LogValuer values. Attributes slog treats as empty
// are skipped, and a group with an empty key is inlined, as slog specifies.
func flattenAttr(prefix string, a slog.Attr, out *[]flatAttr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		group := a.Value.Group()
		if len(group) == 0 {
			return
		}
		inner := prefix
		if a.Key != "" {
			inner = prefix + a.Key + "."
		}
		for _, g := range group {
			flattenAttr(inner, g, out)
		}
		return
	}
	if a.Key == "" {
		return
	}
	*out = append(*out, flatAttr{key: prefix + a.Key, value: logAttrValue(a.Value)})
}

// logAttrValue converts a resolved slog value into the representation a
// buffered line carries. Strings, bools, integers and floats are kept as they
// are; durations become their string form, timestamps RFC3339Nano, errors
// their message, and anything else its fmt %v rendering. Converting here
// rather than at export time keeps a buffered line free of references to
// values the application may still be mutating, and gives every wire format
// the same set of value kinds to map.
func logAttrValue(v slog.Value) any {
	switch v.Kind() {
	case slog.KindString:
		return v.String()
	case slog.KindBool:
		return v.Bool()
	case slog.KindInt64:
		return v.Int64()
	case slog.KindUint64:
		return v.Uint64()
	case slog.KindFloat64:
		return v.Float64()
	case slog.KindDuration:
		return v.Duration().String()
	case slog.KindTime:
		return v.Time().Format(time.RFC3339Nano)
	default:
		value := v.Any()
		if err, ok := value.(error); ok {
			return err.Error()
		}
		return fmt.Sprintf("%v", value)
	}
}

var (
	// logInstallMu guards the installed handler and the logger capture
	// replaced, so install and uninstall cannot race each other.
	logInstallMu sync.Mutex

	// installedLogHandler is the handler currently installed, nil when log
	// capture is off.
	installedLogHandler *logHandler

	// previousDefaultLogger is the slog default capture replaced, restored
	// when capture is uninstalled.
	previousDefaultLogger *slog.Logger

	// logCaptureOn mirrors installedLogHandler for lock-free reads on the
	// request path.
	logCaptureOn atomic.Bool
)

// logCaptureEnabled reports whether log capture is installed.
func logCaptureEnabled() bool { return logCaptureOn.Load() }

// LogHandler returns the installed log capture handler, or nil when log
// capture is off. Applications that build their own logger instead of using
// slog's default can wrap it:
//
//	if h := velwatch.LogHandler(); h != nil {
//	    logger = slog.New(h)
//	}
func LogHandler() slog.Handler {
	logInstallMu.Lock()
	defer logInstallMu.Unlock()
	if installedLogHandler == nil {
		return nil
	}
	return installedLogHandler
}

// installLogCapture wraps the current slog default handler with the capture
// handler and installs the result as the new default. The application's own
// logging keeps working: every record is forwarded to the previous handler.
// Installing twice is a no-op and returns the handler already installed.
func installLogCapture() *logHandler {
	logInstallMu.Lock()
	defer logInstallMu.Unlock()
	if installedLogHandler != nil {
		return installedLogHandler
	}

	previous := slog.Default()
	handler := newLogHandler(previous.Handler())
	installedLogHandler = handler
	previousDefaultLogger = previous
	logCaptureOn.Store(true)
	slog.SetDefault(slog.New(handler))
	return handler
}

// uninstallLogCapture restores the slog default that was in place before
// capture was installed. It is a no-op when capture is not installed.
func uninstallLogCapture() {
	logInstallMu.Lock()
	defer logInstallMu.Unlock()
	if installedLogHandler == nil {
		return
	}
	logCaptureOn.Store(false)
	installedLogHandler = nil
	if previousDefaultLogger != nil {
		slog.SetDefault(previousDefaultLogger)
		previousDefaultLogger = nil
	}
}
