package velwatch

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	vlog "github.com/velocitykode/velocity/log"
)

// LogDriverName is the name the SDK registers its velocity log driver under.
// An application selects it the way it selects any other channel, usually as
// one leaf of a stack:
//
//	LOG_DRIVER=stack
//	LOG_STACK=console,velwatch
//
// Every log line the application already writes through velocity's log
// package then reaches Velwatch as well as the console, with no change at
// any call site.
const LogDriverName = "velwatch"

// init registers the velwatch log driver with velocity's log driver registry.
// Registration happens at package import time, so the blank import that turns
// the SDK on is enough to make the channel name resolvable. The driver is
// registered whether or not the SDK is configured: an application can leave
// "velwatch" in its log stack permanently and switch shipping on and off with
// VELWATCH_LOG_CAPTURE, because a driver with no live SDK behind it silently
// drops every line.
func init() {
	vlog.Drivers().Register(LogDriverName, func(_ context.Context, cfg vlog.LogConfig) (vlog.Logger, error) {
		return vlog.WrapWithRedactors(logDriver{}, cfg), nil
	})
}

// logDriver is the velocity log.Logger the "velwatch" channel resolves to.
// It holds no state of its own: every call reads the live driver state, so a
// logger built during velocity.New() (before the SDK's boot hook has run)
// starts shipping the moment the SDK initializes, and stops the moment it
// shuts down.
type logDriver struct{}

var _ vlog.Logger = logDriver{}

func (logDriver) Debug(msg string, kvs ...any) { emitDriverLog(slog.LevelDebug, msg, kvs) }
func (logDriver) Info(msg string, kvs ...any)  { emitDriverLog(slog.LevelInfo, msg, kvs) }
func (logDriver) Warn(msg string, kvs ...any)  { emitDriverLog(slog.LevelWarn, msg, kvs) }
func (logDriver) Error(msg string, kvs ...any) { emitDriverLog(slog.LevelError, msg, kvs) }

// Fatal ships the line at error severity and returns. The driver never exits
// the process: it is one leaf of a stack, and deciding the process's fate is
// the application's business, not a telemetry sink's.
func (logDriver) Fatal(msg string, kvs ...any) { emitDriverLog(slog.LevelError, msg, kvs) }

// logDriverState is the snapshot the driver reads on every line. It is
// published once at initialization and replaced by nil at shutdown, so the
// hot path is a single atomic load with no lock and no allocation when the
// SDK is dormant.
type logDriverState struct {
	collector   *Collector
	serviceName string

	// floor is the lowest level shipped, from VELWATCH_LOG_LEVEL. A line
	// below it is dropped by this driver only: the other channels in the
	// stack still print it, so the floor is a Velwatch volume control and
	// never changes what the application logs.
	floor slog.Level

	// limiter caps how many lines per second this process ships.
	limiter *logRateLimiter
}

// activeLogDriver holds the live driver state, nil while the SDK is dormant,
// disabled, or running with log shipping off (VELWATCH_LOG_CAPTURE unset).
var activeLogDriver atomic.Pointer[logDriverState]

// logsDroppedRate counts lines the per-second rate cap refused.
var logsDroppedRate atomic.Uint64

// LogsDroppedRate returns how many log lines were dropped by the per-process
// rate cap (VELWATCH_LOG_MAX_PER_SECOND) since the process started. A growing
// number means the service logs faster than the cap allows and Velwatch is
// seeing a sampled view of its output.
func LogsDroppedRate() uint64 {
	return logsDroppedRate.Load()
}

// emitDriverLog turns one velocity log call into one traceless log event and
// hands it to the collector. Log events carry no trace, span or parent id:
// they are searchable by service, level, message and time, not attached to a
// request.
//
// It returns immediately when the SDK is dormant, disabled or shipping is off,
// never blocks (the collector flushes on a background goroutine), and never
// panics: a telemetry sink that takes the application down with it is worse
// than one that loses a line.
func emitDriverLog(level slog.Level, msg string, kvs []any) {
	defer func() { _ = recover() }()

	state := activeLogDriver.Load()
	if state == nil || state.collector == nil {
		return
	}
	if level < state.floor {
		return
	}

	now := time.Now()
	if !state.limiter.allow(now) {
		logsDroppedRate.Add(1)
		return
	}

	event := NewLogEvent(LogLine{
		Time:    now,
		Level:   level,
		Message: msg,
		Attrs:   flattenKVs(kvs),
	})
	event.setDefaultTag("service", state.serviceName)
	state.collector.Add(event)
}

// startLogDriver publishes the driver state for a freshly initialized SDK.
// config is the resolved configuration, so its rate cap and level are already
// defaulted. With LogCapture off nothing is published and the registered
// driver stays a no-op.
func startLogDriver(config Config, collector *Collector) {
	if !config.LogCapture {
		return
	}
	activeLogDriver.Store(&logDriverState{
		collector:   collector,
		serviceName: config.ServiceName,
		floor:       config.LogLevel,
		limiter:     newLogRateLimiter(config.LogMaxPerSecond),
	})
}

// stopLogDriver drops the driver state, so lines logged after shutdown are
// discarded rather than queued onto a collector whose exporter is closing.
func stopLogDriver() {
	activeLogDriver.Store(nil)
}

// logRateLimiter caps how many log lines this process ships per second. It is
// a fixed one-second window rather than a smoothed bucket: a burst inside one
// window is shipped up to the cap and the rest is counted, which is the
// behaviour an operator reading "max per second" expects.
type logRateLimiter struct {
	limit int

	mu          sync.Mutex
	windowStart time.Time
	count       int
}

// newLogRateLimiter returns a limiter for the given per-second cap. A cap of
// zero or less means no limit, which initialization never produces: it
// defaults a zero to defaultLogMaxPerSecond and rejects a negative.
func newLogRateLimiter(limit int) *logRateLimiter {
	return &logRateLimiter{limit: limit}
}

// allow reports whether a line at time now fits inside the current window,
// counting it when it does. A nil limiter or a non-positive cap allows
// everything.
func (r *logRateLimiter) allow(now time.Time) bool {
	if r == nil || r.limit <= 0 {
		return true
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if now.Sub(r.windowStart) >= time.Second {
		r.windowStart = now
		r.count = 0
	}
	if r.count >= r.limit {
		return false
	}
	r.count++
	return true
}

// flatAttr is one attribute already flattened to its dotted key.
type flatAttr struct {
	key   string
	value any
}

// badKVKey is the key a dangling final argument is stored under, the same
// marker slog uses for a key with no value.
const badKVKey = "!BADKEY"

// flattenKVs turns velocity's alternating key/value arguments into the
// attribute map a log event carries. Keys are dotted, so a group argument
// (slog.Group("db", slog.String("host", "primary"))) becomes "db.host".
//
// The degenerate shapes velocity's logger contract requires every driver to
// survive are handled here: a non-string key is rendered with %v, a dangling
// final argument is stored under "!BADKEY", and an empty key is skipped.
// Returns nil for no arguments, so an ordinary bare message allocates nothing.
func flattenKVs(kvs []any) map[string]any {
	if len(kvs) == 0 {
		return nil
	}

	attrs := make(map[string]any, (len(kvs)+1)/2)
	flat := make([]flatAttr, 0, 4)
	for i := 0; i < len(kvs); i++ {
		if attr, ok := kvs[i].(slog.Attr); ok {
			flat = appendFlattened(attrs, flat[:0], attr)
			continue
		}
		if i+1 >= len(kvs) {
			attrs[badKVKey] = logAttrValue(slog.AnyValue(kvs[i]).Resolve())
			break
		}
		key := logKVKey(kvs[i])
		i++
		flat = appendFlattened(attrs, flat[:0], slog.Any(key, kvs[i]))
	}
	return attrs
}

// appendFlattened flattens one attribute into attrs, reusing scratch as the
// working slice. It returns the (possibly grown) scratch slice.
func appendFlattened(attrs map[string]any, scratch []flatAttr, attr slog.Attr) []flatAttr {
	flattenAttr("", attr, &scratch)
	for _, a := range scratch {
		attrs[a.key] = a.value
	}
	return scratch
}

// logKVKey renders a key argument. A string is used as it is; anything else
// gets its %v form, so a mistyped key still names its value rather than
// taking the line down.
func logKVKey(key any) string {
	if s, ok := key.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", key)
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

// logAttrValue converts a resolved slog value into the representation a log
// line carries. Strings, bools, integers and floats are kept as they are;
// durations become their string form, timestamps RFC3339Nano, errors their
// message, and anything else its fmt %v rendering. Converting here rather
// than at export time keeps a queued line free of references to values the
// application may still be mutating, and gives every wire format the same set
// of value kinds to map.
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

// logLevelNames maps the level names VELWATCH_LOG_LEVEL accepts onto slog
// levels. Only the four slog defines are accepted: the floor is a coarse
// volume control, and inventing custom levels here would let a typo like
// "warning" silently land between two real ones.
var logLevelNames = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

// parseLogLevel resolves a VELWATCH_LOG_LEVEL value to the floor it names,
// case-insensitively. An unknown name is an error rather than a silent
// fallback: it decides which lines the service ships at all.
func parseLogLevel(value string) (slog.Level, error) {
	level, ok := logLevelNames[strings.ToLower(strings.TrimSpace(value))]
	if !ok {
		return 0, fmt.Errorf("velwatch: VELWATCH_LOG_LEVEL %q is not a known level "+
			"(want one of \"debug\", \"info\", \"warn\", \"error\")", value)
	}
	return level, nil
}
