package velwatch

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// spanKey identifies a span's log buffer in the registry.
type spanKey struct {
	traceID string
	spanID  string
}

// registryEntry is one registered buffer plus the time it was opened, so a
// span whose end is never observed can be swept.
type registryEntry struct {
	logs   *SpanLogs
	opened time.Time
}

// spanLogRegistry holds the log buffers of spans that carry no buffer on
// their context: velocity requests, jobs and scheduled tasks, where the
// framework owns the context and the SDK cannot add a value to it. The
// capture handler opens a buffer here the first time it sees a line under a
// trace id with no context-bound buffer, and the SDK's framework event
// listeners close it when the span ends, with the real outcome.
//
// A context-bound buffer (Middleware, or an explicit StartSpanLogs) always
// wins over the registry, so a plain net/http application and a velocity
// application can share one process without a line landing twice.
//
// The registry is bounded two ways. maxSpans caps how many buffers are open
// at once; past it a new span's lines are refused and counted on
// LogsDroppedSpanLimit. ttl caps how long a buffer waits for its end event;
// past it the buffer is swept and recorded with no outcome, so a span the
// SDK never sees end (a line logged after request.handled, a job longer
// than the ttl, a hand-rolled trace with no matching event) still ships
// what the keep rules allow and never leaks.
type spanLogRegistry struct {
	maxSpans int
	ttl      time.Duration

	mu      sync.Mutex
	entries map[spanKey]*registryEntry
}

const (
	// registryMaxSpans is the most spans the registry will hold buffers for
	// at once. Each buffer is itself capped by LogMaxLines, so this bounds
	// the memory log capture can hold for spans the SDK did not open.
	registryMaxSpans = 4096

	// registryTTL is how long a registered buffer waits for its span's end
	// event before being swept and recorded without an outcome.
	registryTTL = 10 * time.Minute
)

// activeSpanLogs is the process-wide registry the capture handler and the
// framework event listeners share.
var activeSpanLogs = newSpanLogRegistry(registryMaxSpans, registryTTL)

// logsDroppedSpanLimit counts log records refused because the registry was
// already holding buffers for maxSpans spans.
var logsDroppedSpanLimit atomic.Uint64

// LogsDroppedSpanLimit returns how many log records were dropped because too
// many framework spans had open log buffers at once since the process
// started. A non-zero value means the service had more in-flight requests,
// jobs and tasks logging under trace ids than the registry holds.
func LogsDroppedSpanLimit() uint64 {
	return logsDroppedSpanLimit.Load()
}

func newSpanLogRegistry(maxSpans int, ttl time.Duration) *spanLogRegistry {
	return &spanLogRegistry{
		maxSpans: maxSpans,
		ttl:      ttl,
		entries:  make(map[spanKey]*registryEntry),
	}
}

// attach returns the buffer registered for the span on ctx, opening one when
// the span has none yet. It returns nil when ctx carries no trace context or
// when the registry is full.
func (r *spanLogRegistry) attach(ctx context.Context) *SpanLogs {
	key := spanKey{traceID: GetTraceID(ctx), spanID: GetSpanID(ctx)}
	if key.traceID == "" && key.spanID == "" {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.entries[key]; ok {
		return entry.logs
	}
	if r.maxSpans > 0 && len(r.entries) >= r.maxSpans {
		logsDroppedSpanLimit.Add(1)
		return nil
	}
	logs := &SpanLogs{
		traceID:  key.traceID,
		spanID:   key.spanID,
		parentID: GetParentID(ctx),
		maxLines: int(logMaxLines.Load()),
	}
	r.entries[key] = &registryEntry{logs: logs, opened: time.Now()}
	return logs
}

// lookup returns the buffer registered for the span, or nil.
func (r *spanLogRegistry) lookup(traceID, spanID string) *SpanLogs {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.entries[spanKey{traceID: traceID, spanID: spanID}]; ok {
		return entry.logs
	}
	return nil
}

// detach removes and returns the buffer registered for the span, or nil when
// the span logged nothing. The caller owns the buffer from here: it sets the
// outcome and records it.
func (r *spanLogRegistry) detach(traceID, spanID string) *SpanLogs {
	key := spanKey{traceID: traceID, spanID: spanID}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[key]
	if !ok {
		return nil
	}
	delete(r.entries, key)
	return entry.logs
}

// sweep removes and returns every buffer opened more than ttl before now.
func (r *spanLogRegistry) sweep(now time.Time) []*SpanLogs {
	r.mu.Lock()
	defer r.mu.Unlock()
	var stale []*SpanLogs
	for key, entry := range r.entries {
		if now.Sub(entry.opened) > r.ttl {
			stale = append(stale, entry.logs)
			delete(r.entries, key)
		}
	}
	return stale
}

// len returns how many buffers are registered.
func (r *spanLogRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// reset drops every registered buffer without recording it. Called when log
// capture is uninstalled, so a buffer opened under one SDK instance cannot
// surface under the next.
func (r *spanLogRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = make(map[spanKey]*registryEntry)
}

// recordStaleSpanLogs records and forgets every registered buffer whose span
// end was never observed within the registry ttl. The flush loop calls it on
// every tick.
func recordStaleSpanLogs(now time.Time) {
	for _, logs := range activeSpanLogs.sweep(now) {
		RecordSpanLogs(logs)
	}
}

// spanLogsForContext returns the buffer the span on ctx is logging into: the
// context-bound one when there is one, else the registered one, else nil. It
// is the lookup for code that learns something about a span mid-flight
// (RecordException marking it failed) rather than closing it.
func spanLogsForContext(ctx context.Context) *SpanLogs {
	if ctx == nil {
		return nil
	}
	if logs := SpanLogsFrom(ctx); logs != nil {
		return logs
	}
	return activeSpanLogs.lookup(GetTraceID(ctx), GetSpanID(ctx))
}

// endFrameworkSpanLogs closes the registered log buffer for a span the
// framework has just reported as ended, applying the keep rules with the
// outcome the event carried. It returns the buffer so the caller can report
// what capture refused on the span's own record, or nil when the span
// logged nothing into the registry.
//
// A context-bound buffer is deliberately left alone: whoever opened it
// (Middleware, or the code that called StartSpanLogs) records it, so a
// velocity app wrapped in Middleware as well does not ship its lines twice.
func endFrameworkSpanLogs(traceID, spanID string, outcome SpanOutcome) *SpanLogs {
	logs := activeSpanLogs.detach(traceID, spanID)
	if logs == nil {
		return nil
	}
	logs.SetOutcome(outcome)
	RecordSpanLogs(logs)
	return logs
}
