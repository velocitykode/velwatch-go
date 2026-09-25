package velwatch

import (
	"context"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/velocitykode/velocity/cache"
	"github.com/velocitykode/velocity/contract"
	"github.com/velocitykode/velocity/httpclient"
	"github.com/velocitykode/velocity/mail"
	"github.com/velocitykode/velocity/orm"
	"github.com/velocitykode/velocity/queue"
	"github.com/velocitykode/velocity/router"
	"github.com/velocitykode/velocity/scheduler"
)

// listenerFunc adapts a plain handler function to contract.EventListener.
type listenerFunc func(event interface{}) error

func (f listenerFunc) Handle(_ context.Context, event interface{}) error {
	return f(event)
}

func (f listenerFunc) ShouldQueue() bool { return false }

// Listeners manages event listeners for Velocity framework events
type Listeners struct {
	collector   *Collector
	dispatcher  contract.Dispatcher
	serviceName string
	sampleRate  float64

	// Registered listener IDs for cleanup
	listenerIDs []int
}

// NewListeners creates a new listeners manager bound to the app's event dispatcher
func NewListeners(collector *Collector, dispatcher contract.Dispatcher, serviceName string, sampleRate float64) *Listeners {
	return &Listeners{
		collector:   collector,
		dispatcher:  dispatcher,
		serviceName: serviceName,
		sampleRate:  sampleRate,
		listenerIDs: make([]int, 0),
	}
}

// Register registers all Velocity event listeners
func (l *Listeners) Register() {
	// HTTP request events
	l.registerRawListener("request.handled", func(e interface{}) error {
		if r, ok := e.(*router.RequestHandled); ok {
			return l.onRequestHandled(r)
		}
		return nil
	})
	l.registerRawListener("request.failed", func(e interface{}) error {
		if r, ok := e.(*router.RequestFailed); ok {
			return l.onRequestFailed(r)
		}
		return nil
	})

	// Database query events
	l.registerRawListener("query.executed", func(e interface{}) error {
		if q, ok := e.(*orm.QueryExecuted); ok {
			return l.onQueryExecuted(q)
		}
		return nil
	})
	l.registerRawListener("query.failed", func(e interface{}) error {
		if q, ok := e.(*orm.QueryFailed); ok {
			return l.onQueryFailed(q)
		}
		return nil
	})

	// Cache events - use raw listeners due to OnEvent wrapper issue
	l.registerRawListener("cache.hit", func(e interface{}) error {
		if c, ok := e.(*cache.CacheHit); ok {
			return l.onCacheHit(c)
		}
		return nil
	})
	l.registerRawListener("cache.miss", func(e interface{}) error {
		if c, ok := e.(*cache.CacheMiss); ok {
			return l.onCacheMiss(c)
		}
		return nil
	})
	l.registerRawListener("cache.written", func(e interface{}) error {
		if c, ok := e.(*cache.CacheWritten); ok {
			return l.onCacheWritten(c)
		}
		return nil
	})
	l.registerRawListener("cache.forgotten", func(e interface{}) error {
		if c, ok := e.(*cache.CacheForgotten); ok {
			return l.onCacheForgotten(c)
		}
		return nil
	})
	l.registerRawListener("cache.operation.failed", func(e interface{}) error {
		if c, ok := e.(*cache.CacheOperationFailed); ok {
			return l.onCacheOperationFailed(c)
		}
		return nil
	})

	// Queue job events
	l.registerRawListener("job.queued", func(e interface{}) error {
		if j, ok := e.(*queue.JobQueued); ok {
			return l.onJobQueued(j)
		}
		return nil
	})
	l.registerRawListener("job.processing", func(e interface{}) error {
		if j, ok := e.(*queue.JobProcessing); ok {
			return l.onJobProcessing(j)
		}
		return nil
	})
	l.registerRawListener("job.processed", func(e interface{}) error {
		if j, ok := e.(*queue.JobProcessed); ok {
			return l.onJobProcessed(j)
		}
		return nil
	})
	l.registerRawListener("job.failed", func(e interface{}) error {
		if j, ok := e.(*queue.JobFailed); ok {
			return l.onJobFailed(j)
		}
		return nil
	})

	// HTTP client events
	l.registerRawListener("http.request.sent", func(e interface{}) error {
		if r, ok := e.(*httpclient.RequestSent); ok {
			return l.onHTTPRequestSent(r)
		}
		return nil
	})
	l.registerRawListener("http.request.failed", func(e interface{}) error {
		if r, ok := e.(*httpclient.RequestFailed); ok {
			return l.onHTTPRequestFailed(r)
		}
		return nil
	})

	// Mail events
	l.registerRawListener("mail.sent", func(e interface{}) error {
		if m, ok := e.(*mail.MailSent); ok {
			return l.onMailSent(m)
		}
		return nil
	})
	l.registerRawListener("mail.failed", func(e interface{}) error {
		if m, ok := e.(*mail.MailFailed); ok {
			return l.onMailFailed(m)
		}
		return nil
	})

	// Scheduler events
	l.registerRawListener("scheduled.starting", func(e interface{}) error {
		if s, ok := e.(*scheduler.ScheduledTaskStarting); ok {
			return l.onScheduledTaskStarting(s)
		}
		return nil
	})
	l.registerRawListener("scheduled.finished", func(e interface{}) error {
		if s, ok := e.(*scheduler.ScheduledTaskFinished); ok {
			return l.onScheduledTaskFinished(s)
		}
		return nil
	})
	l.registerRawListener("scheduled.failed", func(e interface{}) error {
		if s, ok := e.(*scheduler.ScheduledTaskFailed); ok {
			return l.onScheduledTaskFailed(s)
		}
		return nil
	})
}

// Unregister removes all registered event listeners
func (l *Listeners) Unregister() {
	for _, id := range l.listenerIDs {
		l.dispatcher.Off(id)
	}
	l.listenerIDs = nil
}

// registerRawListener registers an event listener on the app's dispatcher
func (l *Listeners) registerRawListener(eventName string, handler func(event interface{}) error) {
	id := l.dispatcher.Listen(eventName, listenerFunc(handler))
	l.listenerIDs = append(l.listenerIDs, id)
}

// sampleTrace makes a single, deterministic, all-or-nothing sampling decision
// for an entire trace. Every event that carries the same trace_id hashes to the
// same value, so a trace is either fully kept or fully dropped - no more
// partial traces where a request is kept but half its queries are dropped (or
// vice versa). This replaces the old per-event rand.Float64() coin flip.
//
// Events with no trace context (traceID == "") cannot be correlated to a trace,
// so they fall back to an independent per-event random decision at the same
// rate; there is no shared trace to be consistent with.
func (l *Listeners) sampleTrace(traceID string) bool {
	if l.sampleRate >= 1.0 {
		return true
	}
	if l.sampleRate <= 0 {
		return false
	}
	if traceID == "" {
		return rand.Float64() < l.sampleRate
	}
	// fnv-1a over the trace ID yields a stable 64-bit value; the top 53 bits
	// map to a uniform float in [0,1). fnv (a non-crypto hash) is the right
	// tool here - sampling wants speed and determinism, not cryptographic
	// strength - and velocity exposes no sampling/hashing primitive to prefer.
	h := fnv.New64a()
	_, _ = h.Write([]byte(traceID))
	frac := float64(h.Sum64()>>11) / float64(uint64(1)<<53)
	return frac < l.sampleRate
}

// childParent returns the parent span ID for a child span. It prefers the
// framework-provided parent (e.g. a statement's enclosing transaction span,
// which velocity sets on QueryExecuted.ParentID inside Manager.Transaction),
// and otherwise falls back to the enclosing context span (the request or
// operation span). The child event keeps the unique SpanID minted by NewEvent.
func childParent(ctxSpanID, frameworkParentID string) string {
	if frameworkParentID != "" {
		return frameworkParentID
	}
	return ctxSpanID
}

// msFromDuration converts a time.Duration to fractional milliseconds without
// flooring, so sub-millisecond operations (a 750µs query) are not reported as
// 0. Duration.Milliseconds() truncates to whole ms and must not be used for
// span durations.
func msFromDuration(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

// HTTP Request Handlers

func (l *Listeners) onRequestHandled(e *router.RequestHandled) error {
	// The request is the ROOT span of its trace: it keeps the context span as
	// its own SpanID so child events (queries, cache, ...) can parent onto it.
	// Its ParentID stays whatever the framework provides (nil at the top of a
	// trace, or the inbound span for a propagated distributed trace).
	traceID := e.TraceID
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if !l.sampleTrace(traceID) {
		return nil
	}

	event := NewRequestEvent(
		e.Method,
		e.Path,
		e.StatusCode,
		msFromDuration(e.Duration),
	)
	event.TraceID = traceID
	if e.SpanID != "" {
		event.SpanID = e.SpanID
	}
	if e.ParentID != "" {
		parentID := e.ParentID
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Tags["route"] = e.Route
	event.Attributes["bytes_written"] = e.BytesWritten
	event.Attributes["request_id"] = e.RequestID

	l.collector.Add(event)
	return nil
}

// onRequestFailed records the error detail for a failed request as an
// exception event only. The request record itself comes from the
// request.handled event, which the router fires for every request
// (including failed ones) with the real status code and duration; emitting
// a request event here too would double-count failed requests.
//
// The exception is its own CHILD span under the request span (it keeps the
// unique SpanID from NewEvent and parents onto the request's context span).
// It must NOT reuse the request's span ID: two rows sharing a span ID is
// invalid OTLP and collapses the two nodes. Sharing the trace ID keeps the
// exception correlated with its request.
func (l *Listeners) onRequestFailed(e *router.RequestFailed) error {
	if e.Error == nil {
		return nil
	}

	traceID := e.TraceID
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if !l.sampleTrace(traceID) {
		return nil
	}

	exEvent := NewExceptionEvent(
		"RequestError",
		e.Error.Error(),
		e.Stack,
	)
	exEvent.TraceID = traceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		exEvent.ParentID = &parentID
	}
	exEvent.Tags["service"] = l.serviceName
	exEvent.Attributes["method"] = e.Method
	exEvent.Attributes["path"] = e.Path
	exEvent.Attributes["request_id"] = e.RequestID
	// Always emit recovered as a real bool so consumers can distinguish
	// false (unhandled) from missing.
	exEvent.Attributes["recovered"] = e.Recovered
	l.collector.Add(exEvent)

	return nil
}

// Database Query Handlers

func (l *Listeners) onQueryExecuted(e *orm.QueryExecuted) error {
	// A query is a CHILD span: it keeps the unique SpanID from NewEvent and
	// parents onto the enclosing span. Velocity sets e.SpanID to the enclosing
	// context span and e.ParentID to the transaction span when the query ran
	// inside Manager.Transaction; childParent prefers that tx span, else the
	// context span (the request span).
	traceID := e.TraceID
	if traceID == "" {
		// Query outside any request/trace context (e.g. a model method that
		// did not thread ctx). Mint a standalone trace for it.
		traceID = GenerateTraceID()
	}
	if !l.sampleTrace(traceID) {
		return nil
	}

	event := NewQueryEvent(
		e.SQL,
		msFromDuration(e.Duration),
		e.RowsAffected,
	)
	event.TraceID = traceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	if e.TraceID == "" {
		event.Tags["orphan"] = "true" // no enclosing trace context
	}
	event.Attributes["connection"] = e.Connection
	event.Attributes["file"] = e.File
	event.Attributes["line"] = e.Line

	l.collector.Add(event)
	return nil
}

// onQueryFailed records a failed database query. Without this, failed queries
// are completely invisible in the product. It is emitted as a query event (not
// an exception) so failed queries stay in the query record type alongside
// successful ones - visible in query listings and counts - while carrying the
// error and a failed=true marker. The framework provides no duration for a
// failed query, so duration_ms is 0.
func (l *Listeners) onQueryFailed(e *orm.QueryFailed) error {
	traceID := e.TraceID
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if !l.sampleTrace(traceID) {
		return nil
	}

	event := NewQueryEvent(e.Query, 0, 0)
	event.TraceID = traceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	if e.TraceID == "" {
		event.Tags["orphan"] = "true"
	}
	event.Attributes["connection"] = e.Connection
	event.Attributes["failed"] = true
	event.Attributes["error"] = e.Error

	l.collector.Add(event)
	return nil
}

// Cache Handlers

func (l *Listeners) onCacheHit(e *cache.CacheHit) error {
	if e.TraceID == "" {
		return nil // only record cache ops within a trace
	}
	if !l.sampleTrace(e.TraceID) {
		return nil
	}

	event := NewCacheEvent("get", e.Key, true, 0)
	event.TraceID = e.TraceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["store"] = e.Store

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onCacheMiss(e *cache.CacheMiss) error {
	if e.TraceID == "" {
		return nil
	}
	if !l.sampleTrace(e.TraceID) {
		return nil
	}

	event := NewCacheEvent("get", e.Key, false, 0)
	event.TraceID = e.TraceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["store"] = e.Store

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onCacheWritten(e *cache.CacheWritten) error {
	if e.TraceID == "" {
		return nil
	}
	if !l.sampleTrace(e.TraceID) {
		return nil
	}

	event := NewCacheEvent("set", e.Key, false, 0)
	event.TraceID = e.TraceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["store"] = e.Store
	event.Attributes["ttl_seconds"] = e.TTL.Seconds()

	l.collector.Add(event)
	return nil
}

// onCacheForgotten records a cache delete. The dashboard's cache "deletes"
// series keys on operation = 'delete', which was permanently zero until this
// listener existed.
func (l *Listeners) onCacheForgotten(e *cache.CacheForgotten) error {
	if e.TraceID == "" {
		return nil
	}
	if !l.sampleTrace(e.TraceID) {
		return nil
	}

	event := NewCacheEvent("delete", e.Key, false, 0)
	event.TraceID = e.TraceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["store"] = e.Store

	l.collector.Add(event)
	return nil
}

// onCacheOperationFailed records a failed cache operation, carrying the
// framework operation verb (put/forget/flush/...) and the error so cache
// errors are visible instead of silently swallowed.
func (l *Listeners) onCacheOperationFailed(e *cache.CacheOperationFailed) error {
	if e.TraceID == "" {
		return nil
	}
	if !l.sampleTrace(e.TraceID) {
		return nil
	}

	event := NewCacheEvent(e.Op, e.Key, false, 0)
	event.TraceID = e.TraceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["store"] = e.Store
	event.Attributes["failed"] = true
	event.Attributes["error"] = e.Error

	l.collector.Add(event)
	return nil
}

// Queue Job Handlers

func (l *Listeners) onJobQueued(e *queue.JobQueued) error {
	// Job queued events may not have trace context if queued outside a request.
	traceID := e.TraceID
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if !l.sampleTrace(traceID) {
		return nil
	}

	event := NewJobEvent(e.JobType, e.Queue, "queued", 0)
	event.TraceID = traceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["delayed"] = e.Delayed
	if e.Delayed {
		event.Attributes["delay_ms"] = e.DelayMs
	}

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onJobProcessing(e *queue.JobProcessing) error {
	// We don't record job.processing as a separate event
	// The job.processed or job.failed event will capture the full duration
	return nil
}

func (l *Listeners) onJobProcessed(e *queue.JobProcessed) error {
	traceID := e.TraceID
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if !l.sampleTrace(traceID) {
		return nil
	}

	// DurationMs is a framework-floored int64 (whole ms); sub-ms precision is
	// already lost upstream and cannot be recovered here.
	event := NewJobEvent(e.JobType, e.Queue, "processed", float64(e.DurationMs))
	event.TraceID = traceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onJobFailed(e *queue.JobFailed) error {
	traceID := e.TraceID
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if !l.sampleTrace(traceID) {
		return nil
	}

	event := NewJobEvent(e.JobType, e.Queue, "failed", float64(e.DurationMs))
	event.TraceID = traceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["error"] = e.Error

	l.collector.Add(event)
	return nil
}

// HTTP Client Handlers

func (l *Listeners) onHTTPRequestSent(e *httpclient.RequestSent) error {
	if e.TraceID == "" {
		return nil // Only record requests within a trace
	}
	if !l.sampleTrace(e.TraceID) {
		return nil
	}

	event := NewOutgoingRequestEvent(e.Method, e.URL, e.StatusCode, float64(e.DurationMs))
	event.TraceID = e.TraceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["request_size"] = e.RequestSize
	event.Attributes["response_size"] = e.ResponseSize

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onHTTPRequestFailed(e *httpclient.RequestFailed) error {
	if e.TraceID == "" {
		return nil // Only record requests within a trace
	}
	if !l.sampleTrace(e.TraceID) {
		return nil
	}

	event := NewOutgoingRequestEvent(e.Method, e.URL, 0, float64(e.DurationMs))
	event.TraceID = e.TraceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["error"] = e.Error

	l.collector.Add(event)
	return nil
}

// Mail Handlers

func (l *Listeners) onMailSent(e *mail.MailSent) error {
	traceID := e.TraceID
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if !l.sampleTrace(traceID) {
		return nil
	}

	event := NewMailEvent(e.Subject, len(e.To), e.Channel, "sent", float64(e.DurationMs))
	event.TraceID = traceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onMailFailed(e *mail.MailFailed) error {
	traceID := e.TraceID
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if !l.sampleTrace(traceID) {
		return nil
	}

	event := NewMailEvent(e.Subject, len(e.To), e.Channel, "failed", float64(e.DurationMs))
	event.TraceID = traceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["error"] = e.Error

	l.collector.Add(event)
	return nil
}

// Scheduler Handlers

func (l *Listeners) onScheduledTaskStarting(e *scheduler.ScheduledTaskStarting) error {
	// We don't record starting events - wait for finished/failed
	return nil
}

func (l *Listeners) onScheduledTaskFinished(e *scheduler.ScheduledTaskFinished) error {
	traceID := e.TraceID
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if !l.sampleTrace(traceID) {
		return nil
	}

	event := NewScheduledTaskEvent(e.TaskName, "finished", float64(e.DurationMs))
	event.TraceID = traceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onScheduledTaskFailed(e *scheduler.ScheduledTaskFailed) error {
	traceID := e.TraceID
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if !l.sampleTrace(traceID) {
		return nil
	}

	event := NewScheduledTaskEvent(e.TaskName, "failed", float64(e.DurationMs))
	event.TraceID = traceID
	if parentID := childParent(e.SpanID, e.ParentID); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["error"] = e.Error

	l.collector.Add(event)
	return nil
}

// RecordException manually records an exception event
func RecordException(ctx context.Context, errType, message, stackTrace string) {
	mu.Lock()
	sdk := instance
	mu.Unlock()

	if sdk == nil || sdk.config.Disabled || sdk.collector == nil {
		return
	}

	event := NewExceptionEvent(errType, message, stackTrace)

	traceID := GetTraceID(ctx)
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	event.TraceID = traceID
	// The exception is its own child span under the enclosing operation span;
	// it keeps the unique SpanID from NewEvent and parents onto the context
	// span (or the framework parent when set).
	if parentID := childParent(GetSpanID(ctx), GetParentID(ctx)); parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = sdk.config.ServiceName

	sdk.collector.Add(event)
}
