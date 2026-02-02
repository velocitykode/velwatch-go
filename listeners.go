package velwatch

import (
	"context"
	"math/rand"
	"time"

	"github.com/velocitykode/velocity/pkg/cache"
	"github.com/velocitykode/velocity/pkg/events"
	"github.com/velocitykode/velocity/pkg/orm"
	"github.com/velocitykode/velocity/pkg/queue"
	"github.com/velocitykode/velocity/pkg/router"
)

// Listeners manages event listeners for Velocity framework events
type Listeners struct {
	collector   *Collector
	serviceName string
	sampleRate  float64

	// Registered event names for cleanup
	eventNames []string
}

// NewListeners creates a new listeners manager
func NewListeners(collector *Collector, serviceName string, sampleRate float64) *Listeners {
	return &Listeners{
		collector:   collector,
		serviceName: serviceName,
		sampleRate:  sampleRate,
		eventNames:  make([]string, 0),
	}
}

// Register registers all Velocity event listeners
func (l *Listeners) Register() {
	// HTTP request events
	l.registerTypedListener("request.handled", events.OnEvent(l.onRequestHandled))
	l.registerTypedListener("request.failed", events.OnEvent(l.onRequestFailed))

	// Database query events
	l.registerTypedListener("query.executed", events.OnEvent(l.onQueryExecuted))

	// Cache events
	l.registerTypedListener("cache.hit", events.OnEvent(l.onCacheHit))
	l.registerTypedListener("cache.miss", events.OnEvent(l.onCacheMiss))
	l.registerTypedListener("cache.written", events.OnEvent(l.onCacheWritten))

	// Queue job events
	l.registerTypedListener("job.queued", events.OnEvent(l.onJobQueued))
	l.registerTypedListener("job.processing", events.OnEvent(l.onJobProcessing))
	l.registerTypedListener("job.processed", events.OnEvent(l.onJobProcessed))
	l.registerTypedListener("job.failed", events.OnEvent(l.onJobFailed))
}

// Unregister removes all registered event listeners
func (l *Listeners) Unregister() {
	for _, name := range l.eventNames {
		events.Forget(name)
	}
	l.eventNames = nil
}

func (l *Listeners) registerTypedListener(eventName string, handler func(event interface{}) error) {
	events.On(eventName, handler)
	l.eventNames = append(l.eventNames, eventName)
}

// shouldSample returns true if this request should be sampled
func (l *Listeners) shouldSample() bool {
	if l.sampleRate >= 1.0 {
		return true
	}
	return rand.Float64() < l.sampleRate
}

// HTTP Request Handlers

func (l *Listeners) onRequestHandled(e *router.RequestHandled) error {
	if !l.shouldSample() {
		return nil
	}

	event := NewRequestEvent(
		e.Method,
		e.Path,
		e.StatusCode,
		float64(e.Duration.Milliseconds()),
	)
	// Extract trace context from the request context
	event.TraceID = GetTraceID(e.Context)
	event.SpanID = GetSpanID(e.Context)
	if event.TraceID == "" {
		event.TraceID = GenerateTraceID()
	}
	if event.SpanID == "" {
		event.SpanID = GenerateSpanID()
	}
	event.Tags["service"] = l.serviceName
	event.Tags["route"] = e.Route
	event.Attributes["bytes_written"] = e.BytesWritten
	event.Attributes["request_id"] = e.RequestID

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onRequestFailed(e *router.RequestFailed) error {
	if !l.shouldSample() {
		return nil
	}

	// Extract trace context from the request context
	traceID := GetTraceID(e.Context)
	spanID := GetSpanID(e.Context)
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if spanID == "" {
		spanID = GenerateSpanID()
	}

	// Record request event with error
	event := NewRequestEvent(e.Method, e.Path, 500, 0)
	event.TraceID = traceID
	event.SpanID = spanID
	event.Tags["service"] = l.serviceName

	if e.Error != nil {
		event.Attributes["error"] = e.Error.Error()
	}
	if e.Recovered {
		event.Attributes["recovered"] = true
	}

	l.collector.Add(event)

	// Also record an exception event
	if e.Error != nil {
		exEvent := NewExceptionEvent(
			"RequestError",
			e.Error.Error(),
			e.Stack,
		)
		exEvent.TraceID = traceID
		exEvent.SpanID = spanID
		exEvent.Tags["service"] = l.serviceName
		l.collector.Add(exEvent)
	}

	return nil
}

// Database Query Handlers

func (l *Listeners) onQueryExecuted(e *orm.QueryExecuted) error {
	if !l.shouldSample() {
		return nil
	}

	// Extract trace context from the query context
	traceID := GetTraceID(e.Context)
	spanID := GetSpanID(e.Context)
	parentID := GetParentID(e.Context)

	// Only record queries that are part of a trace
	if traceID == "" {
		return nil
	}

	event := NewQueryEvent(
		e.SQL,
		float64(e.Duration.Milliseconds()),
		e.RowsAffected,
	)
	event.TraceID = traceID
	event.SpanID = spanID
	if parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["connection"] = e.Connection
	event.Attributes["file"] = e.File
	event.Attributes["line"] = e.Line

	l.collector.Add(event)
	return nil
}

// Cache Handlers

func (l *Listeners) onCacheHit(e *cache.CacheHit) error {
	if !l.shouldSample() {
		return nil
	}

	// Extract trace context from the cache context
	traceID := GetTraceID(e.Context)
	spanID := GetSpanID(e.Context)
	parentID := GetParentID(e.Context)

	if traceID == "" {
		return nil
	}

	event := NewCacheEvent("get", e.Key, true, 0)
	event.TraceID = traceID
	event.SpanID = spanID
	if parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["store"] = e.Store

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onCacheMiss(e *cache.CacheMiss) error {
	if !l.shouldSample() {
		return nil
	}

	// Extract trace context from the cache context
	traceID := GetTraceID(e.Context)
	spanID := GetSpanID(e.Context)
	parentID := GetParentID(e.Context)

	if traceID == "" {
		return nil
	}

	event := NewCacheEvent("get", e.Key, false, 0)
	event.TraceID = traceID
	event.SpanID = spanID
	if parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["store"] = e.Store

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onCacheWritten(e *cache.CacheWritten) error {
	if !l.shouldSample() {
		return nil
	}

	// Extract trace context from the cache context
	traceID := GetTraceID(e.Context)
	spanID := GetSpanID(e.Context)
	parentID := GetParentID(e.Context)

	if traceID == "" {
		return nil
	}

	event := NewCacheEvent("set", e.Key, false, 0)
	event.TraceID = traceID
	event.SpanID = spanID
	if parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["store"] = e.Store
	event.Attributes["ttl_seconds"] = e.TTL.Seconds()

	l.collector.Add(event)
	return nil
}

// Queue Job Handlers

func (l *Listeners) onJobQueued(e *queue.JobQueued) error {
	if !l.shouldSample() {
		return nil
	}

	// Extract trace context
	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

	// Job queued events may not have trace context if queued outside a request
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if spanID == "" {
		spanID = GenerateSpanID()
	}

	event := NewJobEvent(e.JobType, e.Queue, "queued", 0)
	event.TraceID = traceID
	event.SpanID = spanID
	if parentID != "" {
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
	if !l.shouldSample() {
		return nil
	}

	// Extract trace context
	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if spanID == "" {
		spanID = GenerateSpanID()
	}

	event := NewJobEvent(e.JobType, e.Queue, "processed", float64(e.DurationMs))
	event.TraceID = traceID
	event.SpanID = spanID
	if parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onJobFailed(e *queue.JobFailed) error {
	if !l.shouldSample() {
		return nil
	}

	// Extract trace context
	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if spanID == "" {
		spanID = GenerateSpanID()
	}

	event := NewJobEvent(e.JobType, e.Queue, "failed", float64(e.DurationMs))
	event.TraceID = traceID
	event.SpanID = spanID
	if parentID != "" {
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
	spanID := GetSpanID(ctx)

	if traceID != "" {
		event.TraceID = traceID
	} else {
		event.TraceID = GenerateTraceID()
	}
	if spanID != "" {
		event.SpanID = spanID
	}
	event.Tags["service"] = sdk.config.ServiceName

	sdk.collector.Add(event)
}

// init seeds the random number generator
func init() {
	rand.Seed(time.Now().UnixNano())
}
