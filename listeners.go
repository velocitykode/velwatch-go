package velwatch

import (
	"context"
	"math/rand"

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

func (f listenerFunc) Async() bool { return false }

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

// onRequestFailed records the error detail for a failed request as an
// exception event only. The request record itself comes from the
// request.handled event, which the router fires for every request
// (including failed ones) with the real status code and duration; emitting
// a request event here too would double-count failed requests. The two
// events share RequestID/TraceID/SpanID, so the exception stays correlated
// with its request record.
func (l *Listeners) onRequestFailed(e *router.RequestFailed) error {
	if e.Error == nil {
		return nil
	}

	// A failed request keeps every log line it wrote, whether or not this
	// exception event itself is sampled: the keep rules read the outcome when
	// the span ends, and the middleware only sees the status code, which a
	// recovered panic may well have rendered as a tidy 500 page.
	SpanLogsFrom(e.Context).MarkFailed()

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

	exEvent := NewExceptionEvent(
		"RequestError",
		e.Error.Error(),
		e.Stack,
	)
	exEvent.TraceID = traceID
	exEvent.SpanID = spanID
	exEvent.Tags["service"] = l.serviceName
	exEvent.Attributes["method"] = e.Method
	exEvent.Attributes["path"] = e.Path
	exEvent.Attributes["request_id"] = e.RequestID
	if e.Recovered {
		exEvent.Attributes["recovered"] = true
	}
	l.collector.Add(exEvent)

	return nil
}

// Database Query Handlers

func (l *Listeners) onQueryExecuted(e *orm.QueryExecuted) error {
	if !l.shouldSample() {
		return nil
	}

	// Extract trace context from the event (populated by Velocity)
	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

	// Generate trace IDs if not present (for queries outside request context)
	// This captures queries from model methods that don't pass context
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if spanID == "" {
		spanID = GenerateSpanID()
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
	event.Tags["orphan"] = "true" // Mark as orphan if no parent trace
	if e.TraceID != "" {
		delete(event.Tags, "orphan")
	}
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

	// Extract trace context from the event (populated by Velocity)
	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

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

	// Extract trace context from the event (populated by Velocity)
	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

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

	// Extract trace context from the event (populated by Velocity)
	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

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

// HTTP Client Handlers

func (l *Listeners) onHTTPRequestSent(e *httpclient.RequestSent) error {
	if !l.shouldSample() {
		return nil
	}

	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

	if traceID == "" {
		return nil // Only record requests within a trace
	}
	if spanID == "" {
		spanID = GenerateSpanID()
	}

	event := NewOutgoingRequestEvent(e.Method, e.URL, e.StatusCode, float64(e.DurationMs))
	event.TraceID = traceID
	event.SpanID = spanID
	if parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName
	event.Attributes["request_size"] = e.RequestSize
	event.Attributes["response_size"] = e.ResponseSize

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onHTTPRequestFailed(e *httpclient.RequestFailed) error {
	if !l.shouldSample() {
		return nil
	}

	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

	if traceID == "" {
		return nil // Only record requests within a trace
	}
	if spanID == "" {
		spanID = GenerateSpanID()
	}

	event := NewOutgoingRequestEvent(e.Method, e.URL, 0, float64(e.DurationMs))
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

// Mail Handlers

func (l *Listeners) onMailSent(e *mail.MailSent) error {
	if !l.shouldSample() {
		return nil
	}

	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if spanID == "" {
		spanID = GenerateSpanID()
	}

	event := NewMailEvent(e.Subject, len(e.To), e.Channel, "sent", float64(e.DurationMs))
	event.TraceID = traceID
	event.SpanID = spanID
	if parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onMailFailed(e *mail.MailFailed) error {
	if !l.shouldSample() {
		return nil
	}

	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if spanID == "" {
		spanID = GenerateSpanID()
	}

	event := NewMailEvent(e.Subject, len(e.To), e.Channel, "failed", float64(e.DurationMs))
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

// Scheduler Handlers

func (l *Listeners) onScheduledTaskStarting(e *scheduler.ScheduledTaskStarting) error {
	// We don't record starting events - wait for finished/failed
	return nil
}

func (l *Listeners) onScheduledTaskFinished(e *scheduler.ScheduledTaskFinished) error {
	if !l.shouldSample() {
		return nil
	}

	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if spanID == "" {
		spanID = GenerateSpanID()
	}

	event := NewScheduledTaskEvent(e.TaskName, "finished", float64(e.DurationMs))
	event.TraceID = traceID
	event.SpanID = spanID
	if parentID != "" {
		event.ParentID = &parentID
	}
	event.Tags["service"] = l.serviceName

	l.collector.Add(event)
	return nil
}

func (l *Listeners) onScheduledTaskFailed(e *scheduler.ScheduledTaskFailed) error {
	if !l.shouldSample() {
		return nil
	}

	traceID := e.TraceID
	spanID := e.SpanID
	parentID := e.ParentID

	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if spanID == "" {
		spanID = GenerateSpanID()
	}

	event := NewScheduledTaskEvent(e.TaskName, "failed", float64(e.DurationMs))
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

// RecordException manually records an exception event. It also marks the
// span active on ctx as failed, so the log lines buffered under it survive
// the keep rules when the span ends (see SpanLogs.MarkFailed).
func RecordException(ctx context.Context, errType, message, stackTrace string) {
	mu.Lock()
	sdk := instance
	mu.Unlock()

	if sdk == nil || sdk.config.Disabled || sdk.collector == nil {
		return
	}

	SpanLogsFrom(ctx).MarkFailed()

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
