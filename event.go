package velwatch

import (
	"encoding/json"
	"time"
)

// Reserved tag keys the SDK stamps onto every outgoing native event when a
// release or commit SHA has been resolved. A tag the caller set explicitly on
// an event is never overwritten.
const (
	tagRelease   = "release"
	tagCommitSHA = "commit_sha"
)

// Event types
const (
	EventTypeRequest         = "request"
	EventTypeQuery           = "query"
	EventTypeCache           = "cache"
	EventTypeException       = "exception"
	EventTypeJob             = "job"
	EventTypeOutgoingRequest = "outgoing_request"
	EventTypeMail            = "mail"
	EventTypeScheduledTask   = "scheduled_task"

	// EventTypeLog is one log line written by the application. It is
	// traceless: it carries no trace, span or parent id, and is searched
	// by service, level, message and time.
	EventTypeLog = "log"
)

// Event represents a single instrumentation event
type Event struct {
	Type       string            `json:"type"`
	Timestamp  time.Time         `json:"timestamp"`
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	ParentID   *string           `json:"parent_id,omitempty"`
	Attributes map[string]any    `json:"attributes"`
	Tags       map[string]string `json:"tags,omitempty"`
}

// NewEvent creates a new event with the given type
func NewEvent(eventType string) *Event {
	return &Event{
		Type:       eventType,
		Timestamp:  time.Now(),
		SpanID:     GenerateSpanID(),
		Attributes: make(map[string]any),
		Tags:       make(map[string]string),
	}
}

// NewRequestEvent creates a new HTTP request event
func NewRequestEvent(method, path string, statusCode int, durationMs float64) *Event {
	e := NewEvent(EventTypeRequest)
	e.Attributes["method"] = method
	e.Attributes["path"] = path
	e.Attributes["status"] = statusCode
	e.Attributes["duration_ms"] = durationMs
	return e
}

// NewQueryEvent creates a new database query event
func NewQueryEvent(query string, durationMs float64, rowCount int64) *Event {
	e := NewEvent(EventTypeQuery)
	e.Attributes["query"] = query
	e.Attributes["duration_ms"] = durationMs
	e.Attributes["row_count"] = rowCount
	return e
}

// NewCacheEvent creates a new cache operation event
func NewCacheEvent(operation string, key string, hit bool, durationMs float64) *Event {
	e := NewEvent(EventTypeCache)
	e.Attributes["operation"] = operation
	e.Attributes["key"] = key
	e.Attributes["hit"] = hit
	e.Attributes["duration_ms"] = durationMs
	return e
}

// NewExceptionEvent creates a new exception event
func NewExceptionEvent(errType, message, stackTrace string) *Event {
	e := NewEvent(EventTypeException)
	e.Attributes["type"] = errType
	e.Attributes["message"] = message
	e.Attributes["stack_trace"] = stackTrace
	return e
}

// NewJobEvent creates a new queue job event
func NewJobEvent(jobType, queueName, status string, durationMs float64) *Event {
	e := NewEvent(EventTypeJob)
	e.Attributes["job_type"] = jobType
	e.Attributes["queue"] = queueName
	e.Attributes["status"] = status
	e.Attributes["duration_ms"] = durationMs
	return e
}

// NewOutgoingRequestEvent creates a new outgoing HTTP request event
func NewOutgoingRequestEvent(method, url string, statusCode int, durationMs float64) *Event {
	e := NewEvent(EventTypeOutgoingRequest)
	e.Attributes["method"] = method
	e.Attributes["url"] = url
	e.Attributes["status"] = statusCode
	e.Attributes["duration_ms"] = durationMs
	return e
}

// NewMailEvent creates a new mail event
func NewMailEvent(subject string, recipientCount int, channel, status string, durationMs float64) *Event {
	e := NewEvent(EventTypeMail)
	e.Attributes["subject"] = subject
	e.Attributes["recipient_count"] = recipientCount
	e.Attributes["channel"] = channel
	e.Attributes["status"] = status
	e.Attributes["duration_ms"] = durationMs
	return e
}

// NewScheduledTaskEvent creates a new scheduled task event
func NewScheduledTaskEvent(taskName, status string, durationMs float64) *Event {
	e := NewEvent(EventTypeScheduledTask)
	e.Attributes["task_name"] = taskName
	e.Attributes["status"] = status
	e.Attributes["duration_ms"] = durationMs
	return e
}

// NewLogEvent creates a log event from a log line. The event timestamp is the
// time the line was written, not the time of the conversion, so a line keeps
// its own position on the timeline however long it waits in a batch. A line
// with no timestamp of its own falls back to the time the event is built.
//
// The event is traceless: unlike every other event kind it carries no trace
// and no span id, because a log line is not tied to a request. The platform
// stores it under the service, level, message and time it was written with.
//
// The line's flattened attributes ride as event attributes under their dotted
// keys, alongside the reserved message, level and severity_number keys. A line
// attribute named after one of those three is overwritten by it.
func NewLogEvent(line LogLine) *Event {
	e := NewEvent(EventTypeLog)
	e.SpanID = ""
	if !line.Time.IsZero() {
		e.Timestamp = line.Time
	}
	for key, value := range line.Attrs {
		if key == "" {
			continue
		}
		e.Attributes[key] = value
	}
	e.Attributes["message"] = line.Message
	e.Attributes["level"] = logLevelName(line.Level)
	e.Attributes["severity_number"] = logSeverityNumber(line.Level)
	return e
}

// WithTraceID sets the trace ID for the event
func (e *Event) WithTraceID(traceID string) *Event {
	e.TraceID = traceID
	return e
}

// WithParent sets the parent span ID for the event
func (e *Event) WithParent(parentID string) *Event {
	e.ParentID = &parentID
	return e
}

// WithTag adds a tag to the event
func (e *Event) WithTag(key, value string) *Event {
	if e.Tags == nil {
		e.Tags = make(map[string]string)
	}
	e.Tags[key] = value
	return e
}

// setDefaultTag sets a tag only when value is non-empty and the key is not
// already present, so SDK-injected defaults (release, commit_sha) never clobber
// a tag the caller set explicitly on the event.
func (e *Event) setDefaultTag(key, value string) {
	if value == "" {
		return
	}
	if e.Tags == nil {
		e.Tags = make(map[string]string)
	}
	if _, ok := e.Tags[key]; ok {
		return
	}
	e.Tags[key] = value
}

// WithAttribute adds an attribute to the event
func (e *Event) WithAttribute(key string, value any) *Event {
	if e.Attributes == nil {
		e.Attributes = make(map[string]any)
	}
	e.Attributes[key] = value
	return e
}

// AttributesJSON returns the attributes as a JSON string
func (e *Event) AttributesJSON() string {
	if e.Attributes == nil {
		return "{}"
	}
	b, err := json.Marshal(e.Attributes)
	if err != nil {
		return "{}"
	}
	return string(b)
}
