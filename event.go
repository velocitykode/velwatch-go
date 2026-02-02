package velwatch

import (
	"encoding/json"
	"time"
)

// Event types
const (
	EventTypeRequest   = "request"
	EventTypeQuery     = "query"
	EventTypeCache     = "cache"
	EventTypeException = "exception"
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
