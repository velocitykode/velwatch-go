package velwatch

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

// Context keys for trace propagation
type contextKey string

const (
	traceIDKey  contextKey = "velwatch_trace_id"
	spanIDKey   contextKey = "velwatch_span_id"
	parentIDKey contextKey = "velwatch_parent_id"
)

// WithTraceContext adds trace context to a context
func WithTraceContext(ctx context.Context, traceID, spanID string) context.Context {
	ctx = context.WithValue(ctx, traceIDKey, traceID)
	ctx = context.WithValue(ctx, spanIDKey, spanID)
	return ctx
}

// WithParentSpan adds parent span ID to a context
func WithParentSpan(ctx context.Context, parentID string) context.Context {
	return context.WithValue(ctx, parentIDKey, parentID)
}

// GetTraceID retrieves the trace ID from context
func GetTraceID(ctx context.Context) string {
	if v := ctx.Value(traceIDKey); v != nil {
		return v.(string)
	}
	return ""
}

// GetSpanID retrieves the span ID from context
func GetSpanID(ctx context.Context) string {
	if v := ctx.Value(spanIDKey); v != nil {
		return v.(string)
	}
	return ""
}

// GetParentID retrieves the parent span ID from context
func GetParentID(ctx context.Context) string {
	if v := ctx.Value(parentIDKey); v != nil {
		return v.(string)
	}
	return ""
}

// GenerateTraceID generates a new random trace ID (32 hex chars)
func GenerateTraceID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// GenerateSpanID generates a new random span ID (16 hex chars)
func GenerateSpanID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// StartSpan creates a new span within the current trace
func StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	traceID := GetTraceID(ctx)
	if traceID == "" {
		traceID = GenerateTraceID()
	}

	parentID := GetSpanID(ctx)
	spanID := GenerateSpanID()

	newCtx := WithTraceContext(ctx, traceID, spanID)
	if parentID != "" {
		newCtx = WithParentSpan(newCtx, parentID)
	}

	span := &Span{
		TraceID:  traceID,
		SpanID:   spanID,
		ParentID: parentID,
		Name:     name,
	}

	return newCtx, span
}

// Span represents an active span that can be finished
type Span struct {
	TraceID  string
	SpanID   string
	ParentID string
	Name     string
}

// End completes the span and records it
func (s *Span) End() {
	// This is a placeholder - actual implementation would record the span
	// Currently spans are recorded via event listeners
}

// TraceHeader represents trace context headers for propagation
type TraceHeader struct {
	TraceID  string
	SpanID   string
	ParentID string
}

// ExtractTraceHeader extracts trace context from HTTP headers
func ExtractTraceHeader(headers map[string]string) *TraceHeader {
	th := &TraceHeader{
		TraceID:  headers["X-Velwatch-Trace-ID"],
		SpanID:   headers["X-Velwatch-Span-ID"],
		ParentID: headers["X-Velwatch-Parent-ID"],
	}
	if th.TraceID == "" && th.SpanID == "" {
		return nil
	}
	return th
}

// InjectTraceHeader injects trace context into HTTP headers
func InjectTraceHeader(ctx context.Context, headers map[string]string) {
	if traceID := GetTraceID(ctx); traceID != "" {
		headers["X-Velwatch-Trace-ID"] = traceID
	}
	if spanID := GetSpanID(ctx); spanID != "" {
		headers["X-Velwatch-Span-ID"] = spanID
	}
	if parentID := GetParentID(ctx); parentID != "" {
		headers["X-Velwatch-Parent-ID"] = parentID
	}
}

// ContextFromTraceHeader creates a context with trace information from headers
func ContextFromTraceHeader(ctx context.Context, th *TraceHeader) context.Context {
	if th == nil {
		return ctx
	}
	if th.TraceID != "" {
		ctx = context.WithValue(ctx, traceIDKey, th.TraceID)
	}
	if th.SpanID != "" {
		// The incoming span becomes our parent
		ctx = context.WithValue(ctx, parentIDKey, th.SpanID)
	}
	// Generate new span ID for this request
	ctx = context.WithValue(ctx, spanIDKey, GenerateSpanID())
	return ctx
}
