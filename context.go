package velwatch

import (
	"context"

	"github.com/velocitykode/velocity/trace"
)

// WithTraceContext adds trace context to a context
// Delegates to Velocity's trace package for compatibility with ORM events
func WithTraceContext(ctx context.Context, traceID, spanID string) context.Context {
	return trace.WithTrace(ctx, traceID, spanID)
}

// WithParentSpan adds parent span ID to a context by creating a new span
// The current span becomes the parent
func WithParentSpan(ctx context.Context, parentID string) context.Context {
	return trace.WithSpan(ctx, parentID)
}

// GetTraceID retrieves the trace ID from context
func GetTraceID(ctx context.Context) string {
	return trace.GetTraceID(ctx)
}

// GetSpanID retrieves the span ID from context
func GetSpanID(ctx context.Context) string {
	return trace.GetSpanID(ctx)
}

// GetParentID retrieves the parent span ID from context
func GetParentID(ctx context.Context) string {
	return trace.GetParentID(ctx)
}

// GenerateTraceID generates a new random trace ID (32 hex chars)
func GenerateTraceID() string {
	return trace.MustGenerateTraceID()
}

// GenerateSpanID generates a new random span ID (16 hex chars)
func GenerateSpanID() string {
	return trace.MustGenerateSpanID()
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
	// Set trace ID and incoming span as the initial span
	if th.TraceID != "" {
		ctx = trace.WithTrace(ctx, th.TraceID, th.SpanID)
	}
	// Create new span for this request (incoming span becomes parent)
	ctx, _ = trace.WithNewSpan(ctx)
	return ctx
}
