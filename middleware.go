package velwatch

import (
	"context"
	"net/http"
	"time"
)

// Middleware returns an HTTP middleware that automatically instruments requests
// for a plain net/http server. A velocity application does not need it: the
// router's own request events carry the trace, and the SDK's listeners record
// them.
//
// Usage:
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/", handler)
//	http.ListenAndServe(":8080", velwatch.Middleware(mux))
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip if SDK not initialized or disabled
		mu.Lock()
		sdk := instance
		mu.Unlock()

		if sdk == nil || sdk.config.Disabled {
			next.ServeHTTP(w, r)
			return
		}

		// Extract trace headers or start new trace
		headers := make(map[string]string)
		for k, v := range r.Header {
			if len(v) > 0 {
				headers[k] = v[0]
			}
		}

		ctx := r.Context()
		th := ExtractTraceHeader(headers)
		if th != nil {
			ctx = ContextFromTraceHeader(ctx, th)
		} else {
			ctx = WithTraceContext(ctx, GenerateTraceID(), GenerateSpanID())
		}

		// Create wrapped response writer to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: 200}

		start := time.Now()

		// Serve request with trace context
		next.ServeHTTP(wrapped, r.WithContext(ctx))

		duration := time.Since(start)

		// Record request event
		traceID := GetTraceID(ctx)
		spanID := GetSpanID(ctx)
		parentID := GetParentID(ctx)

		event := NewRequestEvent(r.Method, r.URL.Path, wrapped.statusCode, float64(duration.Milliseconds()))
		event.TraceID = traceID
		event.SpanID = spanID
		if parentID != "" {
			event.ParentID = &parentID
		}
		event.Tags["service"] = sdk.config.ServiceName

		// Add request metadata
		if r.Header.Get("User-Agent") != "" {
			event.Attributes["user_agent"] = r.Header.Get("User-Agent")
		}
		if r.RemoteAddr != "" {
			event.Attributes["client_ip"] = r.RemoteAddr
		}

		sdk.collector.Add(event)
	})
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (w *responseWriter) WriteHeader(code int) {
	if !w.written {
		w.statusCode = code
		w.written = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWriter) Write(b []byte) (int, error) {
	if !w.written {
		w.written = true
	}
	return w.ResponseWriter.Write(b)
}

// InstrumentHTTPClient wraps an HTTP client to propagate trace context
type InstrumentedHTTPClient struct {
	client *http.Client
}

// NewInstrumentedHTTPClient creates an HTTP client that propagates trace context
func NewInstrumentedHTTPClient(client *http.Client) *InstrumentedHTTPClient {
	if client == nil {
		client = http.DefaultClient
	}
	return &InstrumentedHTTPClient{client: client}
}

// Do executes an HTTP request with trace context propagation
func (c *InstrumentedHTTPClient) Do(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	// Inject trace headers
	traceID := GetTraceID(ctx)
	spanID := GetSpanID(ctx)

	if traceID != "" {
		req.Header.Set("X-Velwatch-Trace-ID", traceID)
	}
	if spanID != "" {
		req.Header.Set("X-Velwatch-Parent-ID", spanID)
	}

	// Create child span
	childSpanID := GenerateSpanID()
	req.Header.Set("X-Velwatch-Span-ID", childSpanID)

	return c.client.Do(req)
}

// Get performs an HTTP GET with trace context
func (c *InstrumentedHTTPClient) Get(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Post performs an HTTP POST with trace context
func (c *InstrumentedHTTPClient) Post(ctx context.Context, url, contentType string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	return c.Do(req)
}

// HTTPClient interface for instrumented HTTP operations
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

var _ HTTPClient = (*InstrumentedHTTPClient)(nil)
