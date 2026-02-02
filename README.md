# Velwatch SDK for Go

The Velwatch SDK provides automatic instrumentation for Go applications built with the Velocity framework. It captures HTTP requests, database queries, cache operations, and exceptions, sending them to the Velwatch APM service for monitoring and analysis.

## Installation

```bash
go get github.com/velocitykode/velwatch-go
```

## Quick Start

```go
package main

import (
    "os"
    "log"

    "github.com/velocitykode/velwatch-go"
)

func main() {
    // Initialize Velwatch SDK
    err := velwatch.Init(velwatch.Config{
        Endpoint:    "velwatch.example.com:50051",
        Token:       os.Getenv("VELWATCH_TOKEN"),
        ServiceName: "my-api",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer velwatch.Shutdown()

    // Your Velocity application code
    app.Run()
}
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Endpoint` | string | required | Velwatch gRPC endpoint |
| `Token` | string | required | Project API token (vw_xxx) |
| `ServiceName` | string | required | Service name for identification |
| `BatchSize` | int | 100 | Events to batch before sending |
| `FlushInterval` | time.Duration | 1s | How often to flush batched events |
| `Insecure` | bool | false | Disable TLS (for local dev) |
| `Disabled` | bool | false | Completely disable SDK |
| `SampleRate` | float64 | 1.0 | Percentage of requests to trace |

## Automatic Instrumentation

The SDK automatically instruments the following Velocity framework events:

### HTTP Requests
- `request.handled` - Captures method, path, status code, duration
- `request.failed` - Captures errors and exceptions

### Database Queries
- `query.executed` - Captures query, duration, row count
- `query.failed` - Captures query errors

### Cache Operations
- `cache.hit` - Captures cache hits
- `cache.miss` - Captures cache misses
- `cache.write` - Captures cache writes

### Exceptions
- `exception.reported` - Captures errors with stack traces

## Manual Instrumentation

For cases where automatic instrumentation isn't sufficient:

### Custom Events

```go
event := velwatch.NewEvent("custom")
event.TraceID = velwatch.CurrentTraceID(ctx)
event.Attributes["custom_field"] = "value"
event.Tags["environment"] = "production"
velwatch.RecordEvent(event)
```

### Custom Spans

```go
ctx, span := velwatch.StartSpan(ctx, "process-data")
defer span.End()

// Your code here
```

### HTTP Middleware

For explicit HTTP instrumentation:

```go
mux := http.NewServeMux()
mux.HandleFunc("/", handler)
http.ListenAndServe(":8080", velwatch.Middleware(mux))
```

### HTTP Client Instrumentation

Propagate trace context to downstream services:

```go
client := velwatch.NewInstrumentedHTTPClient(http.DefaultClient)
resp, err := client.Get(ctx, "https://api.example.com/data")
```

## Trace Context Propagation

The SDK uses the following headers for distributed tracing:

| Header | Description |
|--------|-------------|
| `X-Velwatch-Trace-ID` | Unique trace identifier |
| `X-Velwatch-Span-ID` | Current span identifier |
| `X-Velwatch-Parent-ID` | Parent span identifier |

### Accessing Trace Context

```go
traceID := velwatch.CurrentTraceID(ctx)
spanID := velwatch.CurrentSpanID(ctx)

// Inject into headers
headers := make(map[string]string)
velwatch.InjectTraceHeader(ctx, headers)

// Extract from headers
th := velwatch.ExtractTraceHeader(headers)
ctx = velwatch.ContextFromTraceHeader(ctx, th)
```

## Environment Variables

The SDK respects the following environment variables:

```bash
VELWATCH_TOKEN=vw_xxx       # Project API token
VELWATCH_ENDPOINT=host:port # gRPC endpoint
VELWATCH_SERVICE=my-api     # Service name
VELWATCH_SAMPLE_RATE=0.5    # Sample rate (0.0-1.0)
```

## Testing

Disable the SDK during tests:

```go
velwatch.Init(velwatch.Config{
    Disabled: true,
})
```

## Best Practices

1. **Initialize Early**: Call `velwatch.Init()` as early as possible in your application startup
2. **Graceful Shutdown**: Always call `velwatch.Shutdown()` to flush remaining events
3. **Use Context**: Pass context through your application to maintain trace continuity
4. **Sample in Production**: Use `SampleRate` for high-traffic services
5. **Service Names**: Use consistent, descriptive service names

## Troubleshooting

### Events Not Appearing

1. Check that `Token` is correct
2. Verify `Endpoint` is reachable
3. Check logs for error messages
4. Ensure `Disabled` is not set to `true`

### High Memory Usage

1. Reduce `BatchSize`
2. Decrease `FlushInterval`
3. Increase `SampleRate` to reduce volume

### Missing Traces

1. Ensure trace context is propagated through context
2. Use `InstrumentedHTTPClient` for HTTP calls
3. Check that downstream services support the trace headers

## License

MIT License - see LICENSE file for details.
