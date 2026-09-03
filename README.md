# Velwatch SDK for Go

The Velwatch SDK provides automatic instrumentation for Go applications built with the Velocity framework. It captures HTTP requests, database queries, cache operations, and exceptions, sending them to the Velwatch APM service for monitoring and analysis.

## Installation

```bash
go get github.com/velocitykode/velwatch-go
```

## Quick Start

The SDK auto-initializes during `velocity.New()`. Add a blank import and set
`VELWATCH_TOKEN`; no initialization code is needed. The SDK is flushed and
closed automatically by `App.Shutdown`.

```go
package main

import (
    _ "github.com/velocitykode/velwatch-go" // auto-initializes from VELWATCH_* env
)
```

```bash
VELWATCH_TOKEN=vw_xxx
VELWATCH_ENDPOINT=velwatch.example.com:4317
VELWATCH_SERVICE_NAME=my-api
```

Without `VELWATCH_TOKEN` the SDK stays dormant (no-op), so it is safe to leave
the import in place across environments.

### Programmatic initialization

For explicit configuration, call `Init` with a constructed Velocity app:

```go
err := velwatch.Init(app, velwatch.Config{
    Endpoint:    "velwatch.example.com:4317",
    Token:       os.Getenv("VELWATCH_TOKEN"),
    ServiceName: "my-api",
})
if err != nil {
    log.Fatal(err)
}
defer velwatch.Shutdown()
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Endpoint` | string | per protocol | Ingest endpoint (host:port for OTLP/gRPC, URL for OTLP/HTTP); defaults to `localhost:4317` for `otlp`, `localhost:4318` for `otlphttp` |
| `Token` | string | required | Project API token (vw_xxx), sent as a Bearer token |
| `ServiceName` | string | required | Service name for identification |
| `Protocol` | string | `otlp` | Wire protocol: `otlp` or `otlphttp` |
| `BatchSize` | int | 100 | Events to batch before sending |
| `FlushInterval` | time.Duration | 1s | How often to flush batched events |
| `Insecure` | bool | false | Disable TLS (for local dev) |
| `Disabled` | bool | false | Completely disable SDK |
| `SampleRate` | float64 | 1.0 | Percentage of requests to trace |

## Wire Protocols

The SDK ships events over one of two wires, selected by `Protocol`
(`VELWATCH_PROTOCOL`). All authenticate with the project token as
`Authorization: Bearer <token>`.

| `Protocol` | Wire | Use |
|------------|------|-----|
| `otlp` (default) | OpenTelemetry OTLP/gRPC (port 4317) | Backend services |
| `otlphttp` | OpenTelemetry OTLP/HTTP (`application/x-protobuf`, port 4318) | Browser, serverless, edge, mobile runtimes |

The OTLP exporters map events onto OpenTelemetry spans following OTel semantic
conventions (`http.*`, `db.*`, `messaging.*`, and `exception.*` span events),
preserving W3C-shaped trace and span IDs. This lets Velwatch ingest first-party
SDK telemetry and any standard OpenTelemetry source through one path.

For OTLP/HTTP, `Endpoint` may be a base URL (`https://host:4318`) or a full
traces URL; the standard `/v1/traces` path is appended when absent.

An unrecognized `Protocol` fails initialization with an error listing the valid
values, rather than falling back to a wire you did not ask for.

### OTLP is the default

OTLP is the ingest contract going forward. With `VELWATCH_PROTOCOL` unset the
SDK builds the OTLP/gRPC exporter and `VELWATCH_ENDPOINT` defaults to
`localhost:4317`.

The endpoint default follows the protocol, so each wire reaches its own
standard receiver port when `VELWATCH_ENDPOINT` is left unset:

| `VELWATCH_PROTOCOL` | Default `VELWATCH_ENDPOINT` |
|---------------------|-----------------------------|
| `otlp` (default)    | `localhost:4317`            |
| `otlphttp`          | `localhost:4318`            |

### The first-party ingest wire is gone

Earlier versions shipped a first-party gRPC wire selected with
`VELWATCH_PROTOCOL=grpc` and served on port `50051`. It has been removed, and
the server no longer accepts it. `VELWATCH_PROTOCOL=grpc` now fails
initialization with an error naming the replacement.

To migrate, set `VELWATCH_PROTOCOL=otlp` (or leave it unset, since OTLP is the
default) and point `VELWATCH_ENDPOINT` at the OTLP receiver on port `4317`. No
code change is required.

Port `50051` itself is still a valid endpoint: the platform serves the OTLP
gRPC receiver on that port, so `VELWATCH_ENDPOINT=localhost:50051` with
`VELWATCH_PROTOCOL=otlp` is a supported local and self-hosted configuration.

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

Over the OTLP exporters (`otlp` and `otlphttp`) each tag is exported as a span
attribute named `velwatch.tag.<key>`, so the example above ships
`velwatch.tag.environment=production`. Velwatch promotes those attributes into
the record's tags and drops them from the attributes JSON, which keeps tag keys
from colliding with semantic-convention attribute keys. The reserved tags
`release` and `commit_sha` are also emitted in their flat form for one more
release; that copy is deprecated, so read the prefixed keys.

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
VELWATCH_TOKEN=vw_xxx            # Project API token (unset = SDK dormant)
VELWATCH_ENDPOINT=host:port      # Ingest endpoint (default per protocol: 4317 otlp, 4318 otlphttp)
VELWATCH_SERVICE_NAME=my-api     # Service name (default APP_NAME)
VELWATCH_PROTOCOL=otlp           # Wire protocol: otlp (default) | otlphttp
VELWATCH_SAMPLE_RATE=0.5         # Sample rate (0.0-1.0)
VELWATCH_BATCH_SIZE=100          # Events per batch
VELWATCH_FLUSH_INTERVAL=1s       # Flush cadence
VELWATCH_INSECURE=true           # Disable TLS (local dev)
VELWATCH_DISABLED=true           # Disable the SDK entirely
VELWATCH_LOG_CAPTURE=true        # Capture log lines emitted during a span (default off)
VELWATCH_LOG_SLOW_THRESHOLD=1s   # A span slower than this keeps every line it logged
```

With `VELWATCH_LOG_CAPTURE=true` the SDK wraps the current `slog` default
handler, so the application's own logging keeps working and log lines emitted
while a span is active are buffered on that span; lines emitted outside any
span are dropped. Left unset, `slog` is untouched. Applications that build
their own logger can wrap the handler themselves with `velwatch.LogHandler()`,
which returns `nil` when capture is off.

When the span ends, its buffered lines are queued as `log` records carrying
the trace, span and parent ids of that span, so they are batched and flushed
alongside it. Each record holds the message, the lowercase level, its OTLP
severity number and the line attributes flattened to dotted keys
(`db.query.table`); its timestamp is the one `slog` stamped on the record, not
the flush time. `Middleware` brackets every request itself. A job, a console
command or any other span does the same in two lines:

```go
ctx, logs := velwatch.StartSpanLogs(ctx)
defer velwatch.RecordSpanLogs(logs)
```

Both calls are no-ops while log capture is off.

Which of the buffered lines are actually sent is decided when the span ends,
so healthy traffic ships almost nothing:

1. the span failed (a 5xx response, a recorded exception or panic, a job or
   command that returned an error) - every line is kept;
2. the span was slower than `VELWATCH_LOG_SLOW_THRESHOLD` (a Go duration,
   default `1s`; an invalid value fails initialization) - every line is kept;
3. warn and above is always kept;
4. everything else survives only when the trace is sampled at
   `VELWATCH_SAMPLE_RATE`. The verdict is derived from the trace id, so every
   span of a trace agrees and the ingest service reaches the same answer.

Discarded lines are counted on `SpanLogs.DroppedByKeepRule()`, which
`SpanLogs.Dropped()` reports together with any lines a per-span cap refused.
`Middleware` passes the real status and duration itself. Tell the buffer how
any other span ended before recording it:

```go
logs.SetOutcome(velwatch.SpanOutcome{Failed: err != nil, Duration: time.Since(start)})
velwatch.RecordSpanLogs(logs)
```

Captured lines are exported as OTLP `LogRecord`s over the same connection as
the spans they belong to, on both `otlp` (the `LogsService` on the trace gRPC
connection) and `otlphttp` (`POST /v1/logs` beside `/v1/traces`), grouped under
the same resource so a request and its log lines agree on `service.name` and
`service.version`.

## Testing

Disable the SDK during tests by leaving `VELWATCH_TOKEN` unset (the SDK stays
dormant), or set `VELWATCH_DISABLED=true`. For programmatic init, pass
`Disabled: true`:

```go
velwatch.Init(app, velwatch.Config{Disabled: true})
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
