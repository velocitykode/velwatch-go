package velwatch

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// sdkName and sdkVersion identify this SDK in the OTLP resource, as
// telemetry.sdk.name / telemetry.sdk.version.
const (
	sdkName    = "velwatch-go"
	sdkVersion = "0.1.0"
)

// tagAttrPrefix namespaces event tags on the OTLP wire. The platform promotes
// span attributes named velwatch.tag.<key> into the record's tags column and
// drops them from the attributes JSON, so a caller tag never collides with a
// semantic-convention attribute key.
const tagAttrPrefix = "velwatch.tag."

// flatTagKeys are the reserved tags still emitted as a flat span attribute
// alongside their prefixed form, so platform builds predating the
// velwatch.tag.* contract keep resolving them.
//
// Deprecated: the flat copies exist for one release only and are removed after
// that. Read release and commit_sha from velwatch.tag.release and
// velwatch.tag.commit_sha.
var flatTagKeys = map[string]bool{
	tagRelease:   true,
	tagCommitSHA: true,
}

// OTLPExporter ships events as OpenTelemetry spans over OTLP/gRPC. This is the
// standard, ecosystem-compatible wire: the same protocol any OTel-instrumented
// app speaks, so a Velwatch OTLP receiver ingests first-party and third-party
// telemetry through one path. Velocity-specific richness rides as velocity.*
// span attributes.
type OTLPExporter struct {
	conn        *grpc.ClientConn
	client      coltracepb.TraceServiceClient
	token       string
	serviceName string
	release     string
	commitSHA   string
}

// NewOTLPExporter dials an OTLP/gRPC trace endpoint (default port 4317).
func NewOTLPExporter(endpoint, token, serviceName string, insecureMode bool) (*OTLPExporter, error) {
	var opts []grpc.DialOption
	if insecureMode {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("velwatch: failed to connect to OTLP endpoint: %w", err)
	}

	return &OTLPExporter{
		conn:        conn,
		client:      coltracepb.NewTraceServiceClient(conn),
		token:       token,
		serviceName: serviceName,
	}, nil
}

// Export converts events to OTLP spans and sends them in one ExportTraceService
// request over gRPC.
func (e *OTLPExporter) Export(events []*Event) error {
	if len(events) == 0 {
		return nil
	}

	req := buildExportRequest(events, e.serviceName, e.release, e.commitSHA)

	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"authorization", "Bearer "+e.token)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if _, err := e.client.Export(ctx, req); err != nil {
		log.Printf("velwatch: failed to export OTLP spans: %v", err)
		return err
	}
	return nil
}

// buildExportRequest assembles events into a single OTLP ExportTraceService
// request grouped under one resource for the given service. The release and
// commit SHA, when non-empty, ride as the service.version and
// vcs.ref.head.revision resource attributes. Shared by the gRPC and HTTP
// exporters.
func buildExportRequest(events []*Event, serviceName, release, commitSHA string) *coltracepb.ExportTraceServiceRequest {
	spans := make([]*tracepb.Span, 0, len(events))
	for _, ev := range events {
		spans = append(spans, eventToSpan(ev))
	}

	return &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: otlpResource(serviceName, release, commitSHA),
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope: instrumentationScope(),
				Spans: spans,
			}},
		}},
	}
}

// otlpResource builds the OTLP resource every signal this SDK exports is
// grouped under: the service identity plus the SDK identity, with the release
// and commit SHA riding as service.version and vcs.ref.head.revision when they
// are known. Spans and log records share it, so a request and the lines it
// logged describe the same service instance.
func otlpResource(serviceName, release, commitSHA string) *resourcepb.Resource {
	attrs := []*commonpb.KeyValue{
		stringAttr("service.name", serviceName),
		stringAttr("telemetry.sdk.name", sdkName),
		stringAttr("telemetry.sdk.language", "go"),
		stringAttr("telemetry.sdk.version", sdkVersion),
	}
	if release != "" {
		attrs = append(attrs, stringAttr(otelServiceVersion, release))
	}
	if commitSHA != "" {
		attrs = append(attrs, stringAttr(otelVCSRevision, commitSHA))
	}
	return &resourcepb.Resource{Attributes: attrs}
}

// instrumentationScope names this SDK as the scope that produced the records.
func instrumentationScope() *commonpb.InstrumentationScope {
	return &commonpb.InstrumentationScope{Name: sdkName, Version: sdkVersion}
}

// Close closes the gRPC connection.
func (e *OTLPExporter) Close() error {
	if e.conn != nil {
		return e.conn.Close()
	}
	return nil
}

// eventToSpan maps a Velwatch Event onto an OTLP Span. Start/end times come
// from the event timestamp and its duration_ms attribute; trace, span, and
// parent IDs are decoded from the event's W3C-shaped hex IDs.
func eventToSpan(ev *Event) *tracepb.Span {
	start := ev.Timestamp
	end := start.Add(time.Duration(durationMs(ev) * float64(time.Millisecond)))

	span := &tracepb.Span{
		TraceId:           decodeID(ev.TraceID, 16),
		SpanId:            decodeID(ev.SpanID, 8),
		Name:              spanName(ev),
		Kind:              spanKind(ev.Type),
		StartTimeUnixNano: uint64(start.UnixNano()),
		EndTimeUnixNano:   uint64(end.UnixNano()),
		Attributes:        spanAttributes(ev),
	}
	if ev.ParentID != nil && *ev.ParentID != "" {
		span.ParentSpanId = decodeID(*ev.ParentID, 8)
	}
	if ev.Type == EventTypeException {
		span.Status = &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR}
		span.Events = []*tracepb.Span_Event{exceptionSpanEvent(ev, start)}
	}
	return span
}

// spanKind maps an event type to an OTLP span kind following OTel semantic
// conventions (server for inbound, client for outbound, etc.).
func spanKind(eventType string) tracepb.Span_SpanKind {
	switch eventType {
	case EventTypeRequest:
		return tracepb.Span_SPAN_KIND_SERVER
	case EventTypeOutgoingRequest, EventTypeQuery, EventTypeCache:
		return tracepb.Span_SPAN_KIND_CLIENT
	case EventTypeJob:
		return tracepb.Span_SPAN_KIND_CONSUMER
	case EventTypeMail:
		return tracepb.Span_SPAN_KIND_PRODUCER
	default:
		return tracepb.Span_SPAN_KIND_INTERNAL
	}
}

// spanName derives a human-readable span name from the event's attributes.
func spanName(ev *Event) string {
	a := ev.Attributes
	switch ev.Type {
	case EventTypeRequest, EventTypeOutgoingRequest:
		method, _ := a["method"].(string)
		target, _ := a["path"].(string)
		if target == "" {
			target, _ = a["url"].(string)
		}
		if method != "" && target != "" {
			return method + " " + target
		}
	case EventTypeQuery:
		return "DB query"
	case EventTypeCache:
		if op, ok := a["operation"].(string); ok {
			return "cache " + op
		}
	case EventTypeJob:
		if jt, ok := a["job_type"].(string); ok {
			return jt
		}
	case EventTypeMail:
		return "mail send"
	case EventTypeScheduledTask:
		if tn, ok := a["task_name"].(string); ok {
			return tn
		}
	case EventTypeException:
		if t, ok := a["type"].(string); ok {
			return t
		}
	}
	return ev.Type
}

// semconvKeys renames the well-known per-type attribute keys onto OTel semantic
// conventions. Keys not present are remapped as-is by spanAttributes.
var semconvKeys = map[string]map[string]string{
	EventTypeRequest: {
		"method": "http.request.method",
		"path":   "url.path",
		"status": "http.response.status_code",
	},
	EventTypeOutgoingRequest: {
		"method": "http.request.method",
		"url":    "url.full",
		"status": "http.response.status_code",
	},
	EventTypeQuery: {
		"query":     "db.query.text",
		"row_count": "db.response.returned_rows",
	},
	EventTypeCache: {
		"operation": "db.operation.name",
		"key":       "db.cache.key",
		"hit":       "db.cache.hit",
	},
	EventTypeJob: {
		"job_type": "messaging.message.type",
		"queue":    "messaging.destination.name",
		"status":   "messaging.velwatch.status",
	},
	EventTypeMail: {
		"subject":         "velocity.mail.subject",
		"recipient_count": "velocity.mail.recipient_count",
		"channel":         "messaging.destination.name",
		"status":          "messaging.velwatch.status",
	},
	EventTypeScheduledTask: {
		"task_name": "velocity.scheduler.task",
		"status":    "velocity.scheduler.status",
	},
}

// spanAttributes builds the OTLP attribute list for an event: known keys are
// renamed to semantic conventions, the rest pass through verbatim, and tags are
// appended under the velwatch.tag.* prefix. The duration_ms attribute is
// dropped (it is the span duration) and
// exception fields are dropped (they live on the exception span event).
func spanAttributes(ev *Event) []*commonpb.KeyValue {
	rename := semconvKeys[ev.Type]
	attrs := make([]*commonpb.KeyValue, 0, len(ev.Attributes)+len(ev.Tags)+len(flatTagKeys))

	for k, v := range ev.Attributes {
		if k == "duration_ms" {
			continue
		}
		if ev.Type == EventTypeException && (k == "type" || k == "message" || k == "stack_trace") {
			continue
		}
		key := k
		if mapped, ok := rename[k]; ok {
			key = mapped
		}
		attrs = append(attrs, anyAttr(key, v))
	}
	return appendTagAttributes(attrs, ev)
}

// appendTagAttributes appends an event's tags to attrs under the
// velwatch.tag.* prefix, plus the flat copy the reserved tags still carry. It
// is shared by spans and log records so a tag reads the same on either signal.
func appendTagAttributes(attrs []*commonpb.KeyValue, ev *Event) []*commonpb.KeyValue {
	for k, v := range ev.Tags {
		if k == "" {
			continue
		}
		key := k
		if !strings.HasPrefix(key, tagAttrPrefix) {
			key = tagAttrPrefix + key
		}
		attrs = append(attrs, stringAttr(key, v))
		if flatTagKeys[k] {
			attrs = append(attrs, stringAttr(k, v))
		}
	}
	return attrs
}

// exceptionSpanEvent builds the OTLP "exception" span event from an exception
// event, using the exception.* semantic conventions.
func exceptionSpanEvent(ev *Event, t time.Time) *tracepb.Span_Event {
	a := ev.Attributes
	exType, _ := a["type"].(string)
	msg, _ := a["message"].(string)
	stack, _ := a["stack_trace"].(string)
	return &tracepb.Span_Event{
		TimeUnixNano: uint64(t.UnixNano()),
		Name:         "exception",
		Attributes: []*commonpb.KeyValue{
			stringAttr("exception.type", exType),
			stringAttr("exception.message", msg),
			stringAttr("exception.stacktrace", stack),
		},
	}
}

// durationMs reads the duration_ms attribute, tolerating float64 or int64.
func durationMs(ev *Event) float64 {
	switch v := ev.Attributes["duration_ms"].(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case int:
		return float64(v)
	default:
		return 0
	}
}

// decodeID hex-decodes a W3C-shaped trace/span ID into exactly n bytes. On any
// mismatch it returns n random bytes so the span still carries a valid ID
// rather than being rejected by the receiver.
func decodeID(s string, n int) []byte {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != n {
		out := make([]byte, n)
		_, _ = rand.Read(out)
		return out
	}
	return b
}

// anyAttr builds a KeyValue from an arbitrary Go value.
func anyAttr(key string, v any) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: anyValue(v)}
}

func stringAttr(key, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_StringValue{StringValue: v},
	}}
}

// anyValue maps a Go value onto an OTLP AnyValue. Every integer kind lands on
// IntValue, including the unsigned kinds a captured log attribute carries
// (slog.KindUint64 normalizes to uint64): an unsigned value too large for an
// int64 is the only one that falls back to its string form, because OTLP has
// no wider integer to hold it. Anything else is rendered with fmt.
func anyValue(v any) *commonpb.AnyValue {
	switch x := v.(type) {
	case string:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: x}}
	case bool:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: x}}
	case int:
		return intValue(int64(x))
	case int8:
		return intValue(int64(x))
	case int16:
		return intValue(int64(x))
	case int32:
		return intValue(int64(x))
	case int64:
		return intValue(x)
	case uint8:
		return intValue(int64(x))
	case uint16:
		return intValue(int64(x))
	case uint32:
		return intValue(int64(x))
	case uint:
		if uint64(x) > math.MaxInt64 {
			break
		}
		return intValue(int64(x))
	case uint64:
		if x > math.MaxInt64 {
			break
		}
		return intValue(int64(x))
	case float64:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: x}}
	case float32:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: float64(x)}}
	}
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: fmt.Sprintf("%v", v)}}
}

// intValue wraps an int64 as an OTLP AnyValue.
func intValue(v int64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}
}
