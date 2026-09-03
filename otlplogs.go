package velwatch

import (
	"encoding/hex"
	"math"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

// Reserved attribute keys a log event carries. Each one is promoted to a
// dedicated OTLP LogRecord field (body, severity text, severity number) and is
// not repeated in the record's attribute list.
const (
	logAttrMessage        = "message"
	logAttrLevel          = "level"
	logAttrSeverityNumber = "severity_number"
)

// buildExportLogsRequest assembles log events into a single OTLP
// ExportLogsService request. The records are grouped under the same resource
// the trace exporter builds for the same service, so a span and the lines
// logged inside it agree on service.name, service.version and the rest of the
// resource. Shared by the gRPC and HTTP exporters.
func buildExportLogsRequest(events []*Event, serviceName, release, commitSHA string) *collogspb.ExportLogsServiceRequest {
	records := make([]*logspb.LogRecord, 0, len(events))
	for _, ev := range events {
		if ev == nil {
			continue
		}
		records = append(records, eventToLogRecord(ev))
	}

	return &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: otlpResource(serviceName, release, commitSHA),
			ScopeLogs: []*logspb.ScopeLogs{{
				Scope:      instrumentationScope(),
				LogRecords: records,
			}},
		}},
	}
}

// eventToLogRecord maps a log event onto an OTLP LogRecord. The event
// timestamp is both the record time and its observed time: the SDK stamps a
// line the moment slog produced it, so there is no separate observation to
// report. The trace and span ids come from the span the line was captured in.
func eventToLogRecord(ev *Event) *logspb.LogRecord {
	nanos := logTimeUnixNano(ev.Timestamp)
	return &logspb.LogRecord{
		TimeUnixNano:         nanos,
		ObservedTimeUnixNano: nanos,
		SeverityNumber:       logspb.SeverityNumber(logAttrInt(ev, logAttrSeverityNumber)),
		SeverityText:         logAttrString(ev, logAttrLevel),
		Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{
			StringValue: logAttrString(ev, logAttrMessage),
		}},
		Attributes: logRecordAttributes(ev),
		TraceId:    optionalID(ev.TraceID, 16),
		SpanId:     optionalID(ev.SpanID, 8),
	}
}

// logRecordAttributes builds the attribute list for a log record: everything
// the line carried except the three reserved keys, which already have OTLP
// fields of their own, plus the event's tags under the velwatch.tag.* prefix.
func logRecordAttributes(ev *Event) []*commonpb.KeyValue {
	attrs := make([]*commonpb.KeyValue, 0, len(ev.Attributes)+len(ev.Tags))
	for k, v := range ev.Attributes {
		switch k {
		case "", logAttrMessage, logAttrLevel, logAttrSeverityNumber:
			continue
		}
		attrs = append(attrs, anyAttr(k, v))
	}
	return appendTagAttributes(attrs, ev)
}

// logTimeUnixNano converts a timestamp to the OTLP epoch-nanosecond form. A
// zero or pre-epoch time reports 0, which OTLP reads as "unknown", rather than
// wrapping into a huge unsigned value.
func logTimeUnixNano(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	nanos := t.UnixNano()
	if nanos < 0 {
		return 0
	}
	return uint64(nanos)
}

// logAttrString reads a reserved string attribute off a log event.
func logAttrString(ev *Event, key string) string {
	s, _ := ev.Attributes[key].(string)
	return s
}

// logAttrInt reads a reserved integer attribute off a log event, tolerating
// the integer kinds an attribute map may hold. A value out of int32 range is
// reported as 0, the OTLP "unspecified" severity.
func logAttrInt(ev *Event, key string) int32 {
	var n int64
	switch v := ev.Attributes[key].(type) {
	case int:
		n = int64(v)
	case int32:
		n = int64(v)
	case int64:
		n = v
	case float64:
		n = int64(v)
	default:
		return 0
	}
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0
	}
	return int32(n)
}

// optionalID hex-decodes a trace or span id a log record may carry. Unlike
// decodeID it never invents one: a log line without trace context goes out
// with no ids at all, which is what keeps it traceless on the wire. A
// malformed id is dropped for the same reason.
func optionalID(s string, n int) []byte {
	if s == "" {
		return nil
	}
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != n {
		return nil
	}
	return b
}
