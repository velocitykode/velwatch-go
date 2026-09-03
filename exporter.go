package velwatch

import "sync/atomic"

// Exporter ships a batch of events to a backend. It is the seam between the
// collector (which buffers events) and the wire protocol. Implementations:
//
//	*OTLPExporter      - OpenTelemetry OTLP/gRPC (the default wire)
//	*OTLPHTTPExporter  - OpenTelemetry OTLP/HTTP (protobuf over POST /v1/traces)
//
// Both ship log records too: see LogRecordExporter.
//
// A new wire protocol is added by implementing this interface; no collector,
// listener, or SDK lifecycle code changes.
type Exporter interface {
	// Export sends a batch of span events. It is called from a background
	// goroutine and must be safe for concurrent use. Log records never
	// reach it: the collector routes those to LogRecordExporter.
	Export(events []*Event) error

	// Close releases any underlying connection.
	Close() error
}

// LogRecordExporter is implemented by an Exporter that can also ship log
// records: events of type EventTypeLog, each one a log line captured while a
// span was active. It is the seam a wire protocol plugs log support into.
//
// The collector splits every batch by kind: span events go to Export, log
// records to ExportLogRecords on an exporter that implements this interface.
// An exporter that does not implement it ships spans exactly as before, and
// the log records in the batch are counted by LogRecordsDropped.
type LogRecordExporter interface {
	// ExportLogRecords sends a batch of log records. Like Export, it is
	// called from a background goroutine and must be safe for concurrent
	// use.
	ExportLogRecords(events []*Event) error
}

// logRecordsDropped counts log records discarded because the configured
// exporter does not implement LogRecordExporter.
var logRecordsDropped atomic.Uint64

// LogRecordsDropped returns how many captured log records were discarded
// because the configured exporter cannot ship them. It is zero for an
// exporter that implements LogRecordExporter.
func LogRecordsDropped() uint64 {
	return logRecordsDropped.Load()
}

// maxRecordsPerExport caps how many records ride in one OTLP export request.
// The receiver rejects a request carrying more, so an exporter splits a larger
// batch into several requests rather than losing it. It applies per signal: up
// to this many spans in one trace request, up to this many log records in one
// logs request.
const maxRecordsPerExport = 2048

// chunkEvents splits events into consecutive slices of at most size elements,
// preserving order. A batch already within the limit is returned as a single
// chunk that aliases the input, so the common case allocates one slice header
// and copies nothing.
func chunkEvents(events []*Event, size int) [][]*Event {
	if size <= 0 || len(events) <= size {
		return [][]*Event{events}
	}
	chunks := make([][]*Event, 0, (len(events)+size-1)/size)
	for start := 0; start < len(events); start += size {
		end := start + size
		if end > len(events) {
			end = len(events)
		}
		chunks = append(chunks, events[start:end])
	}
	return chunks
}

// splitLogEvents partitions a batch into span events and log records, keeping
// the order within each half. Neither slice aliases the input.
func splitLogEvents(events []*Event) (spans, logs []*Event) {
	for _, event := range events {
		if event == nil {
			continue
		}
		if event.Type == EventTypeLog {
			logs = append(logs, event)
			continue
		}
		spans = append(spans, event)
	}
	return spans, logs
}
