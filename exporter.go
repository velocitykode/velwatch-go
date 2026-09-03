package velwatch

// Exporter ships a batch of events to a backend. It is the seam between the
// collector (which buffers events) and the wire protocol. Implementations:
//
//	*OTLPExporter      - OpenTelemetry OTLP/gRPC (the default wire)
//	*OTLPHTTPExporter  - OpenTelemetry OTLP/HTTP (protobuf over POST /v1/traces)
//	*Transport         - legacy Velwatch gRPC proto (EventService.Ingest),
//	                     deprecated and slated for removal (VW-43)
//
// A new wire protocol is added by implementing this interface; no collector,
// listener, or SDK lifecycle code changes.
type Exporter interface {
	// Export sends a batch of events. It is called from a background
	// goroutine and must be safe for concurrent use.
	Export(events []*Event) error

	// Close releases any underlying connection.
	Close() error
}
