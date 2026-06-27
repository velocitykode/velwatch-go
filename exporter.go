package velwatch

// Exporter ships a batch of events to a backend. It is the seam between the
// collector (which buffers events) and the wire protocol. Implementations:
//
//	*Transport     - legacy Velwatch gRPC proto (EventService.Ingest)
//	*OTLPExporter  - OpenTelemetry OTLP/gRPC (the standard, ecosystem-compatible)
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
