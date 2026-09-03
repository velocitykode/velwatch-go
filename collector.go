package velwatch

import (
	"log"
	"sync"
	"time"
)

// Collector batches events before handing them to the exporter
type Collector struct {
	events        []*Event
	mu            sync.Mutex
	exporter      Exporter
	batchSize     int
	flushInterval time.Duration
}

// NewCollector creates a new event collector
func NewCollector(exporter Exporter, batchSize int, flushInterval time.Duration) *Collector {
	return &Collector{
		events:        make([]*Event, 0, batchSize),
		exporter:      exporter,
		batchSize:     batchSize,
		flushInterval: flushInterval,
	}
}

// Add adds an event to the collector.
// If the batch size is reached, events are automatically flushed.
func (c *Collector) Add(event *Event) {
	if event == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.events = append(c.events, event)

	if len(c.events) >= c.batchSize {
		c.flushLocked()
	}
}

// Flush sends all batched events to the transport
func (c *Collector) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushLocked()
}

// flushLocked flushes events while holding the lock
func (c *Collector) flushLocked() {
	if len(c.events) == 0 {
		return
	}

	// Take ownership of current events
	events := c.events
	c.events = make([]*Event, 0, c.batchSize)

	// Send asynchronously to avoid blocking
	go c.export(events)
}

// export hands a batch to the exporter, split by kind: span events go to
// Export, log records to ExportLogRecords when the exporter implements
// LogRecordExporter. Both halves of one batch leave together, so a request's
// log lines are shipped with the span they were emitted in.
func (c *Collector) export(events []*Event) {
	if c.exporter == nil {
		return
	}

	spans, logs := splitLogEvents(events)
	if len(logs) > 0 {
		if exporter, ok := c.exporter.(LogRecordExporter); ok {
			_ = exporter.ExportLogRecords(logs)
		} else {
			warnNoLogSupport(c.exporter)
			logRecordsDropped.Add(uint64(len(logs)))
		}
	}
	if len(spans) > 0 {
		_ = c.exporter.Export(spans)
	}
}

// noLogSupportWarned makes the "this exporter cannot ship log records" notice
// a once-per-process line rather than one per flush.
var noLogSupportWarned sync.Once

// warnNoLogSupport reports, once, that captured log lines are being discarded
// because the configured exporter has no log signal. Both wire protocols the
// SDK ships (otlp, otlphttp) implement LogRecordExporter, and the removed grpc
// wire is rejected at initialization, so this only fires for a custom exporter.
func warnNoLogSupport(exporter Exporter) {
	noLogSupportWarned.Do(func() {
		log.Printf("velwatch: exporter %T cannot ship log records; captured log lines are dropped "+
			"(see LogRecordsDropped)", exporter)
	})
}

// Len returns the current number of batched events
func (c *Collector) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.events)
}
