package velwatch

import (
	"log"
	"sync"
	"time"
)

// maxConcurrentExports bounds the number of export goroutines in flight at once.
// Without a bound, sustained backpressure (each export carries a 30s timeout)
// would let every flush tick spawn another goroutine, growing without limit.
const maxConcurrentExports = 4

// Collector batches events before handing them to the exporter
type Collector struct {
	events        []*Event
	mu            sync.Mutex
	exporter      Exporter
	batchSize     int
	flushInterval time.Duration

	// inflight tracks async exports so Shutdown can drain them before the
	// exporter is closed. sem bounds how many run concurrently. draining is
	// set by Wait under mu; from then on Add only buffers.
	inflight sync.WaitGroup
	sem      chan struct{}
	draining bool
}

// NewCollector creates a new event collector
func NewCollector(exporter Exporter, batchSize int, flushInterval time.Duration) *Collector {
	return &Collector{
		events:        make([]*Event, 0, batchSize),
		exporter:      exporter,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		sem:           make(chan struct{}, maxConcurrentExports),
	}
}

// Add adds an event to the collector.
// If the batch size is reached, events are automatically flushed.
//
// Add runs on the application's goroutines (the framework event listeners,
// the error reporter, the velocity log driver, RecordEvent), so it never waits
// on the exporter: a full batch leaves at once only when an export slot is
// free. When every slot is busy the events stay buffered and the next flush
// tick, or the next Add that finds a free slot, ships them.
func (c *Collector) Add(event *Event) {
	if event == nil {
		return
	}

	c.mu.Lock()
	c.events = append(c.events, event)
	var batch []*Event
	if !c.draining && len(c.events) >= c.batchSize && c.tryAcquireSlot() {
		batch = c.takeLocked()
		// Counted before mu is released, so a Flush and Wait that run after
		// this Add let go of the lock cannot miss the export.
		c.inflight.Add(1)
	}
	c.mu.Unlock()

	if batch != nil {
		go c.export(batch)
	}
}

// Flush sends all batched events to the transport. When every export slot is
// busy it waits for one, so it belongs on the SDK's own goroutines (the flush
// loop and shutdown), never on an application path.
func (c *Collector) Flush() {
	c.mu.Lock()
	batch := c.takeLocked()
	c.mu.Unlock()

	if len(batch) == 0 {
		return
	}
	if c.exporter == nil {
		// Nothing can ship the batch; it is dropped, as before.
		return
	}
	c.sem <- struct{}{}
	c.inflight.Add(1)
	go c.export(batch)
}

// takeLocked hands off the current batch and resets the buffer. mu must be held.
func (c *Collector) takeLocked() []*Event {
	if len(c.events) == 0 {
		return nil
	}
	events := c.events
	c.events = make([]*Event, 0, c.batchSize)
	return events
}

// tryAcquireSlot takes an export slot when one is free, without waiting.
func (c *Collector) tryAcquireSlot() bool {
	select {
	case c.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

// export hands a batch to the exporter, split by kind: span events go to
// Export, log records to ExportLogRecords when the exporter implements
// LogRecordExporter. Both halves of one batch leave together, so a request's
// log lines are shipped with the span they were emitted in.
//
// It runs on its own goroutine holding an export slot and counted on
// inflight, and releases both when it returns, so Shutdown can drain it
// before closing the exporter and a slow exporter cannot spawn unbounded
// goroutines. It is wrapped in recover so a panic inside the exporter, which
// runs outside the framework dispatcher's recover, cannot crash the host app.
func (c *Collector) export(events []*Event) {
	defer c.inflight.Done()
	defer func() { <-c.sem }()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("velwatch: recovered from panic during export: %v", r)
		}
	}()

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

// Wait blocks until all in-flight exports have completed. Call it after the
// final Flush and before closing the exporter so the last batch is not raced
// against connection teardown.
//
// From the moment Wait starts, Add only buffers: an event arriving from a
// producer that raced shutdown (a listener already running when the listeners
// were unregistered, a log line already past the driver's check) must not
// start an export that the exporter's Close would race, nor add to inflight
// while Wait is waiting on it.
func (c *Collector) Wait() {
	c.mu.Lock()
	c.draining = true
	c.mu.Unlock()
	c.inflight.Wait()
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
