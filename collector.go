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
	// exporter is closed. sem bounds how many run concurrently.
	inflight sync.WaitGroup
	sem      chan struct{}
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
func (c *Collector) Add(event *Event) {
	if event == nil {
		return
	}

	c.mu.Lock()
	c.events = append(c.events, event)
	var batch []*Event
	if len(c.events) >= c.batchSize {
		batch = c.takeLocked()
	}
	c.mu.Unlock()

	// Export outside the lock so acquiring the concurrency semaphore never
	// blocks other producers appending events.
	c.export(batch)
}

// Flush sends all batched events to the transport
func (c *Collector) Flush() {
	c.mu.Lock()
	batch := c.takeLocked()
	c.mu.Unlock()
	c.export(batch)
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

// export ships a batch asynchronously. It is tracked on inflight so Shutdown
// can drain it before closing the exporter, bounded by sem so a slow exporter
// cannot spawn unbounded goroutines, and wrapped in recover so a panic inside
// the exporter - which runs outside the framework dispatcher's recover - cannot
// crash the host app.
func (c *Collector) export(events []*Event) {
	if len(events) == 0 {
		return
	}
	c.inflight.Add(1)
	c.sem <- struct{}{} // blocks (backpressure) only when all export slots are busy
	go func() {
		defer c.inflight.Done()
		defer func() { <-c.sem }()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("velwatch: recovered from panic during export: %v", r)
			}
		}()
		_ = c.exporter.Export(events)
	}()
}

// Wait blocks until all in-flight exports have completed. Call it after the
// final Flush and before closing the exporter so the last batch is not raced
// against connection teardown.
func (c *Collector) Wait() {
	c.inflight.Wait()
}

// Len returns the current number of batched events
func (c *Collector) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.events)
}
