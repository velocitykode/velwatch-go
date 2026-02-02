package velwatch

import (
	"sync"
	"time"
)

// Collector batches events before sending them to the transport
type Collector struct {
	events        []*Event
	mu            sync.Mutex
	transport     *Transport
	batchSize     int
	flushInterval time.Duration
}

// NewCollector creates a new event collector
func NewCollector(transport *Transport, batchSize int, flushInterval time.Duration) *Collector {
	return &Collector{
		events:        make([]*Event, 0, batchSize),
		transport:     transport,
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
	go c.transport.Send(events)
}

// Len returns the current number of batched events
func (c *Collector) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.events)
}
