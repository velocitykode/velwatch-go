package velwatch

import (
	"sync"
	"testing"
	"time"
)

// gatedExporter blocks every Export until release is closed, so a test can
// hold all of the collector's export slots busy.
type gatedExporter struct {
	release chan struct{}
	started chan struct{}

	mu     sync.Mutex
	events int
}

func newGatedExporter() *gatedExporter {
	return &gatedExporter{release: make(chan struct{}), started: make(chan struct{}, 64)}
}

func (g *gatedExporter) Export(events []*Event) error {
	g.started <- struct{}{}
	<-g.release
	g.mu.Lock()
	g.events += len(events)
	g.mu.Unlock()
	return nil
}

func (g *gatedExporter) Close() error { return nil }

func (g *gatedExporter) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.events
}

// TestCollectorAddNeverWaitsOnBusyExportSlots pins the two contracts the
// collector serves at once: exports are bounded (at most
// maxConcurrentExports run together), and Add, which the log driver and the
// framework listeners call on application goroutines, never waits for a slot.
// Events that find every slot busy stay buffered and leave on the next Flush.
func TestCollectorAddNeverWaitsOnBusyExportSlots(t *testing.T) {
	exporter := newGatedExporter()
	collector := NewCollector(exporter, 1, time.Hour)

	// Each Add fills a batch of one; the first maxConcurrentExports take
	// every export slot and block in the exporter.
	for i := 0; i < maxConcurrentExports; i++ {
		collector.Add(NewRequestEvent("GET", "/", 200, 1))
	}
	for i := 0; i < maxConcurrentExports; i++ {
		select {
		case <-exporter.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("export %d never started", i+1)
		}
	}

	// With every slot busy, further Adds must return at once.
	done := make(chan struct{})
	go func() {
		collector.Add(NewRequestEvent("GET", "/", 200, 1))
		collector.Add(NewRequestEvent("GET", "/", 200, 1))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Add blocked while every export slot was busy")
	}
	if got := collector.Len(); got != 2 {
		t.Fatalf("buffered events = %d, want 2 kept for the next flush", got)
	}

	close(exporter.release)
	collector.Flush()
	collector.Wait()

	if got := exporter.count(); got != maxConcurrentExports+2 {
		t.Errorf("exported events = %d, want %d (none lost)", got, maxConcurrentExports+2)
	}
}

// TestCollectorAddAfterWaitOnlyBuffers covers a producer that raced shutdown:
// once Wait has started, a full batch must not start an export that the
// exporter's Close would race.
func TestCollectorAddAfterWaitOnlyBuffers(t *testing.T) {
	exporter := &capturingExporter{}
	collector := NewCollector(exporter, 1, time.Hour)

	collector.Wait()
	collector.Add(NewRequestEvent("GET", "/late", 200, 1))

	if spans, _ := exporter.counts(); spans != 0 {
		t.Errorf("exported spans after Wait = %d, want 0", spans)
	}
	if got := collector.Len(); got != 1 {
		t.Errorf("buffered events after Wait = %d, want 1", got)
	}
}

// panickingExporter panics on every Export.
type panickingExporter struct{}

func (panickingExporter) Export([]*Event) error { panic("exporter bug") }
func (panickingExporter) Close() error          { return nil }

// TestCollectorRecoversExporterPanic checks that a panicking exporter neither
// crashes the process nor leaks its export slot.
func TestCollectorRecoversExporterPanic(t *testing.T) {
	collector := NewCollector(panickingExporter{}, 1, time.Hour)

	// More exports than slots: a leaked slot would leave Flush waiting forever.
	done := make(chan struct{})
	go func() {
		for i := 0; i < maxConcurrentExports*2; i++ {
			collector.Add(NewRequestEvent("GET", "/", 200, 1))
			collector.Flush()
		}
		collector.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("exports never finished after the exporter panicked")
	}
}
