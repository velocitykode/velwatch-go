package velwatch

import (
	"context"
	"testing"
	"time"

	"github.com/velocitykode/velocity/pkg/queue"
)

// testCollector creates a collector for testing that won't flush automatically
func testCollector() *Collector {
	// Use a large batch size and long flush interval to prevent auto-flushing
	return &Collector{
		events:        make([]*Event, 0, 1000),
		transport:     nil, // Won't be used since we won't reach batch size
		batchSize:     1000,
		flushInterval: time.Hour,
	}
}

// getEvents returns events from collector (for testing)
func getEvents(c *Collector) []*Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.events
}

// clearEvents clears events from collector (for testing)
func clearEvents(c *Collector) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = make([]*Event, 0, c.batchSize)
}

func TestOnJobQueued(t *testing.T) {
	collector := testCollector()
	listeners := NewListeners(collector, "test-service", 1.0)

	t.Run("records job queued event", func(t *testing.T) {
		clearEvents(collector)
		e := &queue.JobQueued{
			Context:  context.Background(),
			JobType:  "*queue.EmailJob",
			Queue:    "emails",
			Delayed:  false,
			DelayMs:  0,
			TraceID:  "trace-123",
			SpanID:   "span-456",
			ParentID: "parent-789",
		}

		err := listeners.onJobQueued(e)
		if err != nil {
			t.Fatalf("onJobQueued returned error: %v", err)
		}

		if len(getEvents(collector)) != 1 {
			t.Fatalf("expected 1 event, got %d", len(getEvents(collector)))
		}

		event := getEvents(collector)[0]
		if event.Type != EventTypeJob {
			t.Errorf("Type = %q, want %q", event.Type, EventTypeJob)
		}
		if event.TraceID != "trace-123" {
			t.Errorf("TraceID = %q, want %q", event.TraceID, "trace-123")
		}
		if event.SpanID != "span-456" {
			t.Errorf("SpanID = %q, want %q", event.SpanID, "span-456")
		}
		if event.ParentID == nil || *event.ParentID != "parent-789" {
			t.Errorf("ParentID = %v, want %q", event.ParentID, "parent-789")
		}
		if event.Attributes["job_type"] != "*queue.EmailJob" {
			t.Errorf("job_type = %v, want %q", event.Attributes["job_type"], "*queue.EmailJob")
		}
		if event.Attributes["queue"] != "emails" {
			t.Errorf("queue = %v, want %q", event.Attributes["queue"], "emails")
		}
		if event.Attributes["status"] != "queued" {
			t.Errorf("status = %v, want %q", event.Attributes["status"], "queued")
		}
		if event.Attributes["delayed"] != false {
			t.Errorf("delayed = %v, want false", event.Attributes["delayed"])
		}
		if event.Tags["service"] != "test-service" {
			t.Errorf("service tag = %v, want %q", event.Tags["service"], "test-service")
		}
	})

	t.Run("records delayed job with delay_ms", func(t *testing.T) {
		clearEvents(collector)
		e := &queue.JobQueued{
			Context:  context.Background(),
			JobType:  "*queue.ReportJob",
			Queue:    "reports",
			Delayed:  true,
			DelayMs:  5000,
			TraceID:  "trace-delayed",
			SpanID:   "span-delayed",
			ParentID: "",
		}

		err := listeners.onJobQueued(e)
		if err != nil {
			t.Fatalf("onJobQueued returned error: %v", err)
		}

		if len(getEvents(collector)) != 1 {
			t.Fatalf("expected 1 event, got %d", len(getEvents(collector)))
		}

		event := getEvents(collector)[0]
		if event.Attributes["delayed"] != true {
			t.Errorf("delayed = %v, want true", event.Attributes["delayed"])
		}
		if event.Attributes["delay_ms"] != int64(5000) {
			t.Errorf("delay_ms = %v, want 5000", event.Attributes["delay_ms"])
		}
	})

	t.Run("generates trace ID if missing", func(t *testing.T) {
		clearEvents(collector)
		e := &queue.JobQueued{
			Context:  context.Background(),
			JobType:  "*queue.TestJob",
			Queue:    "default",
			Delayed:  false,
			DelayMs:  0,
			TraceID:  "", // Missing
			SpanID:   "", // Missing
			ParentID: "",
		}

		err := listeners.onJobQueued(e)
		if err != nil {
			t.Fatalf("onJobQueued returned error: %v", err)
		}

		if len(getEvents(collector)) != 1 {
			t.Fatalf("expected 1 event, got %d", len(getEvents(collector)))
		}

		event := getEvents(collector)[0]
		if event.TraceID == "" {
			t.Error("TraceID should be generated when missing")
		}
		if event.SpanID == "" {
			t.Error("SpanID should be generated when missing")
		}
	})

	t.Run("respects sample rate", func(t *testing.T) {
		clearEvents(collector)
		// Create listener with 0% sample rate
		zeroSampleListeners := NewListeners(nil, "test-service", 0.0)
		zeroSampleListeners.collector = collector

		e := &queue.JobQueued{
			Context: context.Background(),
			JobType: "*queue.TestJob",
			Queue:   "default",
			TraceID: "trace-123",
			SpanID:  "span-456",
		}

		err := zeroSampleListeners.onJobQueued(e)
		if err != nil {
			t.Fatalf("onJobQueued returned error: %v", err)
		}

		if len(getEvents(collector)) != 0 {
			t.Errorf("expected 0 events with 0%% sample rate, got %d", len(getEvents(collector)))
		}
	})
}

func TestOnJobProcessing(t *testing.T) {
	collector := testCollector()
	listeners := NewListeners(collector, "test-service", 1.0)

	t.Run("does not record processing event", func(t *testing.T) {
		clearEvents(collector)
		e := &queue.JobProcessing{
			Context:  context.Background(),
			JobType:  "*queue.TestJob",
			Queue:    "default",
			TraceID:  "trace-123",
			SpanID:   "span-456",
			ParentID: "",
		}

		err := listeners.onJobProcessing(e)
		if err != nil {
			t.Fatalf("onJobProcessing returned error: %v", err)
		}

		// onJobProcessing intentionally does not record events
		// The job.processed or job.failed event captures the full duration
		if len(getEvents(collector)) != 0 {
			t.Errorf("expected 0 events (processing is not recorded), got %d", len(getEvents(collector)))
		}
	})
}

func TestOnJobProcessed(t *testing.T) {
	collector := testCollector()
	listeners := NewListeners(collector, "test-service", 1.0)

	t.Run("records job processed event", func(t *testing.T) {
		clearEvents(collector)
		e := &queue.JobProcessed{
			Context:    context.Background(),
			JobType:    "*queue.EmailJob",
			Queue:      "emails",
			DurationMs: 150,
			TraceID:    "trace-processed",
			SpanID:     "span-processed",
			ParentID:   "parent-processed",
		}

		err := listeners.onJobProcessed(e)
		if err != nil {
			t.Fatalf("onJobProcessed returned error: %v", err)
		}

		if len(getEvents(collector)) != 1 {
			t.Fatalf("expected 1 event, got %d", len(getEvents(collector)))
		}

		event := getEvents(collector)[0]
		if event.Type != EventTypeJob {
			t.Errorf("Type = %q, want %q", event.Type, EventTypeJob)
		}
		if event.TraceID != "trace-processed" {
			t.Errorf("TraceID = %q, want %q", event.TraceID, "trace-processed")
		}
		if event.Attributes["status"] != "processed" {
			t.Errorf("status = %v, want %q", event.Attributes["status"], "processed")
		}
		if event.Attributes["duration_ms"] != float64(150) {
			t.Errorf("duration_ms = %v, want 150", event.Attributes["duration_ms"])
		}
	})

	t.Run("generates trace ID if missing", func(t *testing.T) {
		clearEvents(collector)
		e := &queue.JobProcessed{
			Context:    context.Background(),
			JobType:    "*queue.TestJob",
			Queue:      "default",
			DurationMs: 100,
			TraceID:    "",
			SpanID:     "",
			ParentID:   "",
		}

		err := listeners.onJobProcessed(e)
		if err != nil {
			t.Fatalf("onJobProcessed returned error: %v", err)
		}

		event := getEvents(collector)[0]
		if event.TraceID == "" {
			t.Error("TraceID should be generated when missing")
		}
		if event.SpanID == "" {
			t.Error("SpanID should be generated when missing")
		}
	})

	t.Run("respects sample rate", func(t *testing.T) {
		clearEvents(collector)
		zeroSampleListeners := NewListeners(nil, "test-service", 0.0)
		zeroSampleListeners.collector = collector

		e := &queue.JobProcessed{
			Context:    context.Background(),
			JobType:    "*queue.TestJob",
			Queue:      "default",
			DurationMs: 100,
			TraceID:    "trace-123",
			SpanID:     "span-456",
		}

		err := zeroSampleListeners.onJobProcessed(e)
		if err != nil {
			t.Fatalf("onJobProcessed returned error: %v", err)
		}

		if len(getEvents(collector)) != 0 {
			t.Errorf("expected 0 events with 0%% sample rate, got %d", len(getEvents(collector)))
		}
	})
}

func TestOnJobFailed(t *testing.T) {
	collector := testCollector()
	listeners := NewListeners(collector, "test-service", 1.0)

	t.Run("records job failed event", func(t *testing.T) {
		clearEvents(collector)
		e := &queue.JobFailed{
			Context:    context.Background(),
			JobType:    "*queue.PaymentJob",
			Queue:      "payments",
			Error:      "connection timeout",
			DurationMs: 30000,
			TraceID:    "trace-failed",
			SpanID:     "span-failed",
			ParentID:   "parent-failed",
		}

		err := listeners.onJobFailed(e)
		if err != nil {
			t.Fatalf("onJobFailed returned error: %v", err)
		}

		if len(getEvents(collector)) != 1 {
			t.Fatalf("expected 1 event, got %d", len(getEvents(collector)))
		}

		event := getEvents(collector)[0]
		if event.Type != EventTypeJob {
			t.Errorf("Type = %q, want %q", event.Type, EventTypeJob)
		}
		if event.TraceID != "trace-failed" {
			t.Errorf("TraceID = %q, want %q", event.TraceID, "trace-failed")
		}
		if event.Attributes["status"] != "failed" {
			t.Errorf("status = %v, want %q", event.Attributes["status"], "failed")
		}
		if event.Attributes["duration_ms"] != float64(30000) {
			t.Errorf("duration_ms = %v, want 30000", event.Attributes["duration_ms"])
		}
		if event.Attributes["error"] != "connection timeout" {
			t.Errorf("error = %v, want %q", event.Attributes["error"], "connection timeout")
		}
	})

	t.Run("generates trace ID if missing", func(t *testing.T) {
		clearEvents(collector)
		e := &queue.JobFailed{
			Context:    context.Background(),
			JobType:    "*queue.TestJob",
			Queue:      "default",
			Error:      "test error",
			DurationMs: 50,
			TraceID:    "",
			SpanID:     "",
			ParentID:   "",
		}

		err := listeners.onJobFailed(e)
		if err != nil {
			t.Fatalf("onJobFailed returned error: %v", err)
		}

		event := getEvents(collector)[0]
		if event.TraceID == "" {
			t.Error("TraceID should be generated when missing")
		}
		if event.SpanID == "" {
			t.Error("SpanID should be generated when missing")
		}
	})

	t.Run("respects sample rate", func(t *testing.T) {
		clearEvents(collector)
		zeroSampleListeners := NewListeners(nil, "test-service", 0.0)
		zeroSampleListeners.collector = collector

		e := &queue.JobFailed{
			Context:    context.Background(),
			JobType:    "*queue.TestJob",
			Queue:      "default",
			Error:      "test error",
			DurationMs: 100,
			TraceID:    "trace-123",
			SpanID:     "span-456",
		}

		err := zeroSampleListeners.onJobFailed(e)
		if err != nil {
			t.Fatalf("onJobFailed returned error: %v", err)
		}

		if len(getEvents(collector)) != 0 {
			t.Errorf("expected 0 events with 0%% sample rate, got %d", len(getEvents(collector)))
		}
	})
}

func TestNewJobEvent(t *testing.T) {
	t.Run("creates job event with correct fields", func(t *testing.T) {
		event := NewJobEvent("*queue.EmailJob", "emails", "processed", 150.5)

		if event.Type != EventTypeJob {
			t.Errorf("Type = %q, want %q", event.Type, EventTypeJob)
		}
		if event.Attributes["job_type"] != "*queue.EmailJob" {
			t.Errorf("job_type = %v, want %q", event.Attributes["job_type"], "*queue.EmailJob")
		}
		if event.Attributes["queue"] != "emails" {
			t.Errorf("queue = %v, want %q", event.Attributes["queue"], "emails")
		}
		if event.Attributes["status"] != "processed" {
			t.Errorf("status = %v, want %q", event.Attributes["status"], "processed")
		}
		if event.Attributes["duration_ms"] != 150.5 {
			t.Errorf("duration_ms = %v, want 150.5", event.Attributes["duration_ms"])
		}
		if event.SpanID == "" {
			t.Error("SpanID should be auto-generated")
		}
		if event.Timestamp.IsZero() {
			t.Error("Timestamp should be set")
		}
	})
}

func TestEventTypeJobConstant(t *testing.T) {
	if EventTypeJob != "job" {
		t.Errorf("EventTypeJob = %q, want %q", EventTypeJob, "job")
	}
}
