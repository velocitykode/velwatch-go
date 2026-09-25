package velwatch

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/velocitykode/velocity/events"
	"github.com/velocitykode/velocity/orm"
	"github.com/velocitykode/velocity/router"
)

// --- Finding #1: span hierarchy -------------------------------------------

func TestChildSpanIsUniqueWithContextParent(t *testing.T) {
	collector := testCollector()
	l := NewListeners(collector, events.NewDispatcher(), "svc", 1.0)

	// Query with an enclosing context span but no framework parent: the child
	// must keep a unique span ID and parent onto the context span.
	e := &orm.QueryExecuted{
		Context:  context.Background(),
		SQL:      "SELECT 1",
		Duration: time.Millisecond,
		TraceID:  "trace-1",
		SpanID:   "ctx-span", // enclosing request/operation span
		ParentID: "",         // framework gives none
	}
	if err := l.onQueryExecuted(e); err != nil {
		t.Fatal(err)
	}
	ev := getEvents(collector)[0]
	if ev.SpanID == "" || ev.SpanID == "ctx-span" {
		t.Errorf("child SpanID = %q, want a unique ID distinct from the context span", ev.SpanID)
	}
	if ev.ParentID == nil || *ev.ParentID != "ctx-span" {
		t.Errorf("child ParentID = %v, want %q (the enclosing context span)", ev.ParentID, "ctx-span")
	}
}

func TestChildSpanPrefersFrameworkParent(t *testing.T) {
	collector := testCollector()
	l := NewListeners(collector, events.NewDispatcher(), "svc", 1.0)

	// Query inside a transaction: velocity sets SpanID to the stmt-root span
	// and ParentID to the tx span. The child must parent onto the tx span.
	e := &orm.QueryExecuted{
		Context:  context.Background(),
		SQL:      "INSERT",
		Duration: time.Millisecond,
		TraceID:  "trace-1",
		SpanID:   "stmt-root-span",
		ParentID: "tx-span",
	}
	if err := l.onQueryExecuted(e); err != nil {
		t.Fatal(err)
	}
	ev := getEvents(collector)[0]
	if ev.ParentID == nil || *ev.ParentID != "tx-span" {
		t.Errorf("child ParentID = %v, want the framework-provided tx span %q", ev.ParentID, "tx-span")
	}
	if ev.SpanID == "stmt-root-span" || ev.SpanID == "" {
		t.Errorf("child SpanID = %q, want a unique generated ID", ev.SpanID)
	}
}

func TestRootRequestKeepsContextSpan(t *testing.T) {
	collector := testCollector()
	l := NewListeners(collector, events.NewDispatcher(), "svc", 1.0)

	e := &router.RequestHandled{
		Context:    context.Background(),
		Method:     "GET",
		Path:       "/x",
		StatusCode: 200,
		Duration:   time.Millisecond,
		TraceID:    "trace-1",
		SpanID:     "req-span",
		ParentID:   "", // top of trace
	}
	if err := l.onRequestHandled(e); err != nil {
		t.Fatal(err)
	}
	ev := getEvents(collector)[0]
	if ev.SpanID != "req-span" {
		t.Errorf("root request SpanID = %q, want the context span %q so children can parent onto it", ev.SpanID, "req-span")
	}
	if ev.ParentID != nil {
		t.Errorf("root request ParentID = %v, want nil at the top of a trace", *ev.ParentID)
	}
}

func TestExceptionIsChildOfRequestNotSameSpan(t *testing.T) {
	collector := testCollector()
	l := NewListeners(collector, events.NewDispatcher(), "svc", 1.0)

	e := &router.RequestFailed{
		Context:  context.Background(),
		Method:   "GET",
		Path:     "/boom",
		Error:    context.DeadlineExceeded,
		TraceID:  "trace-1",
		SpanID:   "req-span",
		ParentID: "",
	}
	if err := l.onRequestFailed(e); err != nil {
		t.Fatal(err)
	}
	ev := getEvents(collector)[0]
	if ev.SpanID == "req-span" {
		t.Error("exception must not share the request's span ID (invalid OTLP: two rows, one span)")
	}
	if ev.ParentID == nil || *ev.ParentID != "req-span" {
		t.Errorf("exception ParentID = %v, want the request span %q", ev.ParentID, "req-span")
	}
	// recovered must be an unconditional real bool.
	if got, ok := ev.Attributes["recovered"].(bool); !ok || got != false {
		t.Errorf("recovered = %v (%T), want a bool false", ev.Attributes["recovered"], ev.Attributes["recovered"])
	}
}

// --- Finding #2: timestamp is span START ----------------------------------

func TestTimestampIsSpanStart(t *testing.T) {
	before := time.Now()
	e := NewQueryEvent("SELECT 1", 2000, 0) // 2000ms duration
	after := time.Now()

	// Timestamp must be back-dated by the duration: start ≈ emission - 2s.
	wantMin := before.Add(-2000 * time.Millisecond)
	wantMax := after.Add(-2000 * time.Millisecond)
	if e.Timestamp.Before(wantMin) || e.Timestamp.After(wantMax) {
		t.Errorf("Timestamp = %v, want ≈ emission-2s (between %v and %v)", e.Timestamp, wantMin, wantMax)
	}
	// And end = start + duration must land back at ≈ now.
	end := e.Timestamp.Add(2000 * time.Millisecond)
	if end.Before(before) || end.After(after.Add(time.Millisecond)) {
		t.Errorf("start+duration = %v, want ≈ now (%v..%v)", end, before, after)
	}
}

func TestZeroDurationTimestampUnchanged(t *testing.T) {
	before := time.Now()
	e := NewCacheEvent("get", "k", true, 0)
	if e.Timestamp.Before(before) {
		t.Errorf("point-in-time event timestamp %v should not be back-dated", e.Timestamp)
	}
}

// --- Finding #3: sub-ms duration precision --------------------------------

func TestSubMillisecondDurationPrecision(t *testing.T) {
	collector := testCollector()
	l := NewListeners(collector, events.NewDispatcher(), "svc", 1.0)

	e := &orm.QueryExecuted{
		Context:  context.Background(),
		SQL:      "SELECT 1",
		Duration: 750 * time.Microsecond, // 0.75ms - would floor to 0 with Milliseconds()
		TraceID:  "trace-1",
		SpanID:   "ctx-span",
	}
	if err := l.onQueryExecuted(e); err != nil {
		t.Fatal(err)
	}
	ev := getEvents(collector)[0]
	if got := ev.Attributes["duration_ms"].(float64); got != 0.75 {
		t.Errorf("duration_ms = %v, want 0.75 (no flooring)", got)
	}
}

// --- Finding #4: deterministic per-trace sampling -------------------------

func TestSamplingIsDeterministicPerTrace(t *testing.T) {
	l := NewListeners(nil, events.NewDispatcher(), "svc", 0.5)

	// Same trace id => same decision, every time.
	traceID := GenerateTraceID()
	first := l.sampleTrace(traceID)
	for i := 0; i < 1000; i++ {
		if l.sampleTrace(traceID) != first {
			t.Fatalf("sampleTrace(%q) is not deterministic", traceID)
		}
	}
}

func TestSamplingAllOrNothingAcrossEventsOfATrace(t *testing.T) {
	collector := testCollector()
	l := NewListeners(collector, events.NewDispatcher(), "svc", 0.5)

	// Find a trace id that this rate keeps, then assert every event kind in
	// that trace agrees (all kept). Then find one that is dropped and assert
	// all dropped.
	var kept, dropped string
	for kept == "" || dropped == "" {
		id := GenerateTraceID()
		if l.sampleTrace(id) {
			kept = id
		} else {
			dropped = id
		}
	}

	emit := func(traceID string) int {
		clearEvents(collector)
		_ = l.onRequestHandled(&router.RequestHandled{Context: context.Background(), Method: "GET", Path: "/", Duration: time.Millisecond, TraceID: traceID, SpanID: "s"})
		_ = l.onQueryExecuted(&orm.QueryExecuted{Context: context.Background(), SQL: "SELECT 1", Duration: time.Millisecond, TraceID: traceID, SpanID: "s"})
		return len(getEvents(collector))
	}

	if n := emit(kept); n != 2 {
		t.Errorf("kept trace: got %d events, want 2 (all events kept)", n)
	}
	if n := emit(dropped); n != 0 {
		t.Errorf("dropped trace: got %d events, want 0 (all events dropped)", n)
	}
}

func TestSamplingRateApproximatelyHonored(t *testing.T) {
	l := NewListeners(nil, events.NewDispatcher(), "svc", 0.5)
	const n = 20000
	kept := 0
	for i := 0; i < n; i++ {
		if l.sampleTrace(GenerateTraceID()) {
			kept++
		}
	}
	frac := float64(kept) / float64(n)
	if frac < 0.45 || frac > 0.55 {
		t.Errorf("kept fraction = %.3f, want ≈ 0.5", frac)
	}
}

// --- Finding #5: Shutdown flushes final batch before Close ----------------

type recordingExporter struct {
	mu       sync.Mutex
	batches  [][]*Event
	delay    time.Duration
	seq      *atomic.Int32
	exportAt int32
	closeAt  int32
}

func (r *recordingExporter) Export(events []*Event) error {
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	r.mu.Lock()
	r.batches = append(r.batches, events)
	r.exportAt = r.seq.Add(1)
	r.mu.Unlock()
	return nil
}

func (r *recordingExporter) Close() error {
	r.closeAt = r.seq.Add(1)
	return nil
}

func TestShutdownFlushesFinalBatchBeforeClose(t *testing.T) {
	seq := &atomic.Int32{}
	exp := &recordingExporter{delay: 40 * time.Millisecond, seq: seq}
	collector := NewCollector(exp, 1000, time.Hour) // batch large so no auto-flush
	sdk := &SDK{
		config:    Config{ServiceName: "svc"},
		collector: collector,
		exporter:  exp,
	}

	collector.Add(NewRequestEvent("GET", "/", 200, 1))

	if err := sdk.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	exp.mu.Lock()
	defer exp.mu.Unlock()
	if len(exp.batches) != 1 || len(exp.batches[0]) != 1 {
		t.Fatalf("exporter received %d batches, want the final batch of 1 event", len(exp.batches))
	}
	if exp.exportAt == 0 {
		t.Fatal("export never happened")
	}
	if exp.closeAt < exp.exportAt {
		t.Errorf("Close (seq %d) ran before Export finished (seq %d): final batch raced teardown", exp.closeAt, exp.exportAt)
	}
}

// --- Finding #7: Post sends its body --------------------------------------

func TestPostSendsBody(t *testing.T) {
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	c := NewInstrumentedHTTPClient(srv.Client())
	body := []byte(`{"hello":"world"}`)
	resp, err := c.Post(context.Background(), srv.URL, "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if string(got) != string(body) {
		t.Errorf("server received body %q, want %q", string(got), string(body))
	}
}
