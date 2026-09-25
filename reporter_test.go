package velwatch

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/velocitykode/velocity/contract"
	"github.com/velocitykode/velocity/events"
	"github.com/velocitykode/velocity/problem"
)

// exceptionEvents returns the exception events queued on the collector.
func exceptionEvents(c *Collector) []*Event {
	var out []*Event
	for _, event := range getEvents(c) {
		if event.Type == EventTypeException {
			out = append(out, event)
		}
	}
	return out
}

func TestErrorReporter_Report(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		ec        *contract.ErrorContext
		wantType  string
		wantStack string
		wantAttrs map[string]any
		noAttrs   []string
		wantTrace string
		// wantParent is the span the exception parents onto: the span the
		// report was made under. The exception keeps its own span id.
		wantParent string
	}{
		{
			name: "recovered panic in a request",
			err:  errors.New("panic: a bug"),
			ec: &contract.ErrorContext{
				RequestID:  "req-1",
				TraceID:    "trace-1",
				SpanID:     "span-1",
				Method:     "GET",
				URL:        "/boom",
				Recovered:  true,
				PanicStack: "goroutine 1 [running]:\nmain.boom()",
			},
			wantType:   "RequestError",
			wantStack:  "goroutine 1 [running]:\nmain.boom()",
			wantAttrs:  map[string]any{"method": "GET", "path": "/boom", "request_id": "req-1", "recovered": true},
			wantTrace:  "trace-1",
			wantParent: "span-1",
		},
		{
			name: "returned error in a request",
			err:  errors.New("db down"),
			ec: &contract.ErrorContext{
				RequestID: "req-2",
				TraceID:   "trace-2",
				SpanID:    "span-2",
				Method:    "POST",
				URL:       "/orders",
			},
			// recovered is always a real bool, false for a returned error, so
			// consumers can tell "not a panic" from "not reported".
			wantType:   "RequestError",
			wantAttrs:  map[string]any{"method": "POST", "path": "/orders", "request_id": "req-2", "recovered": false},
			wantTrace:  "trace-2",
			wantParent: "span-2",
		},
		{
			name: "structured stack when no panic stack",
			err:  errors.New("db down"),
			ec: &contract.ErrorContext{
				Method: "GET",
				URL:    "/x",
				StackTrace: &contract.StackTrace{Frames: []contract.StackFrame{
					{File: "/app/handler.go", Line: 12, Function: "Show", Package: "app"},
				}},
			},
			wantType:  "RequestError",
			wantStack: "#0 /app/handler.go:12\n    app.Show\n",
		},
		{
			name:      "report outside a request",
			err:       errors.New("job exhausted its retries"),
			ec:        &contract.ErrorContext{TraceID: "trace-3"},
			wantType:  "Error",
			wantAttrs: map[string]any{"recovered": false},
			noAttrs:   []string{"method", "path", "request_id"},
			wantTrace: "trace-3",
		},
		{
			name:      "nil context",
			err:       errors.New("console failure"),
			wantType:  "Error",
			wantAttrs: map[string]any{"recovered": false},
			noAttrs:   []string{"method", "path", "request_id"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := testCollector()
			r := newErrorReporter(NewListeners(collector, events.NewDispatcher(), "test-service", 1.0))

			r.Report(tt.err, tt.ec)

			got := exceptionEvents(collector)
			if len(got) != 1 {
				t.Fatalf("exception events = %d, want 1", len(got))
			}
			ev := got[0]
			if ev.Attributes["type"] != tt.wantType {
				t.Errorf("type = %v, want %q", ev.Attributes["type"], tt.wantType)
			}
			if ev.Attributes["message"] != tt.err.Error() {
				t.Errorf("message = %v, want %q", ev.Attributes["message"], tt.err.Error())
			}
			if ev.Attributes["stack_trace"] != tt.wantStack {
				t.Errorf("stack_trace = %q, want %q", ev.Attributes["stack_trace"], tt.wantStack)
			}
			for k, want := range tt.wantAttrs {
				if ev.Attributes[k] != want {
					t.Errorf("attribute %s = %v, want %v", k, ev.Attributes[k], want)
				}
			}
			for _, k := range tt.noAttrs {
				if v, ok := ev.Attributes[k]; ok {
					t.Errorf("attribute %s = %v, want it absent", k, v)
				}
			}
			if ev.Tags["service"] != "test-service" {
				t.Errorf("service tag = %q, want %q", ev.Tags["service"], "test-service")
			}
			if tt.wantTrace != "" && ev.TraceID != tt.wantTrace {
				t.Errorf("TraceID = %q, want %q", ev.TraceID, tt.wantTrace)
			}
			if ev.TraceID == "" {
				t.Error("TraceID is empty, want one generated")
			}
			// The exception is its own child span: it never reuses the span
			// the report names (two records, one span id, is invalid OTLP)
			// and parents onto it instead.
			if ev.SpanID == "" {
				t.Error("SpanID is empty, want one generated")
			}
			if tt.wantParent != "" {
				if ev.SpanID == tt.wantParent {
					t.Errorf("SpanID = %q, want a unique id, not the reported span", ev.SpanID)
				}
				if ev.ParentID == nil || *ev.ParentID != tt.wantParent {
					t.Errorf("ParentID = %v, want %q", ev.ParentID, tt.wantParent)
				}
			} else if ev.ParentID != nil {
				t.Errorf("ParentID = %q, want none when the report names no span", *ev.ParentID)
			}
		})
	}
}

func TestErrorReporter_StoppedDropsReports(t *testing.T) {
	collector := testCollector()
	r := newErrorReporter(NewListeners(collector, events.NewDispatcher(), "test-service", 1.0))

	r.stop()
	r.Report(errors.New("late"), &contract.ErrorContext{Method: "GET", URL: "/x"})

	if got := len(getEvents(collector)); got != 0 {
		t.Fatalf("events after stop = %d, want 0", got)
	}
}

func TestErrorReporter_RespectsSampleRate(t *testing.T) {
	collector := testCollector()
	listeners := NewListeners(nil, events.NewDispatcher(), "test-service", 0.0)
	listeners.collector = collector
	r := newErrorReporter(listeners)

	r.Report(errors.New("sampled out"), &contract.ErrorContext{Method: "GET", URL: "/x"})

	if got := len(getEvents(collector)); got != 0 {
		t.Fatalf("events with a 0%% sample rate = %d, want 0", got)
	}
}

// TestInit_AddsReporterToErrorHandler checks the reporter against the
// framework's own error handler: initialization adds it, the handler's
// report gate decides what reaches it (a 5xx and a recovered panic do, a
// 4xx does not), and shutdown stops it although the handler still holds it.
func TestInit_AddsReporterToErrorHandler(t *testing.T) {
	h := problem.NewHandler(problem.WithReporters())

	config := testConfig()
	config.BatchSize = 1000
	config.FlushInterval = time.Hour

	mu.Lock()
	err := initLocked(events.NewDispatcher(), h, config)
	sdk := instance
	mu.Unlock()
	t.Cleanup(func() { _ = Shutdown() })
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}
	if sdk.reporter == nil {
		t.Fatal("SDK has no error reporter, want one added to the error handler")
	}

	h.Report(errors.New("db down"), &contract.ErrorContext{Method: "GET", URL: "/orders", RequestID: "req-500"})
	h.Report(problem.NotFound("no such order"), &contract.ErrorContext{Method: "GET", URL: "/orders/9", RequestID: "req-404"})
	h.Report(errors.New("panic: a bug"), &contract.ErrorContext{Method: "GET", URL: "/boom", RequestID: "req-panic", Recovered: true, PanicStack: "goroutine 7 [running]:"})

	got := exceptionEvents(sdk.collector)
	if len(got) != 2 {
		t.Fatalf("exception events = %d, want 2 (the 5xx and the panic, not the 404)", len(got))
	}
	byRequest := map[string]*Event{}
	for _, ev := range got {
		id, _ := ev.Attributes["request_id"].(string)
		byRequest[id] = ev
	}
	if _, ok := byRequest["req-404"]; ok {
		t.Error("the 404 reached the reporter, want the handler's gate to keep it out")
	}
	// recovered is always a real bool: false on a returned error.
	if ev := byRequest["req-500"]; ev == nil || ev.Attributes["recovered"] != false {
		t.Errorf("5xx event = %+v, want one with recovered false", ev)
	}
	if ev := byRequest["req-panic"]; ev == nil || ev.Attributes["recovered"] != true ||
		!strings.HasPrefix(ev.Attributes["stack_trace"].(string), "goroutine 7") {
		t.Errorf("panic event = %+v, want recovered with its stack", ev)
	}

	// Nothing queued may leave for the (absent) receiver at shutdown.
	clearEvents(sdk.collector)
	if err := Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	h.Report(errors.New("after shutdown"), &contract.ErrorContext{Method: "GET", URL: "/late"})
	if got := len(getEvents(sdk.collector)); got != 0 {
		t.Fatalf("events after shutdown = %d, want 0 (the reporter must be stopped)", got)
	}
}
