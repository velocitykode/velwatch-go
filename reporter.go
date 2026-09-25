package velwatch

import (
	"sync/atomic"

	"github.com/velocitykode/velocity/contract"
)

// Exception types the reporter records. A report made for a request (the
// router's error boundary, or ctx.Report in a handler) keeps the
// "RequestError" type the SDK has always shipped for request failures; a
// report made outside a request (a failed queue job or scheduled task, a
// failed async listener, a console command) is recorded as "Error".
const (
	exceptionTypeRequest = "RequestError"
	exceptionTypeOther   = "Error"
)

// errorReporter is the SDK's contract.Reporter. Initialization adds it to
// the app's error handler (contract.ErrorHandler.AddReporter), so every error
// the handler reports reaches Velwatch as one exception event: each handler
// error and recovered panic the router's error boundary reports (once, with
// a recovered panic flagged), and every other report the application's error
// pipeline makes. Errors the pipeline does not report (a 4xx answer such as a
// mapped not-found, an ignored or throttled error) never arrive.
//
// The error handler has no way to remove a reporter, so shutdown stops this
// one instead: once stopped it drops whatever it is handed.
type errorReporter struct {
	listeners *Listeners
	stopped   atomic.Bool
}

var _ contract.Reporter = (*errorReporter)(nil)

// newErrorReporter returns a reporter recording into listeners' collector.
func newErrorReporter(listeners *Listeners) *errorReporter {
	return &errorReporter{listeners: listeners}
}

// Report records err as an exception event. ec carries the request, trace
// and panic facts the error handler collected; a nil ec is allowed.
func (r *errorReporter) Report(err error, ec *contract.ErrorContext) {
	if r == nil || err == nil || r.stopped.Load() || r.listeners == nil {
		return
	}
	r.listeners.onErrorReported(err, ec)
}

// stop makes the reporter drop every later report.
func (r *errorReporter) stop() {
	if r != nil {
		r.stopped.Store(true)
	}
}

// onErrorReported records one reported error as an exception event. The
// request record itself comes from the request.handled event, which the
// router fires for every request (including failed ones) with the real
// status code and duration; the exception carries the same request, trace
// and span IDs, so it stays correlated with that record.
func (l *Listeners) onErrorReported(err error, ec *contract.ErrorContext) {
	if ec == nil {
		ec = &contract.ErrorContext{}
	}

	if !l.shouldSample() {
		return
	}

	traceID, spanID := ec.TraceID, ec.SpanID
	if traceID == "" {
		traceID = GenerateTraceID()
	}
	if spanID == "" {
		spanID = GenerateSpanID()
	}

	errType := exceptionTypeOther
	if ec.Method != "" {
		errType = exceptionTypeRequest
	}

	exEvent := NewExceptionEvent(errType, err.Error(), reportedStack(ec))
	exEvent.TraceID = traceID
	exEvent.SpanID = spanID
	exEvent.Tags["service"] = l.serviceName
	if ec.Method != "" {
		exEvent.Attributes["method"] = ec.Method
		// The error handler records the path only (no query string).
		exEvent.Attributes["path"] = ec.URL
		exEvent.Attributes["request_id"] = ec.RequestID
	}
	if ec.Recovered {
		exEvent.Attributes["recovered"] = true
	}
	l.collector.Add(exEvent)
}

// reportedStack returns the stack the error handler handed over: the raw
// goroutine stack of a recovered panic, else the structured trace when the
// handler captured one (in debug mode it captures one for every request
// error), else "".
func reportedStack(ec *contract.ErrorContext) string {
	if ec.PanicStack != "" {
		return ec.PanicStack
	}
	if ec.StackTrace != nil && len(ec.StackTrace.Frames) > 0 {
		return ec.StackTrace.String()
	}
	return ""
}
