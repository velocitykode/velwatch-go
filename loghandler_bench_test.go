package velwatch

import (
	"context"
	"log/slog"
	"testing"
)

// discardHandler accepts every record and does nothing with it. It stands in
// for the application's own logger, so the benchmark below measures the
// capture handler rather than whatever the default handler happens to write.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (discardHandler) WithAttrs([]slog.Attr) slog.Handler        { return discardHandler{} }
func (discardHandler) WithGroup(string) slog.Handler             { return discardHandler{} }

// BenchmarkLogHandlerNoopRequest measures what one log line costs inside an
// instrumented span whose handler does no work of its own: the tee to the
// application's handler plus flattening and buffering the line on the span.
// That is the whole per-line cost log capture adds to a request.
//
// The buffer is trimmed periodically so the benchmark measures the steady
// state rather than the cost of growing one slice to b.N entries.
func BenchmarkLogHandlerNoopRequest(b *testing.B) {
	handler := newLogHandler(discardHandler{}, slog.LevelInfo)
	logger := slog.New(handler)

	logs := &SpanLogs{
		traceID: "0123456789abcdef0123456789abcdef",
		spanID:  "0123456789abcdef",
	}
	ctx := context.WithValue(context.Background(), spanLogsKey{}, logs)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%1024 == 0 {
			logs.mu.Lock()
			logs.lines = logs.lines[:0]
			logs.mu.Unlock()
		}
		logger.InfoContext(ctx, "handled request", "path", "/orders", "status", 200)
	}
}
