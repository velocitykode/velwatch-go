package velwatch

import (
	"hash/fnv"
	"log/slog"
	"time"
)

// SpanOutcome describes how a span ended. It is the tail-based input to the
// keep rules: the lines a span buffered are filtered when the span ends, once
// its fate is known, so a healthy request can throw away everything it logged
// while a failed or slow one keeps the whole story.
type SpanOutcome struct {
	// Failed reports that the span ended badly: a 5xx response, a recorded
	// exception or panic, or a non-nil error returned by a job or console
	// command.
	Failed bool

	// Duration is how long the span took. A span slower than the configured
	// threshold keeps every line it logged.
	Duration time.Duration
}

// SetOutcome records how the span ended, for the keep rules to read when
// RecordSpanLogs filters the buffer. Call it before RecordSpanLogs:
//
//	ctx, logs := velwatch.StartSpanLogs(ctx)
//	start := time.Now()
//	defer func() { logs.SetOutcome(velwatch.SpanOutcome{Failed: err != nil, Duration: time.Since(start)}); velwatch.RecordSpanLogs(logs) }()
//
// Middleware does this for every instrumented request. It is a no-op on a nil
// buffer, so a caller need not check whether log capture is on.
//
// A span already marked failed (MarkFailed) stays failed whatever outcome
// says: a recorded exception is a fact about the span, and the status code
// the caller derived Failed from may well be a tidy 200 rendered after the
// error was handled.
func (s *SpanLogs) SetOutcome(outcome SpanOutcome) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	outcome.Failed = outcome.Failed || s.outcome.Failed
	s.outcome = outcome
}

// MarkFailed flags the span as failed without touching its duration, for code
// that learns the span is doomed before it ends: a recovered panic, or an
// exception recorded deep under the span. It is a no-op on a nil buffer.
func (s *SpanLogs) MarkFailed() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outcome.Failed = true
}

// Outcome returns the outcome recorded on the span, zero when none was set.
func (s *SpanLogs) Outcome() SpanOutcome {
	if s == nil {
		return SpanOutcome{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outcome
}

// applyKeepRules decides which of a span's buffered lines survive, given how
// the span ended. It returns the kept lines in capture order and how many were
// discarded. The rules, in order:
//
//  1. The span failed: keep every line, the whole context of the failure.
//  2. The span was slower than slowThreshold: keep every line.
//  3. Otherwise keep warn and above, always.
//  4. Keep the remaining lines only when the trace is sampled at rate.
//
// Healthy, fast, unsampled traffic therefore sends nothing, which is the
// point: the lines that explain an incident survive and the rest do not.
func applyKeepRules(lines []LogLine, outcome SpanOutcome, traceID string, slowThreshold time.Duration, rate float64) (kept []LogLine, dropped uint64) {
	if len(lines) == 0 {
		return nil, 0
	}
	if outcome.Failed || (slowThreshold > 0 && outcome.Duration > slowThreshold) {
		return lines, 0
	}
	if traceSampled(traceID, rate) {
		return lines, 0
	}

	kept = make([]LogLine, 0, len(lines))
	for _, line := range lines {
		if line.Level >= slog.LevelWarn {
			kept = append(kept, line)
			continue
		}
		dropped++
	}
	if len(kept) == 0 {
		return nil, dropped
	}
	return kept, dropped
}

// traceSampled reports whether the trace is sampled at rate.
//
// The SDK has no per-trace sampled flag today: the event listeners draw
// rand.Float64() per event against the same rate, which cannot be replayed
// when a span ends. The verdict is therefore derived from the trace id, so
// every span of a trace and every process that sees it agree, and so a
// restart cannot change its mind mid-trace. The hash is the one the ingest
// service uses for its own KeepTrace sampler, which keeps the two sides from
// disagreeing about the same trace at the same rate.
//
// A span with no trace id fails open (kept): sampling is a volume control,
// not a filter that should silently swallow edge inputs.
func traceSampled(traceID string, rate float64) bool {
	if rate >= 1.0 {
		return true
	}
	if rate <= 0 {
		return false
	}
	if traceID == "" {
		return true
	}
	return traceBucket(traceID) < rate
}

// traceBucket maps a trace id onto [0.0, 1.0) via FNV-1a plus a splitmix64
// finalizer. Raw FNV-1a mixes its low bits well but not its high bits, so the
// finalizer spreads the hash before its low 53 bits feed the float mantissa
// exactly, with no modulo bias. This is a bucketing hash, not a security
// boundary; stdlib hash/fnv is used here because no velocity equivalent
// exists and nothing stdlib reaches this package's public surface.
func traceBucket(traceID string) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(traceID))
	return float64(mixTraceHash(h.Sum64())&(1<<53-1)) / float64(1<<53)
}

// mixTraceHash is the splitmix64 finalizer: a fixed bijection that avalanches
// every input bit across the whole word. The constants are the canonical ones.
func mixTraceHash(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
