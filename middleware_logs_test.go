package velwatch

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMiddlewareKeepRulesUseTheRealOutcome(t *testing.T) {
	newHandler := func(status int) http.Handler {
		return Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			slog.InfoContext(ctx, "handling request")
			slog.WarnContext(ctx, "cache miss")
			w.WriteHeader(status)
		}))
	}

	t.Run("500 keeps the info line", func(t *testing.T) {
		captureForTest(t)
		sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
		if err != nil {
			t.Fatalf("initLocked returned error: %v", err)
		}

		newHandler(http.StatusInternalServerError).ServeHTTP(
			httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/orders", nil))

		sent := messagesOf(logEventsIn(sdk.collector))
		if strings.Join(sent, "|") != "handling request|cache miss" {
			t.Errorf("sent %v, want both lines", sent)
		}
	})

	t.Run("fast 200 on an unsampled trace sends only the warn line", func(t *testing.T) {
		captureForTest(t)
		sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
		if err != nil {
			t.Fatalf("initLocked returned error: %v", err)
		}

		newHandler(http.StatusOK).ServeHTTP(
			httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/orders", nil))

		sent := messagesOf(logEventsIn(sdk.collector))
		if strings.Join(sent, "|") != "cache miss" {
			t.Errorf("sent %v, want only the warn line", sent)
		}
	})
}

// requestEventIn returns the single request record queued on the collector.
func requestEventIn(t *testing.T, c *Collector) *Event {
	t.Helper()

	var found *Event
	for _, event := range getEvents(c) {
		if event.Type == EventTypeRequest {
			if found != nil {
				t.Fatal("more than one request record queued")
			}
			found = event
		}
	}
	if found == nil {
		t.Fatal("no request record queued")
	}
	return found
}

// TestMiddlewareReportsDroppedLogLines asserts the request record carries
// "log.dropped" equal to the lines the cap and the floor refused, and carries
// nothing at all when a request stayed inside both limits.
func TestMiddlewareReportsDroppedLogLines(t *testing.T) {
	newHandler := func(infoLines, debugLines int) http.Handler {
		return Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			for i := 0; i < infoLines; i++ {
				slog.InfoContext(ctx, "chatter")
			}
			for i := 0; i < debugLines; i++ {
				slog.DebugContext(ctx, "noise")
			}
			w.WriteHeader(http.StatusOK)
		}))
	}

	t.Run("cap and floor drops are summed", func(t *testing.T) {
		captureForTestWith(t, Config{LogMaxLines: 3}, slog.LevelDebug)
		sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
		if err != nil {
			t.Fatalf("initLocked returned error: %v", err)
		}

		newHandler(5, 4).ServeHTTP(
			httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/orders", nil))

		// Five info lines against a cap of three drops two; four debug
		// lines are all below the info floor.
		if got := requestEventIn(t, sdk.collector).Attributes["log.dropped"]; got != uint64(6) {
			t.Errorf("log.dropped = %v (%T), want uint64(6): 2 by cap plus 4 by floor", got, got)
		}
	})

	t.Run("a request inside both limits carries no attribute", func(t *testing.T) {
		captureForTestWith(t, Config{LogMaxLines: 10}, slog.LevelDebug)
		sdk, err := initForTest(t, keepRulesConfig(unsampledRate))
		if err != nil {
			t.Fatalf("initLocked returned error: %v", err)
		}

		newHandler(2, 0).ServeHTTP(
			httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/orders", nil))

		if got, ok := requestEventIn(t, sdk.collector).Attributes["log.dropped"]; ok {
			t.Errorf("log.dropped = %v, want the attribute absent", got)
		}
	})

	t.Run("log capture off leaves the request record alone", func(t *testing.T) {
		config := keepRulesConfig(unsampledRate)
		config.LogCapture = false
		sdk, err := initForTest(t, config)
		if err != nil {
			t.Fatalf("initLocked returned error: %v", err)
		}

		newHandler(5, 4).ServeHTTP(
			httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/orders", nil))

		if got, ok := requestEventIn(t, sdk.collector).Attributes["log.dropped"]; ok {
			t.Errorf("log.dropped = %v with capture off, want the attribute absent", got)
		}
	})
}
