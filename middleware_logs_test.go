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
