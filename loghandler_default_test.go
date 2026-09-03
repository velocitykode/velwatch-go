package velwatch

import (
	"bytes"
	"log"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// builtinDefault is slog's default logger as the process started, before any
// test installed its own: the one wrapping the unexported default handler
// that forwards through the stdlib log package.
var builtinDefault = slog.Default()

// TestCaptureDoesNotDeadlockOnTheBuiltinDefault installs capture in front of
// slog's built-in default, exactly what happens in an application that never
// called slog.SetDefault, and logs through both slog and the stdlib log
// package. Before the fix the first line deadlocked: the built-in handler
// writes via the log package, which Go had redirected back into the capture
// handler, and the log package's mutex was taken twice on one goroutine.
func TestCaptureDoesNotDeadlockOnTheBuiltinDefault(t *testing.T) {
	if !isBuiltinDefaultHandler(builtinDefault.Handler()) {
		t.Skip("another test file replaced the slog default before init; nothing to reproduce here")
	}

	var out bytes.Buffer
	previousWriter := log.Writer()
	log.SetOutput(&out)
	slog.SetDefault(builtinDefault)
	t.Cleanup(func() {
		uninstallLogCapture()
		slog.SetDefault(builtinDefault)
		log.SetOutput(previousWriter)
	})

	installLogCapture(Config{LogLevel: slog.LevelInfo})

	done := make(chan struct{})
	go func() {
		defer close(done)
		slog.Warn("framework line via slog", "k", "v")
		log.Printf("application line via the log package")
		slog.Info("second slog line")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("logging deadlocked with capture in front of slog's built-in default handler")
	}

	got := out.String()
	for _, want := range []string{"framework line via slog", "application line via the log package", "second slog line"} {
		if !strings.Contains(got, want) {
			t.Errorf("application output lost %q; got:\n%s", want, got)
		}
	}
}

func TestForwardTargetKeepsAnApplicationHandler(t *testing.T) {
	own := slog.NewJSONHandler(&bytes.Buffer{}, nil)
	if got := forwardTarget(slog.New(own)); got != own {
		t.Errorf("forwardTarget replaced the application's own handler with %T", got)
	}
	if forwardTarget(nil) != nil {
		t.Error("forwardTarget(nil) should be nil")
	}
}
