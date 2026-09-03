package velwatch

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/velocitykode/velocity/app"
	"github.com/velocitykode/velocity/events"
)

func TestConfigFromEnvLogLevel(t *testing.T) {
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv returned error: %v", err)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v with the variable unset, want %v", cfg.LogLevel, slog.LevelInfo)
	}

	for value, want := range map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"INFO":    slog.LevelInfo,
		"Warn":    slog.LevelWarn,
		" error ": slog.LevelError,
	} {
		t.Setenv("VELWATCH_LOG_LEVEL", value)
		cfg, err := configFromEnv()
		if err != nil {
			t.Fatalf("configFromEnv rejected VELWATCH_LOG_LEVEL=%q: %v", value, err)
		}
		if cfg.LogLevel != want {
			t.Errorf("LogLevel = %v for %q, want %v", cfg.LogLevel, value, want)
		}
	}
}

func TestInvalidLogLevelFailsInitialization(t *testing.T) {
	for _, bad := range []string{"warning", "trace", "5"} {
		t.Run(bad, func(t *testing.T) {
			t.Setenv("VELWATCH_LOG_LEVEL", bad)

			_, err := configFromEnv()
			if err == nil {
				t.Fatalf("configFromEnv accepted VELWATCH_LOG_LEVEL=%q, want an error", bad)
			}
			if !strings.Contains(err.Error(), "VELWATCH_LOG_LEVEL") || !strings.Contains(err.Error(), bad) {
				t.Errorf("error = %q, want it to name the variable and the value %q", err, bad)
			}

			t.Setenv("VELWATCH_TOKEN", "vw_test")
			if bootErr := autoBoot(&app.Services{Events: events.NewDispatcher()}); bootErr == nil {
				t.Error("autoBoot accepted the invalid level, want initialization to fail")
			}
			if IsInitialized() {
				t.Error("SDK initialized despite the invalid level")
			}
		})
	}
}

func TestConfigFromEnvLogMaxPerSecond(t *testing.T) {
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv returned error: %v", err)
	}
	if cfg.LogMaxPerSecond != 0 {
		t.Errorf("LogMaxPerSecond = %d with the variable unset, want 0 "+
			"(initialization applies the default)", cfg.LogMaxPerSecond)
	}

	t.Setenv("VELWATCH_LOG_MAX_PER_SECOND", "250")
	cfg, err = configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv returned error: %v", err)
	}
	if cfg.LogMaxPerSecond != 250 {
		t.Errorf("LogMaxPerSecond = %d, want 250", cfg.LogMaxPerSecond)
	}
}

func TestInvalidLogMaxPerSecondFailsInitialization(t *testing.T) {
	for _, bad := range []string{"lots", "12.5", "0", "-1"} {
		t.Run(bad, func(t *testing.T) {
			t.Setenv("VELWATCH_LOG_MAX_PER_SECOND", bad)

			_, err := configFromEnv()
			if err == nil {
				t.Fatalf("configFromEnv accepted VELWATCH_LOG_MAX_PER_SECOND=%q, want an error", bad)
			}
			if !strings.Contains(err.Error(), "VELWATCH_LOG_MAX_PER_SECOND") ||
				!strings.Contains(err.Error(), bad) {
				t.Errorf("error = %q, want it to name the variable and the value %q", err, bad)
			}

			t.Setenv("VELWATCH_TOKEN", "vw_test")
			if bootErr := autoBoot(&app.Services{Events: events.NewDispatcher()}); bootErr == nil {
				t.Error("autoBoot accepted the invalid cap, want initialization to fail")
			}
			if IsInitialized() {
				t.Error("SDK initialized despite the invalid cap")
			}
		})
	}
}

func TestInitRejectsNegativeLogMaxPerSecond(t *testing.T) {
	config := testConfig()
	config.LogMaxPerSecond = -1

	_, err := initForTest(t, config)
	if err == nil {
		t.Fatal("initLocked accepted a negative LogMaxPerSecond, want an error")
	}
	if !strings.Contains(err.Error(), "LogMaxPerSecond") {
		t.Errorf("error = %q, want it to name LogMaxPerSecond", err)
	}
}

func TestInitAppliesDefaultLogMaxPerSecond(t *testing.T) {
	sdk, err := initForTest(t, testConfig())
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}
	if sdk.config.LogMaxPerSecond != defaultLogMaxPerSecond {
		t.Errorf("LogMaxPerSecond = %d, want %d", sdk.config.LogMaxPerSecond, defaultLogMaxPerSecond)
	}
}

func TestConfigFromEnvLogCapture(t *testing.T) {
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv returned error: %v", err)
	}
	if cfg.LogCapture {
		t.Error("LogCapture is on with VELWATCH_LOG_CAPTURE unset, want off")
	}

	t.Setenv("VELWATCH_LOG_CAPTURE", "true")
	cfg, err = configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv returned error: %v", err)
	}
	if !cfg.LogCapture {
		t.Error("LogCapture is off with VELWATCH_LOG_CAPTURE=true, want on")
	}
}
