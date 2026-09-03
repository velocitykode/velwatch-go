package velwatch

import (
	"strings"
	"testing"
	"time"

	"github.com/velocitykode/velocity/app"
	"github.com/velocitykode/velocity/events"
)

func TestConfigFromEnvLogSlowThreshold(t *testing.T) {
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv returned error: %v", err)
	}
	if cfg.LogSlowThreshold != 0 {
		t.Errorf("LogSlowThreshold = %s with the variable unset, want 0 (initialization applies the default)",
			cfg.LogSlowThreshold)
	}

	t.Setenv("VELWATCH_LOG_SLOW_THRESHOLD", "750ms")
	cfg, err = configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv returned error: %v", err)
	}
	if cfg.LogSlowThreshold != 750*time.Millisecond {
		t.Errorf("LogSlowThreshold = %s, want 750ms", cfg.LogSlowThreshold)
	}
}

func TestInvalidLogSlowThresholdFailsInitialization(t *testing.T) {
	for _, bad := range []string{"soon", "1", "-2s"} {
		t.Run(bad, func(t *testing.T) {
			t.Setenv("VELWATCH_LOG_SLOW_THRESHOLD", bad)

			_, err := configFromEnv()
			if err == nil {
				t.Fatalf("configFromEnv accepted VELWATCH_LOG_SLOW_THRESHOLD=%q, want an error", bad)
			}
			if !strings.Contains(err.Error(), "VELWATCH_LOG_SLOW_THRESHOLD") || !strings.Contains(err.Error(), bad) {
				t.Errorf("error = %q, want it to name the variable and the value %q", err, bad)
			}

			t.Setenv("VELWATCH_TOKEN", "vw_test")
			if bootErr := autoBoot(&app.Services{Events: events.NewDispatcher()}); bootErr == nil {
				t.Error("autoBoot accepted the invalid threshold, want initialization to fail")
			}
			if IsInitialized() {
				t.Error("SDK initialized despite the invalid threshold")
			}
		})
	}
}

func TestInitRejectsNegativeLogSlowThreshold(t *testing.T) {
	config := testConfig()
	config.LogSlowThreshold = -time.Second

	_, err := initForTest(t, config)
	if err == nil {
		t.Fatal("initLocked accepted a negative LogSlowThreshold, want an error")
	}
	if !strings.Contains(err.Error(), "LogSlowThreshold") {
		t.Errorf("error = %q, want it to name LogSlowThreshold", err)
	}
}

func TestInitAppliesDefaultLogSlowThreshold(t *testing.T) {
	sdk, err := initForTest(t, testConfig())
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}
	if sdk.config.LogSlowThreshold != defaultLogSlowThreshold {
		t.Errorf("LogSlowThreshold = %s, want %s", sdk.config.LogSlowThreshold, defaultLogSlowThreshold)
	}
}

func TestConfigFromEnvLogMaxLines(t *testing.T) {
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv returned error: %v", err)
	}
	if cfg.LogMaxLines != 0 {
		t.Errorf("LogMaxLines = %d with the variable unset, want 0 (initialization applies the default)",
			cfg.LogMaxLines)
	}

	t.Setenv("VELWATCH_LOG_MAX_LINES", "120")
	cfg, err = configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv returned error: %v", err)
	}
	if cfg.LogMaxLines != 120 {
		t.Errorf("LogMaxLines = %d, want 120", cfg.LogMaxLines)
	}
}

func TestInvalidLogMaxLinesFailsInitialization(t *testing.T) {
	for _, bad := range []string{"lots", "12.5", "0", "-1"} {
		t.Run(bad, func(t *testing.T) {
			t.Setenv("VELWATCH_LOG_MAX_LINES", bad)

			_, err := configFromEnv()
			if err == nil {
				t.Fatalf("configFromEnv accepted VELWATCH_LOG_MAX_LINES=%q, want an error", bad)
			}
			if !strings.Contains(err.Error(), "VELWATCH_LOG_MAX_LINES") || !strings.Contains(err.Error(), bad) {
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

func TestInitRejectsNegativeLogMaxLines(t *testing.T) {
	config := testConfig()
	config.LogMaxLines = -1

	_, err := initForTest(t, config)
	if err == nil {
		t.Fatal("initLocked accepted a negative LogMaxLines, want an error")
	}
	if !strings.Contains(err.Error(), "LogMaxLines") {
		t.Errorf("error = %q, want it to name LogMaxLines", err)
	}
}

func TestInitAppliesDefaultLogMaxLines(t *testing.T) {
	sdk, err := initForTest(t, testConfig())
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}
	if sdk.config.LogMaxLines != defaultLogMaxLines {
		t.Errorf("LogMaxLines = %d, want %d", sdk.config.LogMaxLines, defaultLogMaxLines)
	}
}
