package velwatch

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/velocitykode/velocity/app"
)

// init registers the auto-initialization boot hook. A consumer enables the
// SDK with a blank import of this package plus VELWATCH_* environment
// variables; velocity.New() runs the hook after all services are built.
// Without VELWATCH_TOKEN the hook is a no-op and the SDK stays dormant.
func init() {
	app.OnBoot(autoBoot)
}

// autoBoot initializes the SDK from environment variables against the
// app's event dispatcher, then registers the SDK as an app component so
// App.Shutdown's ShutdownAware sweep flushes and closes it.
func autoBoot(s *app.Services) error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	if cfg.Token == "" || cfg.Disabled {
		return nil // unconfigured or explicitly off: stay dormant
	}

	mu.Lock()
	if instance != nil {
		// Explicit Init already ran, or a previous app in this process
		// initialized the SDK. Keep the existing instance.
		mu.Unlock()
		return nil
	}
	err = initLocked(s.Events, cfg)
	sdk := instance
	mu.Unlock()
	if err != nil {
		return err
	}

	return app.Register(s, sdk)
}

// configFromEnv builds a Config from VELWATCH_* environment variables.
// velocity.New() loads .env before boot hooks run, so values from the
// application's .env file are visible here.
//
// A malformed VELWATCH_LOG_LEVEL or VELWATCH_LOG_MAX_PER_SECOND is an error
// rather than a silent fallback: they decide which log lines the service
// ships, so a typo there would quietly change its output. The older numeric
// variables keep their historical lenient parsing.
func configFromEnv() (Config, error) {
	cfg := Config{
		// Left empty when unset: the default depends on the protocol and is
		// applied during initialization, once the protocol is resolved.
		Endpoint:    os.Getenv("VELWATCH_ENDPOINT"),
		Token:       os.Getenv("VELWATCH_TOKEN"),
		ServiceName: envOr("VELWATCH_SERVICE_NAME", os.Getenv("APP_NAME")),
		Protocol:    envOr("VELWATCH_PROTOCOL", protocolOTLP),
		Insecure:    os.Getenv("VELWATCH_INSECURE") == "true",
		Disabled:    os.Getenv("VELWATCH_DISABLED") == "true",
		LogCapture:  os.Getenv("VELWATCH_LOG_CAPTURE") == "true",
	}
	if v := os.Getenv("VELWATCH_SAMPLE_RATE"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			cfg.SampleRate = f
		}
	}
	if v := os.Getenv("VELWATCH_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.BatchSize = n
		}
	}
	if v := os.Getenv("VELWATCH_FLUSH_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.FlushInterval = d
		}
	}
	if v := os.Getenv("VELWATCH_LOG_LEVEL"); v != "" {
		level, err := parseLogLevel(v)
		if err != nil {
			return Config{}, err
		}
		cfg.LogLevel = level
	}
	if v := os.Getenv("VELWATCH_LOG_MAX_PER_SECOND"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("velwatch: VELWATCH_LOG_MAX_PER_SECOND %q is not a valid integer "+
				"(want a positive count such as \"1000\")", v)
		}
		if n <= 0 {
			return Config{}, fmt.Errorf("velwatch: VELWATCH_LOG_MAX_PER_SECOND %q must be a positive integer "+
				"(the cap cannot be disabled; raise it instead)", v)
		}
		cfg.LogMaxPerSecond = n
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
