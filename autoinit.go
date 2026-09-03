package velwatch

import (
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
	cfg := configFromEnv()
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
	err := initLocked(s.Events, cfg)
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
func configFromEnv() Config {
	cfg := Config{
		// Left empty when unset: the default depends on the protocol and is
		// applied during initialization, once the protocol is resolved.
		Endpoint:    os.Getenv("VELWATCH_ENDPOINT"),
		Token:       os.Getenv("VELWATCH_TOKEN"),
		ServiceName: envOr("VELWATCH_SERVICE_NAME", os.Getenv("APP_NAME")),
		Protocol:    envOr("VELWATCH_PROTOCOL", protocolOTLP),
		Insecure:    os.Getenv("VELWATCH_INSECURE") == "true",
		Disabled:    os.Getenv("VELWATCH_DISABLED") == "true",
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
	return cfg
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
