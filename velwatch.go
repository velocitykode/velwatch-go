// Package velwatch provides automatic instrumentation for Velocity applications.
// It captures requests, database queries, cache operations, and exceptions,
// sending them to the Velwatch APM service for monitoring and analysis.
//
// Usage: add a blank import and set VELWATCH_TOKEN. No initialization code
// is needed; the SDK auto-initializes during velocity.New() and is flushed
// and closed by App.Shutdown.
//
//	import (
//	    _ "github.com/velocitykode/velwatch-go"
//	)
//
// Environment variables:
//
//	VELWATCH_TOKEN          project API token (required; unset = SDK dormant)
//	VELWATCH_ENDPOINT       gRPC ingest endpoint (default "localhost:50051")
//	VELWATCH_SERVICE_NAME   service name in traces (default APP_NAME)
//	VELWATCH_INSECURE       "true" disables TLS for local development
//	VELWATCH_SAMPLE_RATE    fraction of requests traced, 0.0-1.0 (default 1.0)
//	VELWATCH_BATCH_SIZE     events per batch (default 100)
//	VELWATCH_FLUSH_INTERVAL flush cadence, e.g. "2s" (default 1s)
//	VELWATCH_DISABLED       "true" disables the SDK entirely
//
// For programmatic configuration, call Init(app, Config) explicitly instead.
package velwatch

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/velocitykode/velocity"
	"github.com/velocitykode/velocity/contract"
)

// Config contains configuration options for the Velwatch SDK
type Config struct {
	// Endpoint is the Velwatch gRPC endpoint (e.g., "velwatch.example.com:50051")
	Endpoint string

	// Token is the project API token (e.g., "vw_xxx...")
	Token string

	// ServiceName identifies this service in traces
	ServiceName string

	// BatchSize is the number of events to batch before sending (default: 100)
	BatchSize int

	// FlushInterval is how often to flush batched events (default: 1s)
	FlushInterval time.Duration

	// Insecure disables TLS for local development
	Insecure bool

	// Disabled completely disables the SDK (useful for testing)
	Disabled bool

	// SampleRate is the percentage of requests to trace (0.0-1.0, default: 1.0)
	SampleRate float64
}

var (
	instance *SDK
	once     sync.Once
	mu       sync.Mutex

	// ErrNotInitialized is returned when the SDK is used before Init()
	ErrNotInitialized = errors.New("velwatch: SDK not initialized")

	// ErrAlreadyInitialized is returned when Init() is called multiple times
	ErrAlreadyInitialized = errors.New("velwatch: SDK already initialized")
)

// SDK is the main Velwatch SDK instance
type SDK struct {
	config    Config
	collector *Collector
	transport *Transport
	listeners *Listeners
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// Init initializes the Velwatch SDK against a constructed Velocity app.
// Most applications do not need to call this: a blank import of this
// package plus the VELWATCH_* environment variables auto-initializes the
// SDK during velocity.New() (see autoinit.go). Init is the programmatic
// escape hatch for explicit configuration.
func Init(app *velocity.App, config Config) error {
	mu.Lock()
	defer mu.Unlock()

	var dispatcher contract.Dispatcher
	if app != nil {
		dispatcher = app.Services.Events
	}
	return initLocked(dispatcher, config)
}

// initLocked performs the shared initialization. mu must be held.
func initLocked(dispatcher contract.Dispatcher, config Config) error {
	if instance != nil {
		return ErrAlreadyInitialized
	}

	// Apply defaults
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = time.Second
	}
	if config.SampleRate <= 0 || config.SampleRate > 1 {
		config.SampleRate = 1.0
	}

	// If disabled, create a no-op instance
	if config.Disabled {
		instance = &SDK{config: config}
		return nil
	}

	// Validate required fields
	if dispatcher == nil {
		return errors.New("velwatch: a Velocity app with an event dispatcher is required")
	}
	if config.Endpoint == "" {
		return errors.New("velwatch: Endpoint is required")
	}
	if config.Token == "" {
		return errors.New("velwatch: Token is required")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create transport
	transport, err := NewTransport(config.Endpoint, config.Token, config.Insecure)
	if err != nil {
		cancel()
		return err
	}

	// Create collector
	collector := NewCollector(transport, config.BatchSize, config.FlushInterval)

	// Create and register event listeners on the app's dispatcher
	listeners := NewListeners(collector, dispatcher, config.ServiceName, config.SampleRate)

	sdk := &SDK{
		config:    config,
		collector: collector,
		transport: transport,
		listeners: listeners,
		ctx:       ctx,
		cancel:    cancel,
	}

	// Start background flush goroutine
	sdk.wg.Add(1)
	go sdk.flushLoop()

	// Register Velocity event listeners
	listeners.Register()

	instance = sdk
	return nil
}

// Shutdown gracefully shuts down the SDK, flushing any remaining events.
// Auto-initialized apps do not need to call this: the SDK registers itself
// as an app component and App.Shutdown's ShutdownAware sweep closes it.
func Shutdown() error {
	mu.Lock()
	sdk := instance
	instance = nil
	mu.Unlock()

	if sdk == nil {
		return nil
	}
	return sdk.close()
}

// Shutdown flushes remaining events and closes the transport. It satisfies
// contract.ShutdownAware so the SDK can live in the app component registry
// and be torn down by App.Shutdown without consumer code. Idempotent; safe
// to combine with the package-level Shutdown().
func (sdk *SDK) Shutdown(ctx context.Context) error {
	mu.Lock()
	if instance == sdk {
		instance = nil
	}
	mu.Unlock()
	return sdk.close()
}

// close performs the actual teardown exactly once.
func (sdk *SDK) close() error {
	sdk.closeOnce.Do(func() {
		if sdk.config.Disabled {
			return
		}

		// Unregister listeners first so no new events arrive during teardown
		if sdk.listeners != nil {
			sdk.listeners.Unregister()
		}

		// Stop the flush loop and wait for it to finish
		if sdk.cancel != nil {
			sdk.cancel()
		}
		sdk.wg.Wait()

		// Final flush
		if sdk.collector != nil {
			sdk.collector.Flush()
		}

		// Close transport
		if sdk.transport != nil {
			sdk.closeErr = sdk.transport.Close()
		}
	})
	return sdk.closeErr
}

func (sdk *SDK) flushLoop() {
	defer sdk.wg.Done()

	ticker := time.NewTicker(sdk.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sdk.collector.Flush()
		case <-sdk.ctx.Done():
			return
		}
	}
}

// IsInitialized returns true if the SDK has been initialized
func IsInitialized() bool {
	mu.Lock()
	defer mu.Unlock()
	return instance != nil
}

// RecordEvent manually records a custom event.
// This is useful for instrumenting code that isn't automatically captured.
func RecordEvent(event *Event) {
	mu.Lock()
	sdk := instance
	mu.Unlock()

	if sdk == nil || sdk.config.Disabled || sdk.collector == nil {
		return
	}

	sdk.collector.Add(event)
}

// CurrentTraceID returns the current trace ID from context, if any.
func CurrentTraceID(ctx context.Context) string {
	return GetTraceID(ctx)
}

// CurrentSpanID returns the current span ID from context, if any.
func CurrentSpanID(ctx context.Context) string {
	return GetSpanID(ctx)
}
