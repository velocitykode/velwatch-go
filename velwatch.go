// Package velwatch provides automatic instrumentation for Velocity applications.
// It captures requests, database queries, cache operations, and exceptions,
// sending them to the Velwatch APM service for monitoring and analysis.
//
// Basic usage:
//
//	import "github.com/velwatch/sdk-go"
//
//	func main() {
//	    err := velwatch.Init(velwatch.Config{
//	        Endpoint:    "velwatch.example.com:50051",
//	        Token:       os.Getenv("VELWATCH_TOKEN"),
//	        ServiceName: "my-api",
//	    })
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    defer velwatch.Shutdown()
//
//	    // Your Velocity app code
//	}
package velwatch

import (
	"context"
	"errors"
	"sync"
	"time"
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
}

// Init initializes the Velwatch SDK with the given configuration.
// It should be called once at application startup.
func Init(config Config) error {
	mu.Lock()
	defer mu.Unlock()

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

	// Create and register event listeners
	listeners := NewListeners(collector, config.ServiceName, config.SampleRate)

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
// It should be called when the application is shutting down.
func Shutdown() error {
	mu.Lock()
	defer mu.Unlock()

	if instance == nil {
		return nil
	}

	sdk := instance
	instance = nil

	if sdk.config.Disabled {
		return nil
	}

	// Cancel context to stop flush loop
	sdk.cancel()

	// Wait for flush loop to finish
	sdk.wg.Wait()

	// Final flush
	if sdk.collector != nil {
		sdk.collector.Flush()
	}

	// Close transport
	if sdk.transport != nil {
		return sdk.transport.Close()
	}

	// Unregister listeners
	if sdk.listeners != nil {
		sdk.listeners.Unregister()
	}

	return nil
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
