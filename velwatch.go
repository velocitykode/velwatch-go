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
//	VELWATCH_ENDPOINT       ingest endpoint (default per protocol:
//	                        "localhost:4317" for "otlp", "localhost:4318" for
//	                        "otlphttp")
//	VELWATCH_SERVICE_NAME   service name in traces (default APP_NAME)
//	VELWATCH_PROTOCOL       wire protocol: "otlp" (OTLP/gRPC) or "otlphttp"
//	                        (OTLP/HTTP) (default "otlp")
//	VELWATCH_INSECURE       "true" disables TLS for local development
//	VELWATCH_SAMPLE_RATE    fraction of requests traced, 0.0-1.0 (default 1.0)
//	VELWATCH_BATCH_SIZE     events per batch (default 100)
//	VELWATCH_FLUSH_INTERVAL flush cadence, e.g. "2s" (default 1s)
//	VELWATCH_DISABLED       "true" disables the SDK entirely
//	VELWATCH_LOG_CAPTURE    "true" captures log lines emitted during a span
//	                        (default off: slog is left untouched)
//	VELWATCH_LOG_SLOW_THRESHOLD
//	                        a span at least this slow keeps every line it
//	                        logged, e.g. "750ms" (default "1s")
//	VELWATCH_LOG_MAX_LINES  log lines one span buffers before only error
//	                        lines are still captured (default 50)
//	VELWATCH_LOG_LEVEL      lowest level captured: "debug", "info", "warn" or
//	                        "error" (default "info"; the application's own
//	                        logging is unaffected)
//	VELWATCH_RELEASE        deployed service version (OTLP service.version)
//	VELWATCH_COMMIT_SHA     VCS revision (OTLP vcs.ref.head.revision)
//
// VELWATCH_RELEASE and VELWATCH_COMMIT_SHA also fall back to the standard
// OTEL_RESOURCE_ATTRIBUTES keys service.version and vcs.ref.head.revision.
//
// For programmatic configuration, call Init(app, Config) explicitly instead.
package velwatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/velocitykode/velocity"
	"github.com/velocitykode/velocity/contract"
)

// Wire protocols accepted by Config.Protocol and VELWATCH_PROTOCOL. OTLP is
// the ingest contract: the two values differ only in transport.
const (
	protocolOTLP     = "otlp"
	protocolOTLPHTTP = "otlphttp"

	// legacyProtocol is the removed first-party wire. It stays recognized so
	// a deployment still carrying it fails with the migration message rather
	// than the generic unknown-protocol error.
	legacyProtocol = "grpc"
)

const (
	// defaultOTLPPort is the standard OTLP/gRPC receiver port.
	defaultOTLPPort = "4317"

	// defaultOTLPHTTPPort is the standard OTLP/HTTP receiver port.
	defaultOTLPHTTPPort = "4318"
)

// defaultEndpointFor returns the local endpoint used when none is configured.
// The default is per-protocol: each wire listens on its own standard port, so
// a protocol chosen without an endpoint still reaches the right receiver.
func defaultEndpointFor(protocol string) string {
	switch protocol {
	case protocolOTLPHTTP:
		return "localhost:" + defaultOTLPHTTPPort
	default:
		return "localhost:" + defaultOTLPPort
	}
}

// Config contains configuration options for the Velwatch SDK
type Config struct {
	// Endpoint is the Velwatch ingest endpoint: the OTLP receiver
	// (e.g., "velwatch.example.com:4317", or a URL for "otlphttp"). When
	// empty it defaults to the local receiver for the selected protocol:
	// "localhost:4317" for "otlp", "localhost:4318" for "otlphttp".
	Endpoint string

	// Token is the project API token (e.g., "vw_xxx...")
	Token string

	// ServiceName identifies this service in traces
	ServiceName string

	// BatchSize is the number of events to batch before sending (default: 100)
	BatchSize int

	// FlushInterval is how often to flush batched events (default: 1s)
	FlushInterval time.Duration

	// Protocol selects the wire format: "otlp" for OpenTelemetry OTLP/gRPC
	// or "otlphttp" for OTLP/HTTP. Default: "otlp".
	Protocol string

	// Insecure disables TLS for local development
	Insecure bool

	// Disabled completely disables the SDK (useful for testing)
	Disabled bool

	// SampleRate is the percentage of requests to trace (0.0-1.0, default: 1.0)
	SampleRate float64

	// Release identifies the deployed version of the instrumented service
	// (e.g. "1.4.2" or a build tag). When set it is stamped on every event as
	// the "release" tag and emitted as the OTLP service.version resource
	// attribute. If empty it is resolved from VELWATCH_RELEASE, then from
	// service.version in OTEL_RESOURCE_ATTRIBUTES.
	Release string

	// CommitSHA identifies the VCS revision the service was built from. When
	// set it is stamped on every event as the "commit_sha" tag and emitted as
	// the OTLP vcs.ref.head.revision resource attribute. If empty it is
	// resolved from VELWATCH_COMMIT_SHA, then from vcs.ref.head.revision in
	// OTEL_RESOURCE_ATTRIBUTES.
	CommitSHA string

	// LogCapture installs an slog handler that captures log lines emitted
	// while a span is active and buffers them on that span. Off by default:
	// slog is left exactly as the application configured it. When on, the
	// current slog default handler is wrapped rather than replaced, so the
	// application's own logging keeps working.
	LogCapture bool

	// LogLevel is the lowest level captured onto a span. Lines below it are
	// never buffered and never exported; they are still handed to the
	// application's own slog handler, so the floor governs capture only and
	// never what the application logs. Refused lines are counted on
	// SpanLogs.DroppedByFloor and reported on the span's record as
	// "log.dropped". The zero value is slog.LevelInfo, which is the default.
	// Resolved from VELWATCH_LOG_LEVEL ("debug", "info", "warn", "error").
	LogLevel slog.Level

	// LogMaxLines is the most log lines one span will buffer. Once a span
	// reaches it, only error lines are still captured, so a span that logs
	// heavily and then fails still records the failure. Lines refused by the
	// cap are counted on SpanLogs.DroppedByCap and reported on the span's
	// record as "log.dropped". Zero selects the default of 50; a negative
	// value is rejected by initialization. Resolved from
	// VELWATCH_LOG_MAX_LINES, which must be a positive integer.
	LogMaxLines int

	// LogSlowThreshold is the duration above which a span is considered slow
	// and keeps every line it logged, whatever the trace's sampling verdict.
	// Zero selects the default of one second; a negative value is rejected
	// by initialization. Resolved from VELWATCH_LOG_SLOW_THRESHOLD.
	LogSlowThreshold time.Duration
}

// defaultLogMaxLines is the number of log lines one span buffers before only
// error lines are still taken. Fifty lines is a generous request-sized story
// while keeping the worst case a span can hold small and predictable.
const defaultLogMaxLines = 50

// defaultLogSlowThreshold is the span duration above which log capture keeps
// every buffered line. Healthy traffic is fast, so this is the line between
// "nothing interesting happened" and "worth the bytes".
const defaultLogSlowThreshold = time.Second

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
	exporter  Exporter
	listeners *Listeners
	logs      *logHandler
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
	if config.LogMaxLines < 0 {
		return fmt.Errorf("velwatch: LogMaxLines must not be negative, got %d", config.LogMaxLines)
	}
	if config.LogMaxLines == 0 {
		config.LogMaxLines = defaultLogMaxLines
	}
	if config.LogSlowThreshold < 0 {
		return fmt.Errorf("velwatch: LogSlowThreshold must not be negative, got %s", config.LogSlowThreshold)
	}
	if config.LogSlowThreshold == 0 {
		config.LogSlowThreshold = defaultLogSlowThreshold
	}
	if config.Protocol == "" {
		config.Protocol = protocolOTLP
	}
	// The endpoint default depends on the protocol, so it is resolved only
	// after the protocol is known.
	if config.Endpoint == "" {
		config.Endpoint = defaultEndpointFor(config.Protocol)
	}

	// Resolve release/commit metadata once, honoring explicit config over
	// VELWATCH_* env over OTEL_RESOURCE_ATTRIBUTES.
	config.Release, config.CommitSHA = resolveReleaseInfo(config.Release, config.CommitSHA)

	// If disabled, create a no-op instance
	if config.Disabled {
		instance = &SDK{config: config}
		return nil
	}

	// Validate required fields
	if dispatcher == nil {
		return errors.New("velwatch: a Velocity app with an event dispatcher is required")
	}
	if config.Token == "" {
		return errors.New("velwatch: Token is required")
	}
	if err := validateProtocol(config.Protocol); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create the exporter for the configured wire protocol
	exporter, err := newExporter(config)
	if err != nil {
		cancel()
		return err
	}

	// Create collector
	collector := NewCollector(exporter, config.BatchSize, config.FlushInterval)

	// Create and register event listeners on the app's dispatcher
	listeners := NewListeners(collector, dispatcher, config.ServiceName, config.SampleRate)

	sdk := &SDK{
		config:    config,
		collector: collector,
		exporter:  exporter,
		listeners: listeners,
		ctx:       ctx,
		cancel:    cancel,
	}

	// Start background flush goroutine
	sdk.wg.Add(1)
	go sdk.flushLoop()

	// Register Velocity event listeners
	listeners.Register()

	// Install log capture last, so a failed initialization never leaves a
	// handler wired into the application's logger.
	if config.LogCapture {
		sdk.logs = installLogCapture(config)
	}

	instance = sdk
	return nil
}

// newExporter builds the exporter for the configured wire protocol, threading
// the resolved release/commit metadata to it. An unknown protocol is an error
// rather than a silent fallback.
func newExporter(config Config) (Exporter, error) {
	switch config.Protocol {
	case protocolOTLP:
		exp, err := NewOTLPExporter(config.Endpoint, config.Token, config.ServiceName, config.Insecure)
		if err != nil {
			return nil, err
		}
		exp.release, exp.commitSHA = config.Release, config.CommitSHA
		return exp, nil
	case protocolOTLPHTTP:
		exp, err := NewOTLPHTTPExporter(config.Endpoint, config.Token, config.ServiceName, config.Insecure)
		if err != nil {
			return nil, err
		}
		exp.release, exp.commitSHA = config.Release, config.CommitSHA
		return exp, nil
	default:
		if err := validateProtocol(config.Protocol); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("velwatch: no exporter for protocol %q", config.Protocol)
	}
}

// validateProtocol rejects a protocol the SDK cannot ship events over. The
// removed first-party wire gets its own message so an upgraded deployment is
// told what to set instead of only that the value is unknown.
func validateProtocol(protocol string) error {
	switch protocol {
	case protocolOTLP, protocolOTLPHTTP:
		return nil
	case legacyProtocol:
		return fmt.Errorf("velwatch: protocol %q was removed; set VELWATCH_PROTOCOL=%s or leave it unset, "+
			"and point VELWATCH_ENDPOINT at the OTLP receiver (port %s). Valid values: %q, %q",
			legacyProtocol, protocolOTLP, defaultOTLPPort, protocolOTLP, protocolOTLPHTTP)
	default:
		return fmt.Errorf("velwatch: unknown protocol %q (valid values: %q, %q)",
			protocol, protocolOTLP, protocolOTLPHTTP)
	}
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

		// Restore the logger the application had before capture was installed
		if sdk.logs != nil {
			uninstallLogCapture()
			sdk.logs = nil
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

		// Close exporter
		if sdk.exporter != nil {
			sdk.closeErr = sdk.exporter.Close()
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
