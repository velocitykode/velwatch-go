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
//	                        "otlphttp", "localhost:50051" for "grpc")
//	VELWATCH_SERVICE_NAME   service name in traces (default APP_NAME)
//	VELWATCH_PROTOCOL       wire protocol: "otlp" (OTLP/gRPC), "otlphttp"
//	                        (OTLP/HTTP), or "grpc" (deprecated legacy
//	                        EventService wire) (default "otlp")
//	VELWATCH_INSECURE       "true" disables TLS for local development
//	VELWATCH_SAMPLE_RATE    fraction of requests traced, 0.0-1.0 (default 1.0)
//	VELWATCH_BATCH_SIZE     events per batch (default 100)
//	VELWATCH_FLUSH_INTERVAL flush cadence, e.g. "2s" (default 1s)
//	VELWATCH_DISABLED       "true" disables the SDK entirely
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
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/velocitykode/velocity"
	"github.com/velocitykode/velocity/contract"
)

// Wire protocols accepted by Config.Protocol and VELWATCH_PROTOCOL. OTLP is
// the ingest contract going forward; protocolGRPC selects the deprecated
// first-party EventService wire and is kept only for existing deployments.
const (
	protocolOTLP     = "otlp"
	protocolOTLPHTTP = "otlphttp"
	protocolGRPC     = "grpc"
)

const (
	// defaultOTLPPort is the standard OTLP/gRPC receiver port.
	defaultOTLPPort = "4317"

	// defaultOTLPHTTPPort is the standard OTLP/HTTP receiver port.
	defaultOTLPHTTPPort = "4318"

	// legacyGRPCPort is the port the deprecated EventService listens on.
	// An endpoint on this port paired with an OTLP protocol is a misconfig.
	legacyGRPCPort = "50051"
)

// defaultEndpointFor returns the local endpoint used when none is configured.
// The default is per-protocol: each wire listens on its own standard port, so
// a protocol chosen without an endpoint still reaches the right receiver.
func defaultEndpointFor(protocol string) string {
	switch protocol {
	case protocolOTLPHTTP:
		return "localhost:" + defaultOTLPHTTPPort
	case protocolGRPC:
		return "localhost:" + legacyGRPCPort
	default:
		return "localhost:" + defaultOTLPPort
	}
}

// Config contains configuration options for the Velwatch SDK
type Config struct {
	// Endpoint is the Velwatch ingest endpoint. For the OTLP protocols this
	// is the OTLP receiver (e.g., "velwatch.example.com:4317", or a URL for
	// "otlphttp"); for the deprecated "grpc" protocol it is the EventService
	// address (e.g., "velwatch.example.com:50051"). When empty it defaults to
	// the local receiver for the selected protocol: "localhost:4317" for
	// "otlp", "localhost:4318" for "otlphttp", "localhost:50051" for "grpc".
	Endpoint string

	// Token is the project API token (e.g., "vw_xxx...")
	Token string

	// ServiceName identifies this service in traces
	ServiceName string

	// BatchSize is the number of events to batch before sending (default: 100)
	BatchSize int

	// FlushInterval is how often to flush batched events (default: 1s)
	FlushInterval time.Duration

	// Protocol selects the wire format: "otlp" for OpenTelemetry OTLP/gRPC,
	// "otlphttp" for OTLP/HTTP, or "grpc" for the deprecated legacy Velwatch
	// EventService proto. Default: "otlp".
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
	exporter  Exporter
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
	if err := validateEndpoint(config.Protocol, config.Endpoint); err != nil {
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
	case protocolGRPC:
		warnLegacyProtocol()
		exp, err := NewTransport(config.Endpoint, config.Token, config.Insecure)
		if err != nil {
			return nil, err
		}
		exp.release, exp.commitSHA = config.Release, config.CommitSHA
		return exp, nil
	default:
		return nil, fmt.Errorf("velwatch: unknown protocol %q (valid values: %q, %q, %q)",
			config.Protocol, protocolOTLP, protocolOTLPHTTP, protocolGRPC)
	}
}

// legacyProtocolOnce keeps the deprecation notice to a single line per
// process, however many times the SDK is initialized.
var legacyProtocolOnce sync.Once

// warnLegacyProtocol logs the EventService deprecation notice once.
func warnLegacyProtocol() {
	legacyProtocolOnce.Do(func() {
		log.Printf("velwatch: protocol %q selects the legacy EventService wire, which is deprecated "+
			"and is scheduled for removal in the next major version. Unset VELWATCH_PROTOCOL to use "+
			"the OTLP default, and point VELWATCH_ENDPOINT at the OTLP receiver port %s.",
			protocolGRPC, defaultOTLPPort)
	})
}

// validateEndpoint rejects an endpoint that clearly belongs to the deprecated
// EventService wire while an OTLP protocol is selected. OTLP became the
// default, so an upgraded deployment that still points at the legacy port
// would otherwise fail silently at export time instead of at startup.
func validateEndpoint(protocol, endpoint string) error {
	if protocol == protocolGRPC {
		return nil
	}
	if endpointPort(endpoint) != legacyGRPCPort {
		return nil
	}
	return fmt.Errorf("velwatch: endpoint %q uses the legacy EventService port %s but protocol %q is "+
		"selected; point the endpoint at the OTLP receiver (port %s) or set the protocol to %q to keep "+
		"the deprecated wire",
		endpoint, legacyGRPCPort, protocol, defaultOTLPPort, protocolGRPC)
}

// endpointPort extracts the port from an endpoint, which may be a bare
// host:port or a full URL. It returns "" when the endpoint carries no port.
func endpointPort(endpoint string) string {
	hostport := endpoint
	if i := strings.Index(hostport, "://"); i >= 0 {
		hostport = hostport[i+3:]
	}
	if i := strings.IndexAny(hostport, "/?#"); i >= 0 {
		hostport = hostport[:i]
	}
	if i := strings.LastIndex(hostport, "@"); i >= 0 {
		hostport = hostport[i+1:]
	}
	_, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return ""
	}
	return port
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
