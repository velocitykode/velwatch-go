package velwatch

import (
	"strings"
	"testing"

	"github.com/velocitykode/velocity/events"
)

// initForTest runs the shared initialization against a throwaway dispatcher
// and tears the resulting instance down when the test ends.
func initForTest(t *testing.T, config Config) (*SDK, error) {
	t.Helper()

	mu.Lock()
	err := initLocked(events.NewDispatcher(), config)
	sdk := instance
	mu.Unlock()

	t.Cleanup(func() { _ = Shutdown() })
	return sdk, err
}

// testConfig is a minimally valid config pointed at a local OTLP receiver.
func testConfig() Config {
	return Config{
		Endpoint:    "localhost:4317",
		Token:       "vw_test",
		ServiceName: "test-service",
		Insecure:    true,
	}
}

func TestInitDefaultsToOTLPExporter(t *testing.T) {
	config := testConfig() // Protocol left unset

	sdk, err := initForTest(t, config)
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}
	if sdk.config.Protocol != protocolOTLP {
		t.Errorf("Protocol = %q, want %q", sdk.config.Protocol, protocolOTLP)
	}
	if _, ok := sdk.exporter.(*OTLPExporter); !ok {
		t.Errorf("exporter = %T, want *OTLPExporter", sdk.exporter)
	}
}

func TestInitOTLPHTTPExporter(t *testing.T) {
	config := testConfig()
	config.Protocol = protocolOTLPHTTP
	config.Endpoint = "localhost:4318"

	sdk, err := initForTest(t, config)
	if err != nil {
		t.Fatalf("initLocked returned error: %v", err)
	}
	if _, ok := sdk.exporter.(*OTLPHTTPExporter); !ok {
		t.Errorf("exporter = %T, want *OTLPHTTPExporter", sdk.exporter)
	}
}

func TestInitLegacyProtocolFails(t *testing.T) {
	config := testConfig()
	config.Protocol = legacyProtocol
	config.Endpoint = "localhost:4317"

	_, err := initForTest(t, config)
	if err == nil {
		t.Fatal("expected an error for the removed protocol, got nil")
	}
	// The error has to name the migration, not just reject the value.
	for _, want := range []string{legacyProtocol, "removed", "VELWATCH_PROTOCOL=" + protocolOTLP, protocolOTLPHTTP} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
	if IsInitialized() {
		t.Error("SDK should not be initialized after a removed-protocol error")
	}
}

// The removed wire is rejected on its own port too, where the endpoint check
// would otherwise be the first thing to fire.
func TestInitLegacyProtocolOnLegacyPortFails(t *testing.T) {
	config := testConfig()
	config.Protocol = legacyProtocol
	config.Endpoint = "localhost:50051"

	_, err := initForTest(t, config)
	if err == nil {
		t.Fatal("expected an error for the removed protocol, got nil")
	}
	if !strings.Contains(err.Error(), "removed") {
		t.Errorf("error %q should name the removal", err)
	}
}

func TestInitUnknownProtocolFails(t *testing.T) {
	config := testConfig()
	config.Protocol = "bogus"

	if _, err := initForTest(t, config); err == nil {
		t.Fatal("expected an error for an unknown protocol, got nil")
	} else {
		for _, want := range []string{"bogus", protocolOTLP, protocolOTLPHTTP} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q should mention %q", err, want)
			}
		}
	}
	if IsInitialized() {
		t.Error("SDK should not be initialized after a protocol error")
	}
}

func TestInitLegacyEndpointWithOTLPFails(t *testing.T) {
	config := testConfig()
	config.Endpoint = "ingest.velwatch.com:50051" // legacy port, OTLP default

	_, err := initForTest(t, config)
	if err == nil {
		t.Fatal("expected an error for a legacy endpoint under OTLP, got nil")
	}
	for _, want := range []string{"50051", defaultOTLPPort, protocolOTLP} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
}

func TestValidateEndpoint(t *testing.T) {
	cases := []struct {
		endpoint string
		wantErr  bool
	}{
		{"localhost:4317", false},
		{"ingest.velwatch.com", false},
		{"localhost:50051", true},
		{"https://ingest.velwatch.com:50051/v1/traces", true},
		{"https://ingest.velwatch.com:4318/v1/traces", false},
	}
	for _, c := range cases {
		err := validateEndpoint(c.endpoint)
		if (err != nil) != c.wantErr {
			t.Errorf("validateEndpoint(%q) error = %v, wantErr %v", c.endpoint, err, c.wantErr)
		}
	}
}

func TestEndpointPort(t *testing.T) {
	cases := []struct{ endpoint, want string }{
		{"localhost:4317", "4317"},
		{"ingest.velwatch.com", ""},
		{"https://ingest.velwatch.com:4318/v1/traces", "4318"},
		{"http://user:pass@host:50051/path", "50051"},
		{"[::1]:4317", "4317"},
		{"https://ingest.velwatch.com/v1/traces", ""},
	}
	for _, c := range cases {
		if got := endpointPort(c.endpoint); got != c.want {
			t.Errorf("endpointPort(%q) = %q, want %q", c.endpoint, got, c.want)
		}
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	for _, k := range []string{"VELWATCH_ENDPOINT", "VELWATCH_PROTOCOL"} {
		t.Setenv(k, "")
	}

	cfg := configFromEnv()
	// The endpoint default is per-protocol, so configFromEnv leaves it empty
	// and initialization fills it in once the protocol is resolved.
	if cfg.Endpoint != "" {
		t.Errorf("Endpoint = %q, want empty", cfg.Endpoint)
	}
	if cfg.Protocol != protocolOTLP {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, protocolOTLP)
	}
}

func TestDefaultEndpointIsPerProtocol(t *testing.T) {
	cases := []struct{ protocol, want string }{
		{protocolOTLP, "localhost:4317"},
		{protocolOTLPHTTP, "localhost:4318"},
	}
	for _, c := range cases {
		if got := defaultEndpointFor(c.protocol); got != c.want {
			t.Errorf("defaultEndpointFor(%q) = %q, want %q", c.protocol, got, c.want)
		}
	}
}

func TestInitAppliesPerProtocolDefaultEndpoint(t *testing.T) {
	cases := []struct{ protocol, want string }{
		{"", "localhost:4317"}, // unset protocol resolves to otlp
		{protocolOTLP, "localhost:4317"},
		{protocolOTLPHTTP, "localhost:4318"},
	}
	for _, c := range cases {
		t.Run(c.protocol, func(t *testing.T) {
			config := testConfig()
			config.Protocol = c.protocol
			config.Endpoint = "" // no VELWATCH_ENDPOINT configured

			sdk, err := initForTest(t, config)
			if err != nil {
				t.Fatalf("initLocked returned error: %v", err)
			}
			if sdk.config.Endpoint != c.want {
				t.Errorf("Endpoint = %q, want %q", sdk.config.Endpoint, c.want)
			}
		})
	}
}
