package velwatch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
)

// OTLPHTTPExporter ships events as OpenTelemetry spans over OTLP/HTTP using
// protobuf encoding (Content-Type application/x-protobuf). HTTP is the
// transport for runtimes where gRPC is impractical: browsers, serverless and
// edge functions, React Native, and Next.js. Same wire model and Bearer auth
// as the gRPC exporter.
type OTLPHTTPExporter struct {
	url         string
	token       string
	serviceName string
	release     string
	commitSHA   string
	client      *http.Client
}

// NewOTLPHTTPExporter builds an HTTP exporter. endpoint may be a base URL
// (https://host:4318) or a full traces URL; the standard /v1/traces path is
// appended when absent. A bare host[:port] is assumed https.
func NewOTLPHTTPExporter(endpoint, token, serviceName string, insecureMode bool) (*OTLPHTTPExporter, error) {
	url := normalizeTracesURL(endpoint, insecureMode)
	return &OTLPHTTPExporter{
		url:         url,
		token:       token,
		serviceName: serviceName,
		client:      &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// normalizeTracesURL coerces an endpoint into a full OTLP/HTTP traces URL.
func normalizeTracesURL(endpoint string, insecureMode bool) string {
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		scheme := "https://"
		if insecureMode {
			scheme = "http://"
		}
		endpoint = scheme + endpoint
	}
	if !strings.Contains(endpoint, "/v1/traces") {
		endpoint = strings.TrimRight(endpoint, "/") + "/v1/traces"
	}
	return endpoint
}

// Export marshals events to OTLP protobuf and POSTs them to the traces endpoint.
func (e *OTLPHTTPExporter) Export(events []*Event) error {
	if len(events) == 0 {
		return nil
	}

	body, err := proto.Marshal(buildExportRequest(events, e.serviceName, e.release, e.commitSHA))
	if err != nil {
		log.Printf("velwatch: failed to marshal OTLP request: %v", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Authorization", "Bearer "+e.token)

	resp, err := e.client.Do(req)
	if err != nil {
		log.Printf("velwatch: failed to export OTLP/HTTP spans: %v", err)
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 300 {
		err := fmt.Errorf("velwatch: OTLP/HTTP export returned status %d", resp.StatusCode)
		log.Print(err)
		return err
	}
	return nil
}

// Close is a no-op; the HTTP client holds no persistent connection to release.
func (e *OTLPHTTPExporter) Close() error { return nil }
