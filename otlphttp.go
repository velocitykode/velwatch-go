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
	logsURL     string
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
	return &OTLPHTTPExporter{
		url:         normalizeTracesURL(endpoint, insecureMode),
		logsURL:     normalizeLogsURL(endpoint, insecureMode),
		token:       token,
		serviceName: serviceName,
		client:      &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// OTLP/HTTP signal paths. Each signal has its own route on the same receiver.
const (
	otlpTracesPath = "/v1/traces"
	otlpLogsPath   = "/v1/logs"
)

// normalizeTracesURL coerces an endpoint into a full OTLP/HTTP traces URL.
func normalizeTracesURL(endpoint string, insecureMode bool) string {
	return normalizeSignalURL(endpoint, otlpTracesPath, insecureMode)
}

// normalizeLogsURL coerces an endpoint into a full OTLP/HTTP logs URL. An
// endpoint configured as a full traces URL still resolves the logs route
// beside it, so one VELWATCH_ENDPOINT covers both signals.
func normalizeLogsURL(endpoint string, insecureMode bool) string {
	return normalizeSignalURL(endpoint, otlpLogsPath, insecureMode)
}

// normalizeSignalURL coerces an endpoint into a full OTLP/HTTP URL for one
// signal. endpoint may be a base URL (https://host:4318), a bare host[:port]
// (assumed https unless insecureMode), or a full URL for either signal, whose
// path is replaced with the requested one.
func normalizeSignalURL(endpoint, path string, insecureMode bool) string {
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		scheme := "https://"
		if insecureMode {
			scheme = "http://"
		}
		endpoint = scheme + endpoint
	}
	for _, signal := range []string{otlpTracesPath, otlpLogsPath} {
		if i := strings.Index(endpoint, signal); i >= 0 {
			endpoint = endpoint[:i]
			break
		}
	}
	return strings.TrimRight(endpoint, "/") + path
}

// Export marshals events to OTLP protobuf and POSTs them to the traces
// endpoint, one request per batch of at most maxRecordsPerExport spans.
func (e *OTLPHTTPExporter) Export(events []*Event) error {
	if len(events) == 0 {
		return nil
	}

	var firstErr error
	for _, chunk := range chunkEvents(events, maxRecordsPerExport) {
		req := buildExportRequest(chunk, e.serviceName, e.release, e.commitSHA)
		if err := e.post(e.url, req, "spans"); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ExportLogRecords marshals log events to OTLP protobuf and POSTs them to the
// logs endpoint, one request per batch of at most maxRecordsPerExport records.
// It uses the same client, content type, auth header and encoding as the
// traces route, so both signals reach the receiver the same way. It satisfies
// LogRecordExporter, which is what makes the collector route captured log
// lines here instead of counting them dropped.
func (e *OTLPHTTPExporter) ExportLogRecords(events []*Event) error {
	if len(events) == 0 {
		return nil
	}

	var firstErr error
	for _, chunk := range chunkEvents(events, maxRecordsPerExport) {
		req := buildExportLogsRequest(chunk, e.serviceName, e.release, e.commitSHA)
		if err := e.post(e.logsURL, req, "log records"); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// post marshals an OTLP request and POSTs it to url. what names the payload in
// the log line an export failure produces.
func (e *OTLPHTTPExporter) post(url string, message proto.Message, what string) error {
	body, err := proto.Marshal(message)
	if err != nil {
		log.Printf("velwatch: failed to marshal OTLP %s request: %v", what, err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Authorization", "Bearer "+e.token)

	resp, err := e.client.Do(req)
	if err != nil {
		log.Printf("velwatch: failed to export OTLP/HTTP %s: %v", what, err)
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 300 {
		err := fmt.Errorf("velwatch: OTLP/HTTP %s export returned status %d", what, resp.StatusCode)
		log.Print(err)
		return err
	}
	return nil
}

// Close is a no-op; the HTTP client holds no persistent connection to release.
func (e *OTLPHTTPExporter) Close() error { return nil }
