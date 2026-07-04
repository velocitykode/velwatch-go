package velwatch

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	eventsv1 "github.com/velocitykode/velwatch-go/proto/events/v1"
)

// Transport handles sending events to the Velwatch backend via gRPC
type Transport struct {
	conn      *grpc.ClientConn
	client    eventsv1.EventServiceClient
	token     string
	release   string
	commitSHA string
}

// NewTransport creates a new gRPC transport
func NewTransport(endpoint, token string, insecureMode bool) (*Transport, error) {
	var opts []grpc.DialOption

	if insecureMode {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")))
	}

	// Set up connection timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Velwatch: %w", err)
	}

	client := eventsv1.NewEventServiceClient(conn)

	return &Transport{
		conn:   conn,
		client: client,
		token:  token,
	}, nil
}

// Export sends a batch of events. It satisfies the Exporter interface,
// delegating to Send.
func (t *Transport) Export(events []*Event) error {
	return t.Send(events)
}

// Send sends a batch of events to the Velwatch backend
func (t *Transport) Send(events []*Event) error {
	if len(events) == 0 {
		return nil
	}

	// Convert to proto events
	protoEvents := make([]*eventsv1.Event, 0, len(events))
	for _, e := range events {
		// Stamp resolved release/commit metadata without clobbering caller tags.
		e.setDefaultTag(tagRelease, t.release)
		e.setDefaultTag(tagCommitSHA, t.commitSHA)

		protoEvent := &eventsv1.Event{
			Type:           e.Type,
			TimestampMs:    e.Timestamp.UnixMilli(),
			TraceId:        e.TraceID,
			SpanId:         e.SpanID,
			ParentId:       e.ParentID,
			AttributesJson: e.AttributesJSON(),
			Tags:           e.Tags,
		}
		protoEvents = append(protoEvents, protoEvent)
	}

	// Create request
	req := &eventsv1.IngestRequest{
		Events: protoEvents,
	}

	// Add auth metadata
	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"authorization", "Bearer "+t.token,
	)

	// Set timeout
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Send request
	resp, err := t.client.Ingest(ctx, req)
	if err != nil {
		log.Printf("velwatch: failed to send events: %v", err)
		return err
	}

	if !resp.Success && resp.Rejected > 0 {
		log.Printf("velwatch: %d events rejected", resp.Rejected)
	}

	return nil
}

// Close closes the gRPC connection
func (t *Transport) Close() error {
	if t.conn != nil {
		return t.conn.Close()
	}
	return nil
}
