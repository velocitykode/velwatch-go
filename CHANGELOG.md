# Changelog

All notable changes to the Velwatch Go SDK are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **The default wire protocol is now `otlp` (OTLP/gRPC).** With `VELWATCH_PROTOCOL`
  unset the SDK builds the OTLP/gRPC exporter instead of the legacy Velwatch
  `EventService` exporter. `otlphttp` remains selectable and is unchanged.
- **`VELWATCH_ENDPOINT` now defaults to `localhost:4317`** (the standard OTLP/gRPC
  receiver port) instead of `localhost:50051`.
- An unrecognized `VELWATCH_PROTOCOL` now fails initialization with an error
  listing the valid values (`otlp`, `otlphttp`, `grpc`) instead of silently
  falling back to the legacy wire.

### Deprecated

- `VELWATCH_PROTOCOL=grpc` selects the legacy Velwatch `EventService` wire. It
  still works, and now logs a deprecation warning once per process at startup.
  The wire will be removed in a future major version (VW-43).

### Added

- Initialization rejects an endpoint on the legacy port `50051` when an OTLP
  protocol is selected, with an explicit error naming both the OTLP receiver
  port and the `grpc` opt-back-in. This catches deployments that upgrade into
  the new default while still pointing at the legacy ingest port, which would
  otherwise fail silently at export time.

### Migration

Deployments that are not ready to move to OTLP keep the previous behavior by
setting both values explicitly:

```bash
VELWATCH_PROTOCOL=grpc
VELWATCH_ENDPOINT=velwatch.example.com:50051
```

Deployments moving to OTLP only need to point `VELWATCH_ENDPOINT` at the OTLP
receiver (port 4317); no code change is required.
