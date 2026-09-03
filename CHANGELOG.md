# Changelog

All notable changes to the Velwatch Go SDK are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-09-04

This release removes a wire protocol and is therefore a breaking change: it
ships as the next minor version, this being a pre-1.0 module.

### Added

- **Log capture through a velocity log driver.** The SDK registers a `velwatch`
  driver with velocity's log registry. Add it to the app's stack channel
  (`LOG_DRIVER=stack`, `LOG_STACK=console,velwatch`) and set
  `VELWATCH_LOG_CAPTURE=true`: every line the application already writes
  through velocity `log` ships as an OTLP log record, with no call-site
  changes. Lines are traceless: they carry the service, timestamp, level,
  message and attributes, never a trace or span id. `VELWATCH_LOG_LEVEL` sets
  the lowest level shipped and `VELWATCH_LOG_MAX_PER_SECOND` caps the volume
  per process; refusals are counted by `LogsDroppedRate()`.

### Breaking

- **The legacy first-party ingest transport is removed.** The exported
  `Transport` type and `NewTransport` constructor, the first-party gRPC client,
  its generated protobuf package (`proto/events/v1`) and the `grpc` value for
  `VELWATCH_PROTOCOL` are gone; the server no longer serves that wire.
  `VELWATCH_PROTOCOL=grpc` now fails initialization with an error naming the
  replacement instead of building an exporter.
  Migration: set `VELWATCH_PROTOCOL=otlp` (or leave it unset, since OTLP is the
  default) and point `VELWATCH_ENDPOINT` at the OTLP receiver on port `4317`.
  No code change is required.

### Changed

- **The default wire protocol is now `otlp` (OTLP/gRPC).** With
  `VELWATCH_PROTOCOL` unset the SDK builds the OTLP/gRPC exporter. `otlphttp`
  remains selectable and is unchanged.
- **`VELWATCH_ENDPOINT` now defaults per protocol** instead of always
  `localhost:50051`: `localhost:4317` for `otlp` and `localhost:4318` for
  `otlphttp` (the standard OTLP/HTTP receiver port).
- An unrecognized `VELWATCH_PROTOCOL` now fails initialization with an error
  listing the valid values (`otlp`, `otlphttp`) instead of silently falling
  back to a wire that was not asked for.
- **Event tags now export as `velwatch.tag.<key>` span attributes.** Both OTLP
  exporters (`otlp` over gRPC and `otlphttp`) share the span builder, so a tag
  set with `WithTag("team", "billing")` crosses the wire as
  `velwatch.tag.team=billing` instead of a flat `team` attribute. The platform
  promotes prefixed attributes into the record's tags column and removes them
  from the attributes JSON, so caller tags can no longer collide with a
  semantic-convention attribute key. A tag key that already starts with
  `velwatch.tag.` is passed through unchanged rather than prefixed twice, and an
  empty tag key is dropped.

### Deprecated

- The reserved tags `release` and `commit_sha` are still emitted as flat span
  attributes in addition to their `velwatch.tag.release` and
  `velwatch.tag.commit_sha` forms, so platform builds predating the prefix
  contract keep resolving them. The flat copies are kept for one release only
  and are removed after that; read the prefixed keys.

### Migration

Deployments on the removed wire move to OTLP by dropping the protocol override
and pointing the endpoint at the OTLP receiver:

```bash
# before
VELWATCH_PROTOCOL=grpc
VELWATCH_ENDPOINT=velwatch.example.com:50051

# after
VELWATCH_ENDPOINT=velwatch.example.com:4317
```

Deployments already on OTLP need no change.
