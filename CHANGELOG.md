# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-02-25

### Added
- Auto-geração de `span_id` quando não fornecido manualmente (16 caracteres hex)
- `span_id` agora é sempre incluído nos logs
- Persistência de IDs gerados automaticamente no contexto global
- Filtragem automática de valores `null`/`undefined` do campo `context`

### Changed
- `trace_id` agora usa `correlation_id` como fallback (antes gerava UUID aleatório)
- Middleware Express: `trace_id` reutiliza `correlation_id` por padrão
- IDs gerados automaticamente (`correlation_id`, `trace_id`, `span_id`) são mantidos entre chamadas de log

### Fixed
- IDs gerados automaticamente agora são persistidos no contexto e reutilizados
- Context não envia mais valores nulos/indefinidos ao backend
- `span_id` nunca mais será `null` nos logs
- `trace_id` agora segue o padrão de distributed tracing (fallback para `correlation_id`)

## [0.1.4] - 2026-02-15

### Fixed
- Captura de module, file e line agora captura corretamente da aplicação, não do SDK
- Stack trace é capturado no método `log` antes de chamar `createLogEntry`

## [0.1.3] - 2026-02-15

### Added
- Auto-detecção de trace headers (correlation_id, trace_id, span_id)
- Captura automática de module, file e line via stack trace
- Envio de campos extras (module, file, line) para o backend

### Fixed
- SDK agora envia correlation_id, trace_id, span_id automaticamente

## [0.1.2] - 2026-02-14

### Fixed
- Uses native Node.js http/https modules (no external dependencies needed)

## [0.1.1] - 2026-02-14

### Fixed
- Logger initialization fix

## [0.1.0] - 2026-02-14

### Added
- **HTTP Mode** - New log sending mode via REST API (recommended)
- **Kafka Mode** - Kept as alternative for high performance
- `TransportMode` enum to choose transport mode
- Native HTTP client for sending logs
- API Key authentication support
- Environment variables: `VIU_TRANSPORT_MODE`, `VIU_API_URL`, `VIU_API_KEY`

### Changed
- HTTP is now the default log sending mode
- Does not expose Kafka directly (more secure)
- Simplified configuration for development

### Security
- API Key in Authorization header
- Kafka not exposed directly in HTTP mode

### Performance
- HTTP: immediate sending with retry
- Kafka: batching with gzip compression

## [0.0.1] - 2026-02-12

### Added
- Initial release of viu-pino
- Support for sending logs to Kafka with Pino integration
- Circuit breaker pattern for Kafka connection resilience (5 failures → 60s timeout)
- Smart batching (100 logs or 1000ms timeout)
- Auto-flush when batch size is reached
- SASL_SSL security protocol as default
- Support for SASL/SCRAM-SHA-256 authentication
- Structured logging with Pino
- Correlation ID, Trace ID, and Span ID tracking
- Express middleware for request context
- Singleton pattern for logger instance
- ESM and CommonJS support
- TypeScript type definitions
- Comprehensive test suite with Vitest
- Code coverage reporting

### Security
- Default to SASL_SSL for secure connections
- Clean credential handling
- No sensitive data in logs

### Performance
- Batch processing for efficient message delivery
- Circuit breaker prevents connection storms
- Gzip compression support via KafkaJS
- Efficient memory usage with batch flushing

[0.1.0]: https://github.com/viu-team/viu/releases/tag/viu-pino-v0.1.0
[0.0.1]: https://github.com/viu-team/viu/releases/tag/viu-pino-v0.0.1
