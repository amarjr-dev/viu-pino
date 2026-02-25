/**
 * Auto-detecção de headers de tracing
 * 
 * Supported formats:
 * - X-Correlation-ID
 * - X-Request-ID
 * - traceparent (W3C standard)
 * - X-B3-TraceId (Zipkin B3)
 * - X-B3-SpanId (Zipkin B3)
 */

export interface TraceHeaders {
  correlationId?: string;
  traceId?: string;
  spanId?: string;
}

/**
 * Parse W3C traceparent header
 * Format: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
 * Parts: version-traceId-spanId-flags
 */
function parseTraceparent(traceparent: string): { traceId?: string; spanId?: string } | null {
  if (!traceparent) return null;

  try {
    const parts = traceparent.split('-');
    if (parts.length < 3) return null;

    let traceId = parts[1];
    let spanId = parts[2];

    // spanId pode conter flags, separar
    if (spanId && spanId.length > 16) {
      spanId = spanId.substring(0, 16);
    }

    return { traceId, spanId };
  } catch {
    return null;
  }
}

/**
 * Detecta headers de tracing de um objeto de headers HTTP
 */
export function detectTraceHeaders(headers: Record<string, string>): TraceHeaders {
  // Normalizar keys para lowercase
  const normalized: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    normalized[key.toLowerCase()] = value;
  }

  const result: TraceHeaders = {};

  // 1. traceparent (W3C) - prioritário
  const traceparent = normalized['traceparent'];
  if (traceparent) {
    const parsed = parseTraceparent(traceparent);
    if (parsed) {
      result.traceId = parsed.traceId;
      result.spanId = parsed.spanId;
    }
  }

  // 2. X-B3-TraceId (Zipkin)
  if (!result.traceId) {
    result.traceId = normalized['x-b3-traceid'];
  }

  // 3. X-B3-SpanId (Zipkin)
  if (!result.spanId) {
    result.spanId = normalized['x-b3-spanid'];
  }

  // 4. X-Correlation-ID
  result.correlationId = normalized['x-correlation-id'];

  // 5. X-Request-ID (fallback)
  if (!result.correlationId) {
    result.correlationId = normalized['x-request-id'];
  }

  // 6. X-Trace-ID
  if (!result.traceId) {
    result.traceId = normalized['x-trace-id'];
  }

  return result;
}

// Variáveis de classe estáticas para contexto
let _correlationId: string | null = null;
let _traceId: string | null = null;
let _spanId: string | null = null;

export function setCorrelationId(id: string): void {
  _correlationId = id;
}

export function getCorrelationId(): string | null {
  return _correlationId;
}

export function setTraceContext(traceId: string, spanId?: string): void {
  _traceId = traceId;
  _spanId = spanId || null;
}

export function getTraceContext(): { traceId: string | null; spanId: string | null } {
  return { traceId: _traceId, spanId: _spanId };
}

export function clearTraceContext(): void {
  _correlationId = null;
  _traceId = null;
  _spanId = null;
}
