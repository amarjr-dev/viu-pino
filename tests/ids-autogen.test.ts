import { describe, it, expect, beforeEach } from 'vitest';
import { ViuPino } from '../src/index';

const TransportMode = { HTTP: 'http', KAFKA: 'kafka' } as const;

describe('IDs Auto-generation', () => {
  let logger: ViuPino;

  beforeEach(() => {
    // Reset singleton instance before each test
    ViuPino.resetInstance();
    
    logger = new ViuPino({
      serviceName: 'test-service',
      transportMode: TransportMode.HTTP,
      apiUrl: 'http://localhost:3000',
      apiKey: 'test-key',
    });
  });

  it('should auto-generate span_id when not set', () => {
    const entry = logger['createLogEntry']('info', 'Test message', {}) as any;
    
    expect(entry.span_id).toBeTruthy();
    expect(typeof entry.span_id).toBe('string');
    expect(entry.span_id.length).toBe(16);
  });

  it('should auto-generate correlation_id when not set', () => {
    const entry = logger['createLogEntry']('info', 'Test message', {}) as any;
    
    expect(entry.correlation_id).toBeTruthy();
    expect(typeof entry.correlation_id).toBe('string');
    // UUID v4 has 36 chars with dashes
    expect(entry.correlation_id.length).toBe(36);
  });

  it('should use correlation_id as trace_id fallback', () => {
    const entry = logger['createLogEntry']('info', 'Test message', {}) as any;
    
    expect(entry.trace_id).toBe(entry.correlation_id);
  });

  it('should persist generated IDs across log calls', () => {
    const entry1 = logger['createLogEntry']('info', 'First log', {}) as any;
    const entry2 = logger['createLogEntry']('info', 'Second log', {}) as any;
    
    // IDs should be the same for both logs
    expect(entry2.correlation_id).toBe(entry1.correlation_id);
    expect(entry2.trace_id).toBe(entry1.trace_id);
    expect(entry2.span_id).toBe(entry1.span_id);
  });

  it('should allow manual override of span_id', () => {
    const customSpanId = 'custom-span-123';
    ViuPino.spanId = customSpanId;
    
    const entry = logger['createLogEntry']('info', 'Test message', {}) as any;
    
    expect(entry.span_id).toBe(customSpanId);
  });

  it('should allow manual override of correlation_id', () => {
    const customCorrelationId = 'custom-correlation-456';
    ViuPino.correlationId = customCorrelationId;
    
    const entry = logger['createLogEntry']('info', 'Test message', {}) as any;
    
    expect(entry.correlation_id).toBe(customCorrelationId);
  });

  it('should allow manual override of trace_id', () => {
    const customTraceId = 'custom-trace-789';
    ViuPino.traceId = customTraceId;
    
    const entry = logger['createLogEntry']('info', 'Test message', {}) as any;
    
    expect(entry.trace_id).toBe(customTraceId);
  });

  it('should preserve manual span_id across calls', () => {
    const customSpanId = 'preserved-span';
    ViuPino.spanId = customSpanId;
    
    const entry1 = logger['createLogEntry']('info', 'First log', {}) as any;
    const entry2 = logger['createLogEntry']('info', 'Second log', {}) as any;
    
    expect(entry1.span_id).toBe(customSpanId);
    expect(entry2.span_id).toBe(customSpanId);
  });

  it('should generate different IDs for different logger instances', () => {
    ViuPino.resetInstance();
    const logger1 = new ViuPino({
      serviceName: 'service-1',
      transportMode: TransportMode.HTTP,
      apiUrl: 'http://localhost:3000',
      apiKey: 'test-key',
    });
    
    const entry1 = logger1['createLogEntry']('info', 'Message 1', {}) as any;
    
    ViuPino.resetInstance();
    const logger2 = new ViuPino({
      serviceName: 'service-2',
      transportMode: TransportMode.HTTP,
      apiUrl: 'http://localhost:3000',
      apiKey: 'test-key',
    });
    
    const entry2 = logger2['createLogEntry']('info', 'Message 2', {}) as any;
    
    // Different instances should have different IDs
    expect(entry2.correlation_id).not.toBe(entry1.correlation_id);
    expect(entry2.span_id).not.toBe(entry1.span_id);
  });

  it('should have span_id format compatible with W3C Trace Context', () => {
    const entry = logger['createLogEntry']('info', 'Test message', {}) as any;
    const spanId = entry.span_id as string;
    
    // W3C span-id is 16 hex chars (8 bytes)
    expect(spanId.length).toBe(16);
    expect(/^[0-9a-f-]{16}$/i.test(spanId)).toBe(true);
  });
});
