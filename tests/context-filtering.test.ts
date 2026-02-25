import { describe, it, expect, beforeEach } from 'vitest';
import { ViuPino } from '../src/index';

const TransportMode = { HTTP: 'http', KAFKA: 'kafka' } as const;

describe('Context Filtering', () => {
  let logger: ViuPino;

  beforeEach(() => {
    ViuPino.resetInstance();
    
    logger = new ViuPino({
      serviceName: 'test-service',
      transportMode: TransportMode.HTTP,
      apiUrl: 'http://localhost:3000',
      apiKey: 'test-key',
    });
  });

  it('should filter null values from context', () => {
    const entry = logger['createLogEntry']('info', 'Test message', {
      foo: 'bar',
      baz: null,
      qux: 'value',
    }) as any;
    
    expect(entry.context).toEqual({
      foo: 'bar',
      qux: 'value',
    });
    expect(entry.context).not.toHaveProperty('baz');
  });

  it('should filter undefined values from context', () => {
    const entry = logger['createLogEntry']('info', 'Test message', {
      foo: 'bar',
      baz: undefined,
      qux: 'value',
    }) as any;
    
    expect(entry.context).toEqual({
      foo: 'bar',
      qux: 'value',
    });
    expect(entry.context).not.toHaveProperty('baz');
  });

  it('should keep valid values in context', () => {
    const entry = logger['createLogEntry']('info', 'Test message', {
      string: 'text',
      number: 42,
      boolean: true,
      object: { nested: 'value' },
      array: [1, 2, 3],
      zero: 0,
      emptyString: '',
      falseValue: false,
    }) as any;
    
    expect(entry.context).toEqual({
      string: 'text',
      number: 42,
      boolean: true,
      object: { nested: 'value' },
      array: [1, 2, 3],
      zero: 0,
      emptyString: '',
      falseValue: false,
    });
  });

  it('should return empty object for empty context', () => {
    const entry = logger['createLogEntry']('info', 'Test message', {}) as any;
    
    expect(entry.context).toEqual({});
  });

  it('should return empty object when context is undefined', () => {
    const entry = logger['createLogEntry']('info', 'Test message') as any;
    
    expect(entry.context).toEqual({});
  });

  it('should handle context with only null/undefined values', () => {
    const entry = logger['createLogEntry']('info', 'Test message', {
      a: null,
      b: undefined,
      c: null,
    }) as any;
    
    expect(entry.context).toEqual({});
  });

  it('should preserve nested objects with null values', () => {
    const entry = logger['createLogEntry']('info', 'Test message', {
      user: {
        id: '123',
        email: null,  // null inside nested object is preserved
      },
      topLevelNull: null,  // top level null is filtered
    }) as any;
    
    expect(entry.context).toEqual({
      user: {
        id: '123',
        email: null,
      },
    });
    expect(entry.context).not.toHaveProperty('topLevelNull');
  });

  it('should handle complex context with mixed values', () => {
    const entry = logger['createLogEntry']('info', 'Test message', {
      valid1: 'string',
      invalid1: null,
      valid2: 123,
      invalid2: undefined,
      valid3: { nested: 'object' },
      valid4: [1, 2, 3],
      invalid3: null,
    }) as any;
    
    expect(entry.context).toEqual({
      valid1: 'string',
      valid2: 123,
      valid3: { nested: 'object' },
      valid4: [1, 2, 3],
    });
  });

  it('should preserve Date objects in context', () => {
    const date = new Date('2026-02-25');
    const entry = logger['createLogEntry']('info', 'Test message', {
      createdAt: date,
      nullValue: null,
    }) as any;
    
    expect(entry.context).toEqual({
      createdAt: date,
    });
  });

  it('should not affect correlation_id, trace_id, span_id fields', () => {
    const entry = logger['createLogEntry']('info', 'Test message', {
      foo: 'bar',
      baz: null,
    }) as any;
    
    // IDs should always be present and not null
    expect(entry.correlation_id).toBeTruthy();
    expect(entry.trace_id).toBeTruthy();
    expect(entry.span_id).toBeTruthy();
    
    expect(entry.correlation_id).not.toBeNull();
    expect(entry.trace_id).not.toBeNull();
    expect(entry.span_id).not.toBeNull();
  });
});
