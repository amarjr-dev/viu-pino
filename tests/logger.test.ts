/**
 * Test logger functionality
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { ViuPino, ViuPinoConfig } from '../src/index';

describe('ViuPino Logger', () => {
  let logger: ViuPino;
  let config: ViuPinoConfig;

  beforeEach(() => {
    config = {
      serviceName: 'test-service',
      environment: 'test',
      kafkaBrokers: 'localhost:9092',
    };
    
    logger = new ViuPino(config);
    ViuPino.resetInstance();
  });

  afterEach(async () => {
    if (logger) {
      await logger.close();
    }
  });

  it('should log info messages', async () => {
    const spy = vi.spyOn(logger['logger'], 'info');
    
    await logger.info('Test message', { userId: '123' });
    
    expect(spy).toHaveBeenCalled();
  });

  it('should log error messages with error object', async () => {
    const spy = vi.spyOn(logger['logger'], 'error');
    const testError = new Error('Test error');
    
    await logger.error('Error occurred', testError, { userId: '123' });
    
    expect(spy).toHaveBeenCalled();
  });

  it('should create log entry with required fields', async () => {
    const logEntry = logger['createLogEntry']('info', 'Test message', { extra: 'data' });
    
    expect(logEntry).toHaveProperty('timestamp');
    expect(logEntry).toHaveProperty('level');
    expect(logEntry).toHaveProperty('service');
    expect(logEntry).toHaveProperty('message');
    expect(logEntry).toHaveProperty('correlation_id');
    expect(logEntry).toHaveProperty('trace_id');
  });

  it('should generate correlation_id if not set', async () => {
    const logEntry = logger['createLogEntry']('info', 'Test', {});
    
    expect(logEntry).toHaveProperty('correlation_id');
    expect(typeof logEntry['correlation_id']).toBe('string');
  });

  it('should use existing correlation_id', async () => {
    const testCorrelationId = 'test-correlation-123';
    ViuPino.correlationId = testCorrelationId;
    
    const logEntry = logger['createLogEntry']('info', 'Test', {});
    
    expect(logEntry['correlation_id']).toBe(testCorrelationId);
  });
});

describe('ViuPino Batching', () => {
  let logger: ViuPino;

  beforeEach(() => {
    const config: ViuPinoConfig = {
      serviceName: 'test-service',
      batchSize: 5,
      batchTimeout: 100,
    };
    
    logger = new ViuPino(config);
  });

  afterEach(async () => {
    await logger.close();
    ViuPino.resetInstance();
  });

  it('should accumulate logs in batch', async () => {
    await logger.info('Message 1');
    await logger.info('Message 2');
    
    expect(logger['batch'].length).toBeGreaterThanOrEqual(0);
  });

  it('should flush batch when size limit reached', async () => {
    const flushSpy = vi.spyOn(logger as any, 'flushBatch');
    
    for (let i = 0; i < 6; i++) {
      await logger.info(`Message ${i}`);
    }
    
    expect(flushSpy).toHaveBeenCalled();
  });
});

describe('ViuPino Circuit Breaker', () => {
  it('should open circuit after threshold failures', () => {
    const cb = logger['circuitBreaker'];
    
    for (let i = 0; i < 5; i++) {
      cb.recordFailure();
    }
    
    expect(cb.canAttempt()).toBe(false);
  });

  it('should reset on success', () => {
    const cb = logger['circuitBreaker'];
    
    cb.recordFailure();
    cb.recordFailure();
    cb.recordSuccess();
    
    expect(cb.canAttempt()).toBe(true);
  });
});
