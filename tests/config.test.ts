"""
Test configuration for ViuPino
"""
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ViuPino, ViuPinoConfig } from '../src/index';

describe('ViuPino Configuration', () => {
  let config: ViuPinoConfig;

  beforeEach(() => {
    config = {
      serviceName: 'test-service',
      environment: 'test',
      kafkaBrokers: 'localhost:9092',
      kafkaTopic: 'test.topic',
    };
  });

  it('should create instance with default values', () => {
    const logger = new ViuPino(config);
    
    expect(logger).toBeDefined();
    expect(logger['config'].serviceName).toBe('test-service');
    expect(logger['config'].environment).toBe('test');
  });

  it('should use SASL_SSL as default security protocol', () => {
    const logger = new ViuPino(config);
    
    expect(logger['config'].kafkaSecurityProtocol).toBe('SASL_SSL');
  });

  it('should accept custom batch configuration', () => {
    const customConfig: ViuPinoConfig = {
      ...config,
      batchSize: 50,
      batchTimeout: 2000,
    };
    
    const logger = new ViuPino(customConfig);
    
    expect(logger['config'].batchSize).toBe(50);
    expect(logger['config'].batchTimeout).toBe(2000);
  });

  it('should accept Kafka credentials', () => {
    const secureConfig: ViuPinoConfig = {
      ...config,
      kafkaUsername: 'test-user',
      kafkaPassword: 'test-pass',
      kafkaSaslMechanism: 'scram-sha-256',
    };
    
    const logger = new ViuPino(secureConfig);
    
    expect(logger['config'].kafkaUsername).toBe('test-user');
    expect(logger['config'].kafkaPassword).toBe('test-pass');
  });
});

describe('ViuPino Singleton', () => {
  beforeEach(() => {
    ViuPino.resetInstance();
  });

  it('should create singleton instance', () => {
    const config: ViuPinoConfig = {
      serviceName: 'test-service',
    };
    
    const instance1 = ViuPino.getInstance(config);
    const instance2 = ViuPino.getInstance();
    
    expect(instance1).toBe(instance2);
  });

  it('should throw error if getInstance called without config first time', () => {
    expect(() => ViuPino.getInstance()).toThrow();
  });

  it('should reset instance', () => {
    const config: ViuPinoConfig = {
      serviceName: 'test-service',
    };
    
    const instance1 = ViuPino.getInstance(config);
    ViuPino.resetInstance();
    
    expect(() => ViuPino.getInstance()).toThrow();
  });
});
