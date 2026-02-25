import pino, { Logger } from 'pino';
import { Kafka, Producer, CompressionTypes } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';
import http from 'http';
import https from 'https';

export type TransportMode = 'http' | 'kafka';

import { 
  detectTraceHeaders, 
  setCorrelationId, 
  setTraceContext,
} from './trace-headers';

export interface ViuPinoConfig {
  serviceName: string;
  environment?: string;
  
  // Transport mode (http or kafka)
  transportMode?: TransportMode;
  
  // HTTP config (for transportMode: 'http')
  apiUrl?: string;
  apiKey?: string;
  httpTimeout?: number;
  
  // Kafka config (for transportMode: 'kafka')
  kafkaBrokers?: string;
  kafkaTopic?: string;
  kafkaUsername?: string;
  kafkaPassword?: string;
  kafkaSaslMechanism?: 'scram-sha-256' | 'scram-sha-512' | 'plain';
  kafkaSecurityProtocol?: 'SASL_SSL' | 'SASL_PLAINTEXT';
  
  // Common options
  level?: 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal';
  prettyPrint?: boolean;
  batchSize?: number;
  batchTimeout?: number;
}

class CircuitBreaker {
  private failures = 0;
  private lastFailureTime: number | null = null;
  private state: 'closed' | 'open' | 'half-open' = 'closed';

  constructor(
    private failureThreshold = 5,
    private timeout = 60000
  ) {}

  recordSuccess(): void {
    this.failures = 0;
    this.state = 'closed';
  }

  recordFailure(): void {
    this.failures++;
    this.lastFailureTime = Date.now();
    
    if (this.failures >= this.failureThreshold) {
      this.state = 'open';
    }
  }

  canAttempt(): boolean {
    if (this.state === 'closed') return true;
    
    if (this.state === 'open') {
      if (Date.now() - (this.lastFailureTime || 0) > this.timeout) {
        this.state = 'half-open';
        return true;
      }
      return false;
    }
    
    return true;
  }
}

class HTTPClient {
  private apiUrl: string;
  private apiKey: string;
  private timeout: number;
  
  constructor(apiUrl: string, apiKey: string, timeout: number = 10000) {
    this.apiUrl = apiUrl.replace(/\/$/, '');
    this.apiKey = apiKey;
    this.timeout = timeout;
  }
  
  async send(logEntry: Record<string, unknown>): Promise<boolean> {
    return new Promise((resolve) => {
      const url = new URL('/api/v1/logs', this.apiUrl);
      const isHttps = url.protocol === 'https:';
      const client = isHttps ? https : http;
      
      const data = JSON.stringify(logEntry);
      
      const options = {
        hostname: url.hostname,
        port: url.port || (isHttps ? 443 : 80),
        path: url.pathname,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `ApiKey ${this.apiKey}`,
          'Content-Length': Buffer.byteLength(data),
        },
        timeout: this.timeout,
      };
      
      const req = client.request(options, (res) => {
        res.on('data', () => {});
        res.on('end', () => {
          resolve(res.statusCode !== undefined && res.statusCode < 400);
        });
      });
      
      req.on('error', () => resolve(false));
      req.on('timeout', () => {
        req.destroy();
        resolve(false);
      });
      
      req.write(data);
      req.end();
    });
  }
  
  async sendBatch(logEntries: Record<string, unknown>[]): Promise<boolean> {
    return new Promise((resolve) => {
      const url = new URL('/api/v1/logs', this.apiUrl);
      const isHttps = url.protocol === 'https:';
      const client = isHttps ? https : http;
      
      const data = JSON.stringify(logEntries);
      
      const options = {
        hostname: url.hostname,
        port: url.port || (isHttps ? 443 : 80),
        path: url.pathname,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `ApiKey ${this.apiKey}`,
          'Content-Length': Buffer.byteLength(data),
        },
        timeout: this.timeout,
      };
      
      const req = client.request(options, (res) => {
        res.on('data', () => {});
        res.on('end', () => {
          resolve(res.statusCode !== undefined && res.statusCode < 400);
        });
      });
      
      req.on('error', () => resolve(false));
      req.on('timeout', () => {
        req.destroy();
        resolve(false);
      });
      
      req.write(data);
      req.end();
    });
  }
}

export class ViuPino {
  private logger: Logger;
  private producer: Producer | null = null;
  private kafka: Kafka | null = null;
  private httpClient: HTTPClient | null = null;
  private config: Required<ViuPinoConfig>;
  private initialized = false;
  private batch: Array<{ value: string }> = [];
  private batchTimer: NodeJS.Timeout | null = null;
  private circuitBreaker: CircuitBreaker;
  private transportMode: TransportMode = 'http';

  private static _instance: ViuPino | null = null;
  private static _correlationId: string | null = null;
  private static _traceId: string | null = null;
  private static _spanId: string | null = null;

  constructor(config: ViuPinoConfig) {
    this.config = {
      environment: 'development',
      transportMode: 'http',
      apiUrl: '',
      apiKey: '',
      httpTimeout: 10000,
      kafkaBrokers: 'localhost:9092',
      kafkaTopic: 'logs.app.raw',
      kafkaSaslMechanism: 'scram-sha-256',
      kafkaSecurityProtocol: 'SASL_SSL',
      level: 'info',
      prettyPrint: false,
      batchSize: 100,
      batchTimeout: 1000,
      ...config,
    } as Required<ViuPinoConfig>;

    this.transportMode = this.config.transportMode || 'http';
    this.circuitBreaker = new CircuitBreaker();

    this.logger = pino({
      level: this.config.level,
      base: {
        service: this.config.serviceName,
        environment: this.config.environment,
      },
      formatters: {
        level: (label: string) => ({ level: label }),
      },
      timestamp: () => `,"time":"${new Date().toISOString()}"`,
    });
  }

  static getInstance(config?: ViuPinoConfig): ViuPino {
    if (ViuPino._instance === null) {
      if (!config) {
        throw new Error('ViuPino requires config on first initialization');
      }
      ViuPino._instance = new ViuPino(config);
    }
    return ViuPino._instance;
  }

  static resetInstance(): void {
    ViuPino._instance = null;
    ViuPino._correlationId = null;
    ViuPino._traceId = null;
    ViuPino._spanId = null;
  }

  static get correlationId(): string | null {
    return ViuPino._correlationId;
  }

  static set correlationId(value: string) {
    ViuPino._correlationId = value;
  }

  static get traceId(): string | null {
    return ViuPino._traceId;
  }

  static set traceId(value: string) {
    ViuPino._traceId = value;
  }

  static get spanId(): string | null {
    return ViuPino._spanId;
  }

  static set spanId(value: string) {
    ViuPino._spanId = value;
  }

  async initialize(): Promise<void> {
    if (this.initialized) return;

    if (this.transportMode === 'http') {
      // HTTP mode
      if (!this.config.apiUrl || !this.config.apiKey) {
        throw new Error('API URL and API Key are required for HTTP mode');
      }
      this.httpClient = new HTTPClient(
        this.config.apiUrl,
        this.config.apiKey,
        this.config.httpTimeout
      );
    } else {
      // Kafka mode (legacy)
      const kafkaConfig: ConstructorParameters<typeof Kafka>[0] = {
        clientId: this.config.serviceName,
        brokers: [this.config.kafkaBrokers],
        retry: {
          initialRetryTime: 300,
          retries: 8,
          multiplier: 2,
        },
      };

      if (this.config.kafkaUsername && this.config.kafkaPassword) {
        kafkaConfig.sasl = {
          mechanism: this.config.kafkaSaslMechanism as any,
          username: this.config.kafkaUsername,
          password: this.config.kafkaPassword,
        };
        kafkaConfig.ssl = this.config.kafkaSecurityProtocol === 'SASL_SSL';
      }

      this.kafka = new Kafka(kafkaConfig);
      this.producer = this.kafka.producer({
        allowAutoTopicCreation: false,
      });

      try {
        await this.producer.connect();
        this.circuitBreaker.recordSuccess();
      } catch (error) {
        this.circuitBreaker.recordFailure();
        throw error;
      }
    }

    this.initialized = true;
  }

  private createLogEntry(
    level: string,
    message: string,
    context?: Record<string, unknown>,
    appStack?: string
  ): object {
    // Auto-gerar e persistir correlation_id
    const correlationId = ViuPino._correlationId || (() => {
      const id = uuidv4();
      ViuPino._correlationId = id;
      return id;
    })();

    // Auto-gerar e persistir trace_id (fallback para correlation_id)
    const traceId = ViuPino._traceId || (() => {
      const id = correlationId;
      ViuPino._traceId = id;
      return id;
    })();

    // Auto-gerar e persistir span_id (16 chars)
    const spanId = ViuPino._spanId || (() => {
      const id = uuidv4().slice(0, 16);
      ViuPino._spanId = id;
      return id;
    })();

    const stack = appStack || new Error().stack;
    let module = '';
    let file = '';
    let line = 0;

    if (stack) {
      const lines = stack.split('\n');
      for (const lineInfo of lines) {
        const match = lineInfo.match(/at\s+(?:.*\s+)?\(?(.+):(\d+):\d+\)?/);
        if (match && !lineInfo.includes('ViuPino') && !lineInfo.includes('createLogEntry') && !lineInfo.includes('at log ') && !lineInfo.includes('processTicksAndRejections')) {
          file = match[1];
          line = parseInt(match[2], 10);
          const fileParts = file.split('/');
          module = fileParts[fileParts.length - 1].replace('.ts', '').replace('.js', '');
          break;
        }
      }
    }

    return {
      timestamp: new Date().toISOString(),
      level,
      service: this.config.serviceName,
      environment: this.config.environment,
      message,
      correlation_id: correlationId,
      trace_id: traceId,
      span_id: spanId,
      module,
      file,
      line,
      context: context
        ? Object.fromEntries(
            Object.entries(context).filter(([_, v]) => v !== null && v !== undefined)
          )
        : {},
    };
  }

  private async flushBatch(): Promise<void> {
    if (this.batch.length === 0) return;

    if (this.transportMode === 'http') {
      // HTTP mode - não usa batch para simplificar
      this.batch = [];
      return;
    }

    // Kafka mode
    if (!this.circuitBreaker.canAttempt()) {
      this.batch.forEach(msg => console.log(msg.value));
      this.batch = [];
      return;
    }

    if (!this.producer) {
      this.batch.forEach(msg => console.log(msg.value));
      this.batch = [];
      return;
    }

    const currentBatch = [...this.batch];
    this.batch = [];

    try {
      await this.producer.send({
        topic: this.config.kafkaTopic,
        messages: currentBatch,
        compression: CompressionTypes.GZIP,
      });
      this.circuitBreaker.recordSuccess();
    } catch (error) {
      this.circuitBreaker.recordFailure();
      currentBatch.forEach(msg => console.log(msg.value));
    }
  }

  private async sendLog(logEntry: Record<string, unknown>): Promise<void> {
    if (!this.initialized) {
      await this.initialize();
    }

    if (this.transportMode === 'http' && this.httpClient) {
      // HTTP mode - send directly
      try {
        await this.httpClient.send(logEntry);
      } catch (error) {
        console.error('Failed to send log via HTTP:', error);
      }
      return;
    }

    // Kafka mode
    const message = { value: JSON.stringify(logEntry) };
    this.batch.push(message);

    if (this.batch.length >= this.config.batchSize) {
      if (this.batchTimer) {
        clearTimeout(this.batchTimer);
        this.batchTimer = null;
      }
      await this.flushBatch();
    } else if (!this.batchTimer) {
      this.batchTimer = setTimeout(() => {
        this.batchTimer = null;
        this.flushBatch().catch(() => {});
      }, this.config.batchTimeout);
    }
  }

  async log(
    level: string,
    message: string,
    context?: Record<string, unknown>
  ): Promise<void> {
    const appStack = new Error().stack;
    const logEntry = this.createLogEntry(level, message, context, appStack);

    const pinoLogMethod = this.logger[level as keyof typeof this.logger] as (
      msg: string,
      ctx?: Record<string, unknown>
    ) => void;

    if (typeof pinoLogMethod === 'function') {
      pinoLogMethod.call(this.logger, message, {
        ...context,
        correlation_id: (logEntry as { correlation_id: string }).correlation_id,
        trace_id: (logEntry as { trace_id: string }).trace_id,
        span_id: (logEntry as { span_id: string | null }).span_id,
      });
    }

    await this.sendLog(logEntry as Record<string, unknown>);
  }

  async trace(message: string, context?: Record<string, unknown>): Promise<void> {
    await this.log('trace', message, context);
  }

  async debug(message: string, context?: Record<string, unknown>): Promise<void> {
    await this.log('debug', message, context);
  }

  async info(message: string, context?: Record<string, unknown>): Promise<void> {
    await this.log('info', message, context);
  }

  async warn(message: string, context?: Record<string, unknown>): Promise<void> {
    await this.log('warn', message, context);
  }

  async error(message: string, error?: Error, context?: Record<string, unknown>): Promise<void> {
    const fullContext = {
      ...context,
      error: error ? {
        name: error.name,
        message: error.message,
        stack: error.stack,
      } : undefined,
    };
    await this.log('error', message, fullContext);
  }

  async fatal(message: string, context?: Record<string, unknown>): Promise<void> {
    await this.log('fatal', message, context);
  }

  setTraceHeaders(headers: Record<string, string>): void {
    /**
     * Define headers de tracing para a requisição atual
     * 
     * @param headers - Objeto com headers HTTP (case-insensitive)
     *                  Ex: {'X-Correlation-ID': 'req-123', 'traceparent': '00-xxx-yyy-01'}
     */
    const traceInfo = detectTraceHeaders(headers);
    
    if (traceInfo.correlationId) {
      setCorrelationId(traceInfo.correlationId);
    }
    if (traceInfo.traceId) {
      setTraceContext(traceInfo.traceId, traceInfo.spanId);
    }
  }

  async close(): Promise<void> {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }

    if (this.transportMode === 'kafka') {
      await this.flushBatch();

      if (this.producer) {
        await this.producer.disconnect();
        this.producer = null;
      }
    }

    this.initialized = false;
  }

  child(bindings: Record<string, unknown>): Logger {
    return this.logger.child(bindings);
  }
}

export function createViuPino(config: ViuPinoConfig): ViuPino {
  return new ViuPino(config);
}
