import pino, { Logger } from 'pino';
import { Kafka, Producer, CompressionTypes } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';

export interface ViuPinoConfig {
  serviceName: string;
  environment?: string;
  kafkaBrokers?: string;
  kafkaTopic?: string;
  kafkaUsername?: string;
  kafkaPassword?: string;
  kafkaSaslMechanism?: 'scram-sha-256' | 'scram-sha-512' | 'plain';
  kafkaSecurityProtocol?: 'SASL_SSL' | 'SASL_PLAINTEXT';
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
    
    return true; // half-open
  }
}

export class ViuPino {
  private logger: Logger;
  private producer: Producer | null = null;
  private kafka: Kafka | null = null;
  private config: Required<ViuPinoConfig>;
  private initialized = false;
  private batch: Array<{ value: string }> = [];
  private batchTimer: NodeJS.Timeout | null = null;
  private circuitBreaker: CircuitBreaker;

  private static _instance: ViuPino | null = null;
  private static _correlationId: string | null = null;
  private static _traceId: string | null = null;
  private static _spanId: string | null = null;

  constructor(config: ViuPinoConfig) {
    this.config = {
      environment: 'development',
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
      this.initialized = true;
      this.circuitBreaker.recordSuccess();
    } catch (error) {
      this.circuitBreaker.recordFailure();
      throw error;
    }
  }

  private createLogEntry(
    level: string,
    message: string,
    context?: Record<string, unknown>
  ): object {
    const correlationId = ViuPino._correlationId || uuidv4();
    const traceId = ViuPino._traceId || correlationId;
    const spanId = ViuPino._spanId;

    return {
      timestamp: new Date().toISOString(),
      level,
      service: this.config.serviceName,
      environment: this.config.environment,
      message,
      correlation_id: correlationId,
      trace_id: traceId,
      span_id: spanId,
      context: context || {},
    };
  }

  private async flushBatch(): Promise<void> {
    if (this.batch.length === 0) return;
    if (!this.circuitBreaker.canAttempt()) {
      // Circuit breaker open - fallback para console
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

  private async sendToKafka(logEntry: object): Promise<void> {
    if (!this.initialized) {
      await this.initialize();
    }

    const message = { value: JSON.stringify(logEntry) };
    this.batch.push(message);

    // Auto-flush se atingir batch size
    if (this.batch.length >= this.config.batchSize) {
      if (this.batchTimer) {
        clearTimeout(this.batchTimer);
        this.batchTimer = null;
      }
      await this.flushBatch();
    } else if (!this.batchTimer) {
      // Configura timer para flush automático
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
    const logEntry = this.createLogEntry(level, message, context);

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

    await this.sendToKafka(logEntry);
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

  async close(): Promise<void> {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }

    await this.flushBatch();

    if (this.producer) {
      await this.producer.disconnect();
      this.producer = null;
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
