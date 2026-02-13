# viu-pino

<div align="center">

[![npm version](https://img.shields.io/npm/v/viu-pino.svg)](https://www.npmjs.com/package/viu-pino)
[![npm downloads](https://img.shields.io/npm/dm/viu-pino.svg)](https://www.npmjs.com/package/viu-pino)
[![License](https://img.shields.io/github/license/viu-team/viu)](https://github.com/viu-team/viu/blob/main/LICENSE)
[![Test Coverage](https://img.shields.io/badge/coverage-92%25-brightgreen.svg)](https://github.com/viu-team/viu)

![TypeScript](https://img.shields.io/badge/typescript-%23007ACC.svg?style=for-the-badge&logo=typescript&logoColor=white)
![Node.js](https://img.shields.io/badge/node.js-6DA55F?style=for-the-badge&logo=node.js&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Pino](https://img.shields.io/badge/Pino-00A98F?style=for-the-badge&logo=javascript&logoColor=white)

</div>

**Pino adapter for Viu logging (Kafka + Loki)**

## ✨ Quer ver logs? → Joga no Viu. Viu?

Viu-pino é uma biblioteca TypeScript/JavaScript que integra o [Pino](https://getpino.io) com Kafka e Loki, oferecendo logging estruturado de alta performance com rastreabilidade completa para aplicações Node.js.

### 🚀 Features

- ✅ **Circuit Breaker** - Previne connection storms (5 falhas → 60s timeout)
- ✅ **Smart Batching** - 100 logs ou 1000ms (auto-flush)
- ✅ **Security First** - SASL_SSL por padrão
- ✅ **Correlation IDs** - Rastreamento de requisições
- ✅ **Express Middleware** - Integração nativa
- ✅ **TypeScript** - Type-safe com definições completas
- ✅ **ESM + CommonJS** - Suporte dual module
- ✅ **Singleton Pattern** - Gerenciamento simplificado
- ✅ **Pino Performance** - Ultra-rápido e eficiente

## 📦 Instalação

```bash
npm install viu-pino
# or
yarn add viu-pino
# or
pnpm add viu-pino
# or
bun add viu-pino
```

## 🎯 Quick Start

### Uso Básico

```typescript
import { ViuPino } from 'viu-pino';

// Configuração para desenvolvimento (sem autenticação)
const logger = ViuPino.getInstance({
  serviceName: 'my-api',
  environment: 'development',
  kafkaBrokers: 'localhost:9092',
  kafkaTopic: 'logs.app.raw',
});

await logger.initialize();

// Logging estruturado
logger.info('User logged in', { userId: '123', ip: '192.168.1.1' });
logger.error('Payment failed', new Error('Insufficient funds'), { amount: 99.90 });
logger.warn('High memory usage', { memoryPercent: 85.5 });
```

### 🔐 Produção com Autenticação SASL

```typescript
import { ViuPino } from 'viu-pino';

const logger = ViuPino.getInstance({
  serviceName: 'my-api',
  environment: 'production',
  kafkaBrokers: 'viu-kafka.example.com:9092',
  kafkaTopic: 'logs.production',
  kafkaUsername: 'viu_tenant123abc',
  kafkaPassword: 'your-secure-password',
  kafkaSaslMechanism: 'scram-sha-256',
  kafkaSecurityProtocol: 'SASL_SSL', // Default!
});

await logger.initialize();

logger.info('Application started', { version: '1.2.3' });
```

### 🌍 Configuração via Environment Variables

```bash
export VIU_SERVICE_NAME=my-api
export VIU_ENVIRONMENT=production
export VIU_KAFKA_BROKERS=viu-kafka.example.com:9092
export VIU_KAFKA_TOPIC=logs.production
export VIU_KAFKA_USERNAME=viu_tenant123abc
export VIU_KAFKA_PASSWORD=your-secure-password
export VIU_KAFKA_SASL_MECHANISM=scram-sha-256
export VIU_KAFKA_SECURITY_PROTOCOL=SASL_SSL
```

```typescript
import { ViuPino } from 'viu-pino';

const logger = ViuPino.getInstance({
  serviceName: process.env.VIU_SERVICE_NAME!,
  environment: process.env.VIU_ENVIRONMENT,
  kafkaBrokers: process.env.VIU_KAFKA_BROKERS,
  kafkaTopic: process.env.VIU_KAFKA_TOPIC,
  kafkaUsername: process.env.VIU_KAFKA_USERNAME,
  kafkaPassword: process.env.VIU_KAFKA_PASSWORD,
});
```

### 🔄 Integração com Express

```typescript
import express from 'express';
import { viuCorrelationMiddleware } from 'viu-pino/express';
import { ViuPino } from 'viu-pino';

const app = express();

// Adicionar middleware para correlation IDs
app.use(viuCorrelationMiddleware({
  serviceName: 'my-api',
  environment: 'production',
  kafkaBrokers: 'kafka.example.com:9092',
  kafkaUsername: 'user',
  kafkaPassword: 'pass',
}));

// Logger
const logger = ViuPino.getInstance();

app.get('/users/:id', (req, res) => {
  // Correlation ID já está setado pelo middleware
  logger.info('Fetching user', { userId: req.params.id });
  res.json({ id: req.params.id });
});

app.listen(3000, () => {
  logger.info('Server started', { port: 3000 });
});
```

### 📊 Logging Estruturado Avançado

```typescript
// Contexto rico
logger.info('User action completed', {
  userId: 'user_123',
  action: 'purchase',
  productId: 'prod_456',
  amount: 149.99,
  currency: 'USD',
  paymentMethod: 'credit_card',
  durationMs: 234,
});

// Tratamento de exceções
try {
  const result = await riskyOperation();
} catch (error) {
  logger.error('Operation failed', error as Error, {
    operation: 'riskyOperation',
    attemptNumber: 3,
  });
}
```

### 🔍 Correlation IDs e Rastreamento

```typescript
import { ViuPino } from 'viu-pino';

// Definir correlation ID manualmente
ViuPino.correlationId = 'request-123-abc';
ViuPino.traceId = 'trace-456-def';
ViuPino.spanId = 'span-789-ghi';

logger.info('Processing request'); // Inclui IDs automaticamente

// Resetar
ViuPino.resetInstance();
```

### 🔧 Customização com Pino Child Logger

```typescript
// Criar child logger com contexto fixo
const userLogger = logger.child({ module: 'user-service' });

userLogger.info('User created', { userId: '123' });
// Log terá: { module: 'user-service', userId: '123' }
```

## ⚙️ Configuração

| Opção | Tipo | Descrição | Padrão |
|-------|------|-----------|--------|
| `serviceName` | string | Nome do serviço | (obrigatório) |
| `environment` | string | Ambiente | `'development'` |
| `kafkaBrokers` | string | Endereço Kafka | `'localhost:9092'` |
| `kafkaTopic` | string | Topic Kafka | `'logs.app.raw'` |
| `kafkaUsername` | string | Username SASL | `undefined` |
| `kafkaPassword` | string | Password SASL | `undefined` |
| `kafkaSaslMechanism` | string | Mecanismo SASL | `'scram-sha-256'` |
| `kafkaSecurityProtocol` | string | Protocolo | `'SASL_SSL'` |
| `level` | string | Nível do log | `'info'` |
| `prettyPrint` | boolean | Pretty print | `false` |
| `batchSize` | number | Tamanho do batch | `100` |
| `batchTimeout` | number | Timeout do batch (ms) | `1000` |

## 🧪 Testes

```bash
# Instalar dependências
npm install

# Rodar testes
npm test

# Testes com watch
npm run test:watch

# Com cobertura
npm run test:coverage

# Build
npm run build
```

## 📈 Performance

- **Batching**: Agrupa até 100 logs ou 1000ms antes de enviar
- **Auto-flush**: Flush automático ao atingir batch size
- **Circuit Breaker**: Evita sobrecarga em falhas
- **Pino**: Um dos loggers mais rápidos do Node.js
- **Compression**: Suporte gzip via KafkaJS

## 🔒 Segurança

- SASL_SSL ativado por padrão
- Credenciais nunca aparecem nos logs
- Suporte TLS/SSL completo
- Opções de autenticação: SCRAM-SHA-256, SCRAM-SHA-512, PLAIN

## 📦 Exports

```typescript
// Main export
import { ViuPino, ViuPinoConfig, createViuPino } from 'viu-pino';

// Express middleware
import { 
  viuCorrelationMiddleware, 
  getCorrelationId, 
  getTraceId, 
  getSpanId 
} from 'viu-pino/express';
```

## 🆚 ESM vs CommonJS

```typescript
// ESM
import { ViuPino } from 'viu-pino';

// CommonJS
const { ViuPino } = require('viu-pino');
```

## 🤝 Contributing

Contribuições são bem-vindas! Veja [CONTRIBUTING.md](../../CONTRIBUTING.md).

## 📄 License

MIT License - veja [LICENSE](LICENSE) para detalhes.

## 🔗 Links

- [Documentação](https://github.com/viu-team/viu/tree/main/sdks-monorepo/packages/viu-pino)
- [Issues](https://github.com/viu-team/viu/issues)
- [NPM](https://www.npmjs.com/package/viu-pino)
- [Changelog](CHANGELOG.md)

---

<div align="center">
  <sub>Built with ❤️ by Viu Team - @mar.jr</sub>
</div>
