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

**Pino adapter for Viu logging**

## ✨ Quer ver logs? → Joga no Viu. Viu?

viu-pino é uma biblioteca TypeScript/JavaScript que integra o [Pino](https://getpino.io) com o sistema Viu, oferecendo dois modos de transporte:
- **HTTP** (recomendado): Envia logs via API REST
- **Kafka**: Envia logs diretamente para o Kafka

### 🚀 Features

- ✅ **Modo HTTP** - Não expõe Kafka, autenticação via API Key
- ✅ **Modo Kafka** - Para alta performance (legacy)
- ✅ **Circuit Breaker** - Previne connection storms
- ✅ **Smart Batching** - 100 logs ou 1000ms (auto-flush)
- ✅ **Correlation IDs** - Rastreamento de requisições
- ✅ **Express Middleware** - Integração nativa
- ✅ **TypeScript** - Type-safe com definições completas
- ✅ **ESM + CommonJS** - Suporte dual module
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

### Modo HTTP (Recomendado)

```typescript
import { ViuPino, TransportMode } from 'viu-pino';

const logger = new ViuPino({
  serviceName: 'my-api',
  environment: 'production',
  transportMode: TransportMode.HTTP,
  apiUrl: 'http://localhost:3000',  // URL do backend VIU
  apiKey: 'viu_live_xxx',           // Gere no dashboard
});

await logger.initialize();

// Logging estruturado
logger.info('User logged in', { userId: '123', ip: '192.168.1.1' });
logger.error('Payment failed', new Error('Insufficient funds'), { amount: 99.90 });
```

### Modo Kafka (Legacy/Alternativo)

```typescript
import { ViuPino, TransportMode } from 'viu-pino';

const logger = new ViuPino({
  serviceName: 'my-api',
  environment: 'production',
  transportMode: TransportMode.KAFKA,
  kafkaBrokers: 'kafka.example.com:9092',
  kafkaTopic: 'logs.tenant-id',
  kafkaUsername: 'tenant_user',
  kafkaPassword: 'secure-password',
});

await logger.initialize();

logger.info('Application started', { version: '1.2.3' });
```

### 🔐 Produção com Autenticação SASL

```typescript
import { ViuPino, TransportMode } from 'viu-pino';

const logger = new ViuPino({
  serviceName: 'my-api',
  environment: 'production',
  transportMode: TransportMode.KAFKA,
  kafkaBrokers: 'viu-kafka.example.com:9092',
  kafkaTopic: 'logs.production',
  kafkaUsername: 'viu_tenant123abc',
  kafkaPassword: 'your-secure-password',
  kafkaSaslMechanism: 'scram-sha-256',
  kafkaSecurityProtocol: 'SASL_SSL',
});
```

### 🌍 Configuração via Environment Variables

```bash
# Modo HTTP
export VIU_TRANSPORT_MODE=http
export VIU_SERVICE_NAME=my-api
export VIU_ENVIRONMENT=production
export VIU_API_URL=http://localhost:3000
export VIU_API_KEY=viu_live_xxx

# Modo Kafka
export VIU_TRANSPORT_MODE=kafka
export VIU_KAFKA_BROKERS=kafka.example.com:9092
export VIU_KAFKA_TOPIC=logs.production
export VIU_KAFKA_USERNAME=viu_tenant123abc
export VIU_KAFKA_PASSWORD=your-secure-password
```

### 🔄 Integração com Express

```typescript
import express from 'express';
import { viuCorrelationMiddleware } from 'viu-pino/express';
import { ViuPino, TransportMode } from 'viu-pino';

const app = express();

// Middleware para correlation IDs
app.use(viuCorrelationMiddleware({
  serviceName: 'my-api',
  environment: 'production',
  transportMode: TransportMode.HTTP,
  apiUrl: 'http://localhost:3000',
  apiKey: 'viu_live_xxx',
}));

const logger = new ViuPino({
  serviceName: 'my-api',
  transportMode: TransportMode.HTTP,
  apiUrl: 'http://localhost:3000',
  apiKey: 'viu_live_xxx',
});

app.get('/users/:id', async (req, res) => {
  logger.info('Fetching user', { userId: req.params.id });
  res.json({ id: req.params.id });
});

app.listen(3000, () => {
  logger.info('Server started', { port: 3000 });
});
```

## ⚙️ Configuração

| Opção | Tipo | Descrição | Padrão |
|-------|------|-----------|--------|
| `serviceName` | string | Nome do serviço | (obrigatório) |
| `environment` | string | Ambiente | `'development'` |
| `transportMode` | `'http' \| 'kafka'` | Modo de transporte | `'http'` |
| `apiUrl` | string | URL da API (HTTP mode) | undefined |
| `apiKey` | string | API Key (HTTP mode) | undefined |
| `kafkaBrokers` | string | Endereço Kafka | `'localhost:9092'` |
| `kafkaTopic` | string | Topic Kafka | `'logs.app.raw'` |
| `kafkaUsername` | string | Username SASL | undefined |
| `kafkaPassword` | string | Password SASL | undefined |
| `kafkaSaslMechanism` | string | Mecanismo SASL | `'scram-sha-256'` |
| `kafkaSecurityProtocol` | string | Protocolo | `'SASL_SSL'` |
| `level` | string | Nível do log | `'info'` |
| `batchSize` | number | Tamanho do batch | `100` |
| `batchTimeout` | number | Timeout do batch (ms) | `1000` |

## 📈 Performance

- **Modo HTTP**: Envio imediato, retry automático
- **Modo Kafka**: Batching (100 logs ou 1000ms), compressão gzip
- **Circuit Breaker**: Evita sobrecarga em falhas
- **Pino**: Um dos loggers mais rápidos do Node.js

## 🔒 Segurança

- **Modo HTTP**: API Key no header Authorization
- **Modo Kafka**: SASL_SSL ativado por padrão
- Suporte TLS/SSL completo
- Autenticação: SCRAM-SHA-256, SCRAM-SHA-512, PLAIN

## 📦 Exports

```typescript
// Main export
import { ViuPino, ViuPinoConfig, TransportMode, createViuPino } from 'viu-pino';

// Express middleware
import { 
  viuCorrelationMiddleware, 
  getCorrelationId, 
  getTraceId, 
  getSpanId 
} from 'viu-pino/express';
```

## 🆚 HTTP vs Kafka

| Aspecto | HTTP | Kafka |
|---------|------|-------|
| Complexidade | Baixa | Alta |
| Exposição Kafka | Não | Sim |
| Autenticação | API Key | SASL |
| Performance | Boa | Excelente |
| Recomendado | Padrão | Alta performance |

## 🤝 Contributing

Contribuições são bem-vindas!

## 📄 License

MIT License - see [LICENSE](LICENSE) para detalhes.

---

<div align="center">
  <sub>Built with ❤️ by @mar.jr</sub>
</div>
