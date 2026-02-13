import { Request, Response, NextFunction } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { ViuPino, ViuPinoConfig } from './index';

export interface ViuExpressMiddlewareConfig extends Partial<ViuPinoConfig> {
  serviceName: string;
}

export function viuCorrelationMiddleware(
  _config: ViuExpressMiddlewareConfig
) {
  return async (req: Request, res: Response, next: NextFunction) => {
    const correlationId = req.headers['x-correlation-id'] as string || uuidv4();
    const traceId = uuidv4();
    const spanId = uuidv4().slice(0, 16);

    ViuPino.correlationId = correlationId;
    ViuPino.traceId = traceId;
    ViuPino.spanId = spanId;

    (req as any).correlationId = correlationId;
    (req as any).traceId = traceId;
    (req as any).spanId = spanId;

    res.setHeader('X-Correlation-ID', correlationId);
    res.setHeader('X-Trace-ID', traceId);

    next();
  };
}

export function getCorrelationId(req: Request): string | undefined {
  return (req as any).correlationId;
}

export function getTraceId(req: Request): string | undefined {
  return (req as any).traceId;
}

export function getSpanId(req: Request): string | undefined {
  return (req as any).spanId;
}
