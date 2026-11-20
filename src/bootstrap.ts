import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import type { INestApplication } from '@nestjs/common';
import type { NextFunction, Request, Response } from 'express';
import { AppModule } from './app.module';
import { ErrorsFilter } from './http/errors.filter';
import type { AppConfig } from './infrastructure/config';
import { JsonLogger } from './infrastructure/observability/logger';

const CORRELATION_ID = /^[\x21-\x7E]{1,128}$/;

export async function createApp(config: AppConfig): Promise<INestApplication> {
  const app = await NestFactory.create(AppModule.register(config), { bufferLogs: true, abortOnError: false });
  const logger = app.get(JsonLogger);
  app.useLogger(logger);
  app.useGlobalFilters(new ErrorsFilter(logger));
  app.use((req: Request & { correlationId?: string }, res: Response, next: NextFunction) => {
    const incoming = req.header('x-correlation-id');
    req.correlationId = incoming && CORRELATION_ID.test(incoming) ? incoming : Bun.randomUUIDv7();
    res.setHeader('X-Correlation-Id', req.correlationId);
    next();
  });
  app.enableShutdownHooks();
  return app;
}
