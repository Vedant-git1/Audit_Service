import express, { type Express } from 'express';
import type { AppConfig } from './config/env.js';
import type { HealthDependencies } from './modules/health/health.routes.js';
import { createHealthRouter } from './modules/health/health.routes.js';
import { requestIdMiddleware } from './middleware/request-id.js';
import { createRequestLoggingMiddleware } from './middleware/request-logging.js';
import { errorHandler, notFoundHandler } from './shared/errors.js';
import { createLogger } from './shared/logger.js';
import { createAuditRouter } from './modules/audit/audit.routes.js';
import type { AuditAuthorizationService, JwtVerifier } from './security/auth.types.js';
import type { AuditQueryRepository } from './modules/audit/audit-query.repository.js';

export interface AppDependencies extends HealthDependencies {
  auditApi: {
    repository: AuditQueryRepository;
    verifier: JwtVerifier;
    authorization: AuditAuthorizationService;
  };
}

export function createApp(config: AppConfig, dependencies: AppDependencies): Express {
  const app = express();
  const logger = createLogger(config);

  app.disable('x-powered-by');
  app.use(requestIdMiddleware);
  app.use(createRequestLoggingMiddleware(logger));
  app.use(express.json());
  app.use(createHealthRouter(config, dependencies));
  app.use('/api/v1/audit', createAuditRouter(dependencies.auditApi));
  app.use(notFoundHandler);
  app.use(errorHandler);

  return app;
}
