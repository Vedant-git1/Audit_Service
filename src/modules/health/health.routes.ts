import { Router } from 'express';
import type { AppConfig } from '../../config/env.js';

export interface HealthDependencies {
  checkDatabase(): Promise<void>;
  checkKafka(): Promise<void>;
  checkAuthState(): Promise<void>;
}

export function createHealthRouter(config: AppConfig, dependencies: HealthDependencies): Router {
  const router = Router();

  router.get('/health', (_request, response) => {
    response.status(200).json({
      status: 'ok',
      service: config.SERVICE_NAME,
      timestamp: new Date().toISOString(),
      requestId: response.locals.requestId,
    });
  });

  router.get('/health/ready', async (request, response) => {
    const checks = await Promise.allSettled([
      dependencies.checkDatabase(),
      dependencies.checkKafka(),
      dependencies.checkAuthState(),
    ]);
    const databaseReady = checks[0]?.status === 'fulfilled';
    const kafkaReady = !config.KAFKA_ENABLED || checks[1]?.status === 'fulfilled';
    const authReady = checks[2]?.status === 'fulfilled';
    const ready = databaseReady && kafkaReady && authReady;

    checks.forEach((check, index) => {
      if (check.status === 'rejected') {
        request.log.error(
          {
            err: check.reason,
            dependency: ['database', 'kafka', 'authentication'][index],
          },
          'Readiness dependency check failed',
        );
      }
    });

    response.status(ready ? 200 : 503).json({
      status: ready ? 'ready' : 'not_ready',
      service: config.SERVICE_NAME,
      checks: {
        database: databaseReady ? 'up' : 'down',
        kafka: config.KAFKA_ENABLED ? (kafkaReady ? 'up' : 'down') : 'disabled',
        authentication: authReady ? 'up' : 'down',
      },
      requestId: response.locals.requestId,
    });
  });

  return router;
}
