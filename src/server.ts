import { loadConfig } from './config/env.js';
import { createApp } from './app.js';
import {
  checkDatabaseHealth,
  connectDatabase,
  disconnectDatabase,
  prisma,
} from './infrastructure/database/prisma.js';
import { createKafkaConnection } from './infrastructure/kafka/client.js';
import { createAuditIngestionService } from './modules/audit/audit-ingestion.service.js';
import { createAuditRepository } from './modules/audit/audit.repository.js';
import { createLogger } from './shared/logger.js';
import Redis from 'ioredis';
import { createRedisRevocationChecker } from './infrastructure/redis/revocation.js';
import { createJwksJwtVerifier } from './security/jwt.js';
import { auditAuthorizationUnavailable } from './modules/audit/audit.authorization.js';
import { createAuditQueryRepository } from './modules/audit/audit-query.repository.js';

const config = loadConfig();
const logger = createLogger(config);
const redis = new Redis(config.REDIS_URL, {
  lazyConnect: true,
  maxRetriesPerRequest: 1,
  enableOfflineQueue: false,
});
const revocationChecker = createRedisRevocationChecker(redis);
const jwtVerifier = createJwksJwtVerifier({
  jwksUrl: config.JWT_JWKS_URL,
  issuer: config.JWT_ISSUER,
  ...(config.JWT_AUDIENCE ? { audience: config.JWT_AUDIENCE } : {}),
  revocationChecker,
});
const auditRepository = createAuditRepository(prisma);
const auditQueryRepository = createAuditQueryRepository(prisma);
const ingestionService = createAuditIngestionService(auditRepository);
const kafkaConnection = createKafkaConnection(config, ingestionService, logger);
const app = createApp(config, {
  checkDatabase: checkDatabaseHealth,
  checkKafka: () => kafkaConnection.checkHealth(),
  checkAuthState: async () => {
    await redis.ping();
  },
  auditApi: {
    repository: auditQueryRepository,
    verifier: jwtVerifier,
    authorization: auditAuthorizationUnavailable,
  },
});

let shuttingDown = false;

async function closeDependencies(): Promise<void> {
  const results = await Promise.allSettled([
    kafkaConnection.disconnect(),
    disconnectDatabase(),
    redis.quit(),
  ]);
  results.forEach((result, index) => {
    if (result.status === 'rejected') {
      logger.error(
        {
          err: result.reason,
          dependency: ['kafka', 'database', 'redis'][index],
        },
        'Failed to close dependency',
      );
      process.exitCode = 1;
    }
  });
}

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.info({ signal }, 'Shutting down');

  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  }).catch((error: unknown) => {
    logger.error({ err: error }, 'Failed to close HTTP server');
    process.exitCode = 1;
  });

  await closeDependencies();
}

const server = app.listen(config.PORT, () => {
  logger.info({ port: config.PORT }, 'HTTP server listening');
  void connectDatabase().catch((error: unknown) => {
    logger.error({ err: error }, 'Initial database connection failed; readiness will report unavailable');
  });
  void kafkaConnection.start().catch((error: unknown) => {
    logger.error({ err: error }, 'Initial Kafka consumer start failed; readiness will report unavailable');
  });
  void redis.connect().catch((error: unknown) => {
    logger.error({ err: error }, 'Initial Redis connection failed; authentication readiness will report unavailable');
  });
});
server.on('error', (error) => {
  logger.fatal({ err: error }, 'Failed to start HTTP server');
  process.exitCode = 1;
});
process.once('SIGINT', () => void shutdown('SIGINT'));
process.once('SIGTERM', () => void shutdown('SIGTERM'));
