import request from 'supertest';
import { createApp } from '../../src/app.js';
import { loadConfig } from '../../src/config/env.js';

const config = loadConfig({
  NODE_ENV: 'test',
  REDIS_URL: 'redis://localhost:6379',
  JWT_JWKS_URL: 'http://localhost:3001/.well-known/jwks.json',
  DATABASE_URL:
    'postgresql://audit_user:local-test-password@localhost:5433/resolve_audit',
  KAFKA_ENABLED: 'false',
  LOG_LEVEL: 'silent',
});

function createTestApp(
  appConfig: ReturnType<typeof loadConfig>,
  checks: {
    checkDatabase?: () => Promise<void>;
    checkKafka?: () => Promise<void>;
    checkAuthState?: () => Promise<void>;
  } = {},
) {
  return createApp(appConfig, {
    checkDatabase: checks.checkDatabase ?? (async () => undefined),
    checkKafka: checks.checkKafka ?? (async () => undefined),
    checkAuthState: checks.checkAuthState ?? (async () => undefined),
    auditApi: {
      repository: {
        findPage: jest.fn().mockResolvedValue({
          content: [],
          page: 0,
          size: 20,
          totalElements: 0,
          totalPages: 0,
        }),
      },
      verifier: { verify: jest.fn() },
      authorization: { canReadAudit: jest.fn().mockResolvedValue(false) },
    },
  });
}

describe('health endpoints', () => {
  it('reports process liveness and request correlation', async () => {
    const app = createTestApp(config, {
      checkDatabase: async () => undefined,
      checkKafka: async () => undefined,
    });

    const response = await request(app)
      .get('/health')
      .set('x-request-id', 'test-request-id');

    expect(response.status).toBe(200);
    expect(response.body).toMatchObject({
      status: 'ok',
      service: 'audit-service',
      requestId: 'test-request-id',
    });
    expect(response.headers['x-request-id']).toBe('test-request-id');
    expect(response.headers['x-trace-id']).toEqual(expect.any(String));
  });

  it('reports readiness when database is available and Kafka is disabled', async () => {
    const app = createTestApp(config, {
      checkDatabase: async () => undefined,
      checkKafka: async () => undefined,
    });

    const response = await request(app).get('/health/ready');

    expect(response.status).toBe(200);
    expect(response.body).toMatchObject({
      status: 'ready',
      checks: { database: 'up', kafka: 'disabled', authentication: 'up' },
    });
  });

  it('reports readiness failure when PostgreSQL is unavailable', async () => {
    const app = createTestApp(config, {
      checkDatabase: async () => {
        throw new Error('database connection failed');
      },
      checkKafka: async () => undefined,
    });

    const response = await request(app).get('/health/ready');

    expect(response.status).toBe(503);
    expect(response.body).toMatchObject({
      status: 'not_ready',
      checks: { database: 'down', kafka: 'disabled', authentication: 'up' },
    });
  });

  it('reports readiness failure when configured Kafka is unavailable', async () => {
    const kafkaConfig = loadConfig({
      NODE_ENV: 'test',
      REDIS_URL: 'redis://localhost:6379',
      JWT_JWKS_URL: 'http://localhost:3001/.well-known/jwks.json',
      DATABASE_URL:
        'postgresql://audit_user:local-test-password@localhost:5433/resolve_audit',
      KAFKA_ENABLED: 'true',
      KAFKA_BOOTSTRAP_SERVERS: 'broker:9092',
      LOG_LEVEL: 'silent',
    });
    const app = createTestApp(kafkaConfig, {
      checkDatabase: async () => undefined,
      checkKafka: async () => {
        throw new Error('Kafka connection failed');
      },
    });

    const response = await request(app).get('/health/ready');

    expect(response.status).toBe(503);
    expect(response.body.checks).toMatchObject({
      database: 'up',
      kafka: 'down',
      authentication: 'up',
    });
  });

  it('does not expose application routes outside health checks', async () => {
    const app = createTestApp(config, {
      checkDatabase: async () => undefined,
      checkKafka: async () => undefined,
    });

    const response = await request(app).get('/api/v1/not-found');

    expect(response.status).toBe(404);
    expect(response.body.code).toBe('ROUTE_NOT_FOUND');
  });
});
