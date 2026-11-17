import request from 'supertest';
import { createApp } from '../../src/app.js';
import { loadConfig } from '../../src/config/env.js';
import type {
  AuthContext,
  JwtVerifier,
} from '../../src/security/auth.types.js';
import type { AuditQueryRepository } from '../../src/modules/audit/audit-query.repository.js';

const config = loadConfig({
  NODE_ENV: 'test',
  DATABASE_URL: 'postgresql://localhost:5433/resolve_audit',
  REDIS_URL: 'redis://localhost:6379',
  JWT_JWKS_URL: 'http://localhost:3001/.well-known/jwks.json',
  KAFKA_ENABLED: 'false',
  LOG_LEVEL: 'silent',
});

const auth: AuthContext = {
  userId: '00000000-0000-4000-8000-000000000001',
  tenantId: '00000000-0000-4000-8000-000000000002',
  tokenId: '00000000-0000-4000-8000-000000000003',
};

function setup(options: { verifier?: JwtVerifier; authorized?: boolean } = {}) {
  const resultPage = {
    content: [],
    page: 0,
    size: 20,
    totalElements: 0,
    totalPages: 0,
  };
  const repository: AuditQueryRepository = {
    findPage: jest.fn().mockImplementation(async (_tenantId, query) => ({
      ...resultPage,
      page: query.page,
      size: query.size,
    })),
  };
  const app = createApp(config, {
    checkDatabase: async () => undefined,
    checkKafka: async () => undefined,
    checkAuthState: async () => undefined,
    auditApi: {
      repository,
      verifier: options.verifier ?? {
        verify: jest.fn().mockResolvedValue(auth),
      },
      authorization: {
        canReadAudit: jest.fn().mockResolvedValue(options.authorized ?? true),
      },
    },
  });
  return { app, repository };
}

describe('GET /api/v1/audit', () => {
  it('requires a bearer token before querying audit records', async () => {
    const { app, repository } = setup();
    const response = await request(app).get('/api/v1/audit');

    expect(response.status).toBe(401);
    expect(response.body.code).toBe('AUTH_MISSING_TOKEN');
    expect(repository.findPage).not.toHaveBeenCalled();
  });

  it('rejects invalid tokens', async () => {
    const { app, repository } = setup({
      verifier: {
        verify: jest.fn().mockRejectedValue(new Error('invalid token')),
      },
    });
    const response = await request(app)
      .get('/api/v1/audit')
      .set('Authorization', 'Bearer invalid');

    expect(response.status).toBe(401);
    expect(response.body.code).toBe('AUTH_MISSING_TOKEN');
    expect(repository.findPage).not.toHaveBeenCalled();
  });

  it('fails closed until the documented audit-read authorization adapter is available', async () => {
    const { app, repository } = setup({ authorized: false });
    const response = await request(app)
      .get('/api/v1/audit')
      .set('Authorization', 'Bearer valid');

    expect(response.status).toBe(403);
    expect(response.body.code).toBe('AUDIT_FORBIDDEN');
    expect(repository.findPage).not.toHaveBeenCalled();
  });

  it('retrieves tenant-scoped pages with supported filters and sorting', async () => {
    const { app, repository } = setup();
    const response = await request(app)
      .get('/api/v1/audit')
      .query({
        resourceId: '00000000-0000-4000-8000-000000000004',
        actorId: '00000000-0000-4000-8000-000000000005',
        eventType: 'CaseCreated',
        resourceType: 'CASE',
        from: '2026-01-01T00:00:00Z',
        to: '2026-01-31T23:59:59Z',
        page: '1',
        size: '10',
        sort: 'timestamp,asc',
      })
      .set('Authorization', 'Bearer valid');

    expect(response.status).toBe(200);
    expect(response.body).toEqual({
      content: [],
      page: 1,
      size: 10,
      totalElements: 0,
      totalPages: 0,
    });
    expect(repository.findPage).toHaveBeenCalledWith(auth.tenantId, {
      resourceId: '00000000-0000-4000-8000-000000000004',
      actorId: '00000000-0000-4000-8000-000000000005',
      eventType: 'CaseCreated',
      resourceType: 'CASE',
      from: new Date('2026-01-01T00:00:00Z'),
      to: new Date('2026-01-31T23:59:59Z'),
      page: 1,
      size: 10,
      sortBy: 'timestamp',
      sortDirection: 'asc',
    });
  });

  it('returns not found when a record ID has no match in the authenticated tenant', async () => {
    const { app, repository } = setup();
    const response = await request(app)
      .get('/api/v1/audit')
      .query({ id: '00000000-0000-4000-8000-000000000006' })
      .set('Authorization', 'Bearer valid');

    expect(response.status).toBe(404);
    expect(response.body.code).toBe('AUDIT_NOT_FOUND');
    expect(repository.findPage).toHaveBeenCalledWith(
      auth.tenantId,
      expect.objectContaining({ id: '00000000-0000-4000-8000-000000000006' }),
    );
  });

  it.each(['post', 'put', 'delete'] as const)(
    'does not expose %s operations for immutable audit history',
    async (method) => {
      const { app } = setup();
      const response = await request(app)[method]('/api/v1/audit');

      expect(response.status).toBe(404);
    },
  );

  it.each([
    ['size', '101'],
    ['page', '-1'],
    ['sort', 'payload,desc'],
    ['from', 'not-a-date'],
    ['id', 'not-a-uuid'],
  ])('rejects invalid %s query values', async (key, value) => {
    const { app, repository } = setup();
    const response = await request(app)
      .get('/api/v1/audit')
      .query({ [key]: value })
      .set('Authorization', 'Bearer valid');

    expect(response.status).toBe(400);
    expect(response.body.code).toBe('AUDIT_VALIDATION_ERROR');
    expect(repository.findPage).not.toHaveBeenCalled();
  });

  it('rejects a date range whose start is after its end', async () => {
    const { app } = setup();
    const response = await request(app)
      .get('/api/v1/audit')
      .query({ from: '2026-02-01T00:00:00Z', to: '2026-01-01T00:00:00Z' })
      .set('Authorization', 'Bearer valid');

    expect(response.status).toBe(400);
  });
});
