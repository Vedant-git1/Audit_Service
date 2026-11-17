import type { PrismaClient } from '@prisma/client';
import { createAuditQueryRepository } from '../../src/modules/audit/audit-query.repository.js';
import type { AuditQuery } from '../../src/modules/audit/audit-query.types.js';

describe('audit query repository', () => {
  it('applies tenant and query filters in the database with bounded sorting and pagination', async () => {
    const findMany = jest.fn().mockResolvedValue([]);
    const count = jest.fn().mockResolvedValue(0);
    const prisma = {
      auditLog: { findMany, count },
      $transaction: jest.fn((queries: Promise<unknown>[]) =>
        Promise.all(queries),
      ),
    } as unknown as PrismaClient;
    const repository = createAuditQueryRepository(prisma);
    const query: AuditQuery = {
      resourceId: '00000000-0000-4000-8000-000000000004',
      actorId: '00000000-0000-4000-8000-000000000005',
      eventType: 'CaseCreated',
      resourceType: 'CASE',
      from: new Date('2026-01-01T00:00:00Z'),
      to: new Date('2026-01-31T23:59:59Z'),
      page: 2,
      size: 10,
      sortBy: 'timestamp',
      sortDirection: 'asc',
    };

    await repository.findPage('00000000-0000-4000-8000-000000000002', query);

    const where = {
      tenantId: '00000000-0000-4000-8000-000000000002',
      resourceId: query.resourceId,
      actorId: query.actorId,
      action: query.eventType,
      resourceType: query.resourceType,
      timestamp: { gte: query.from, lte: query.to },
    };
    expect(findMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where,
        skip: 20,
        take: 10,
        orderBy: [{ timestamp: 'asc' }, { id: 'asc' }],
      }),
    );
    expect(count).toHaveBeenCalledWith({ where });
  });
});
