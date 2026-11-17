import { PrismaClient } from '@prisma/client';
import { createAuditRepository } from '../../src/modules/audit/audit.repository.js';

describe('Audit repository database failures', () => {
  it('propagates PostgreSQL unavailability instead of treating persistence as successful', async () => {
    const prisma = new PrismaClient({
      datasources: {
        db: {
          url: 'postgresql://test:test@127.0.0.1:1/audit_test?connect_timeout=1',
        },
      },
    });

    try {
      await expect(
        createAuditRepository(prisma).persistEvent(
          { topic: 'resolve.case.created', partition: 0, offset: '0' },
          {
            eventId: 'b2e1a4c0-1111-4a2b-9c3d-000000000010',
            eventType: 'CaseCreated',
            eventVersion: 1,
            tenantId: '3fa85f64-5717-4562-b3fc-2c963f66afa6',
            actorId: null,
            resourceType: 'CASE',
            resourceId: 'c1a2b3d4-0000-4000-8000-000000000042',
            timestamp: '2026-09-08T10:00:00Z',
            metadata: { caseId: 'c1a2b3d4-0000-4000-8000-000000000042' },
          },
        ),
      ).rejects.toThrow();
    } finally {
      await prisma.$disconnect();
    }
  });
});
