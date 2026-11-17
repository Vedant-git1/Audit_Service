import type { Prisma, PrismaClient } from '@prisma/client';
import type { AuditPage, AuditQuery } from './audit-query.types.js';

const auditRecordSelect = {
  id: true,
  eventId: true,
  tenantId: true,
  actorId: true,
  action: true,
  eventVersion: true,
  resourceType: true,
  resourceId: true,
  timestamp: true,
  oldValue: true,
  newValue: true,
  requestId: true,
  traceId: true,
  createdAt: true,
} satisfies Prisma.AuditLogSelect;

export type AuditRecord = Prisma.AuditLogGetPayload<{
  select: typeof auditRecordSelect;
}>;

export interface AuditQueryRepository {
  findPage(
    tenantId: string,
    query: AuditQuery,
  ): Promise<AuditPage<AuditRecord>>;
}

export function createAuditQueryRepository(
  prisma: PrismaClient,
): AuditQueryRepository {
  return {
    async findPage(tenantId, query) {
      const where: Prisma.AuditLogWhereInput = {
        tenantId,
        ...(query.id ? { id: query.id } : {}),
        ...(query.resourceId ? { resourceId: query.resourceId } : {}),
        ...(query.actorId ? { actorId: query.actorId } : {}),
        ...(query.eventType ? { action: query.eventType } : {}),
        ...(query.resourceType ? { resourceType: query.resourceType } : {}),
        ...(query.from || query.to
          ? {
              timestamp: {
                ...(query.from ? { gte: query.from } : {}),
                ...(query.to ? { lte: query.to } : {}),
              },
            }
          : {}),
      };
      const orderBy: Prisma.AuditLogOrderByWithRelationInput = {
        [query.sortBy]: query.sortDirection,
      };
      const skip = query.page * query.size;

      const [content, totalElements] = await prisma.$transaction([
        prisma.auditLog.findMany({
          where,
          select: auditRecordSelect,
          skip,
          take: query.size,
          orderBy: [orderBy, { id: 'asc' }],
        }),
        prisma.auditLog.count({ where }),
      ]);

      return {
        content,
        page: query.page,
        size: query.size,
        totalElements,
        totalPages: Math.ceil(totalElements / query.size),
      };
    },
  };
}
