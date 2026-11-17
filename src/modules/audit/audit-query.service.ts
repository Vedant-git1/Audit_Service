import { AppError } from '../../shared/errors.js';
import type { AuditPage } from './audit-query.types.js';
import type { AuditQuery } from './audit-query.types.js';
import type { AuditRecord, AuditQueryRepository } from './audit-query.repository.js';

export interface AuditQueryService {
  list(tenantId: string, query: AuditQuery): Promise<AuditPage<AuditRecord>>;
}

export function createAuditQueryService(
  repository: AuditQueryRepository,
): AuditQueryService {
  return {
    async list(tenantId, query) {
      const result = await repository.findPage(tenantId, query);
      if (query.id && result.totalElements === 0) {
        throw new AppError(404, 'AUDIT_NOT_FOUND', 'Audit record not found');
      }
      return result;
    },
  };
}
