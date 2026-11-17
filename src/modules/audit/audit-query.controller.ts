import type { RequestHandler } from 'express';
import { AppError } from '../../shared/errors.js';
import type { AuditQueryService } from './audit-query.service.js';

export function createAuditQueryController(service: AuditQueryService): RequestHandler {
  return async (_request, response, next) => {
    try {
      const auth = response.locals.auth;
      const query = response.locals.auditQuery;
      if (!auth || !query) {
        throw new AppError(500, 'INTERNAL_SERVER_ERROR', 'Audit request context is unavailable');
      }
      response.status(200).json(await service.list(auth.tenantId, query));
    } catch (error) {
      next(error);
    }
  };
}
