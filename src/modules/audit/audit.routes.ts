import { Router, type RequestHandler } from 'express';
import { AppError } from '../../shared/errors.js';
import { authenticate } from '../../middleware/authentication.js';
import { requireAuditReadAccess } from '../../middleware/audit-authorization.js';
import type { AuditAuthorizationService, JwtVerifier } from '../../security/auth.types.js';
import { createAuditQueryController } from './audit-query.controller.js';
import type { AuditQueryRepository } from './audit-query.repository.js';
import { createAuditQueryService } from './audit-query.service.js';
import { parseAuditQuery } from './audit-query.schema.js';

function validateAuditQuery(): RequestHandler {
  return (request, response, next) => {
    try {
      response.locals.auditQuery = parseAuditQuery(request.query);
      next();
    } catch (error) {
      if (error instanceof Error && 'issues' in error) {
        next(new AppError(400, 'AUDIT_VALIDATION_ERROR', 'Audit query parameters are invalid'));
        return;
      }
      next(error);
    }
  };
}

export function createAuditRouter(dependencies: {
  repository: AuditQueryRepository;
  verifier: JwtVerifier;
  authorization: AuditAuthorizationService;
}): Router {
  const router = Router();
  const controller = createAuditQueryController(
    createAuditQueryService(dependencies.repository),
  );

  router.get(
    '/',
    authenticate(dependencies.verifier),
    requireAuditReadAccess(dependencies.authorization),
    validateAuditQuery(),
    controller,
  );

  return router;
}
