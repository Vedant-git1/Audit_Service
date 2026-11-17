import type { RequestHandler } from 'express';
import { AppError } from '../shared/errors.js';
import type { AuditAuthorizationService } from '../security/auth.types.js';

export function requireAuditReadAccess(
  authorization: AuditAuthorizationService,
): RequestHandler {
  return async (_request, response, next) => {
    const auth = response.locals.auth;
    if (!auth) {
      next(new AppError(401, 'AUTH_MISSING_TOKEN', 'Authentication is required'));
      return;
    }

    try {
      if (!(await authorization.canReadAudit(auth))) {
        next(new AppError(403, 'AUDIT_FORBIDDEN', 'Caller is not authorized to view audit records'));
        return;
      }
      next();
    } catch (error) {
      next(error);
    }
  };
}
