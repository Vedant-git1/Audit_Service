import type { AuthContext } from '../security/auth.types.js';
import type { AuditQuery } from '../modules/audit/audit-query.types.js';

declare global {
  namespace Express {
    interface Locals {
      auth?: AuthContext;
      auditQuery?: AuditQuery;
    }
  }
}

export {};
