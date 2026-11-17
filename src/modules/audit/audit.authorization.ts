import type { AuditAuthorizationService } from '../../security/auth.types.js';

export const auditAuthorizationUnavailable: AuditAuthorizationService = {
  async canReadAudit() {
    return false;
  },
};
