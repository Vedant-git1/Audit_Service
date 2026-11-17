export interface AuthContext {
  userId: string;
  tenantId: string;
  tokenId: string;
}

export interface JwtVerifier {
  verify(token: string): Promise<AuthContext>;
}

export interface TokenRevocationChecker {
  isRevoked(tokenId: string): Promise<boolean>;
}

export interface AuditAuthorizationService {
  canReadAudit(auth: AuthContext): Promise<boolean>;
}
