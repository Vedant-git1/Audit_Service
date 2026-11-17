import {
  createLocalJWKSet,
  createRemoteJWKSet,
  errors as joseErrors,
  jwtVerify,
  type JWTPayload,
} from 'jose';
import { z } from 'zod';
import { AppError } from '../shared/errors.js';
import type {
  AuthContext,
  JwtVerifier,
  TokenRevocationChecker,
} from './auth.types.js';

interface ResolveJwtPayload extends JWTPayload {
  tenantId?: unknown;
  tokenId?: unknown;
  type?: unknown;
}

const uuid = z.string().uuid();
const invalidToken = () =>
  new AppError(401, 'AUTH_MISSING_TOKEN', 'Authentication token is invalid');

async function toAuthContext(
  payload: ResolveJwtPayload,
  revocationChecker: TokenRevocationChecker,
): Promise<AuthContext> {
  const userId = uuid.safeParse(payload.sub);
  const tenantId = uuid.safeParse(payload.tenantId);
  const tokenId = uuid.safeParse(payload.tokenId);
  if (
    !userId.success ||
    !tenantId.success ||
    !tokenId.success ||
    payload.type !== 'access' ||
    typeof payload.exp !== 'number'
  ) {
    throw invalidToken();
  }

  let revoked: boolean;
  try {
    revoked = await revocationChecker.isRevoked(tokenId.data);
  } catch {
    throw new AppError(
      503,
      'AUTH_STATE_UNAVAILABLE',
      'Authentication state could not be verified',
    );
  }
  if (revoked) throw invalidToken();

  return {
    userId: userId.data,
    tenantId: tenantId.data,
    tokenId: tokenId.data,
  };
}

async function verifyToken(
  token: string,
  keySet: Parameters<typeof jwtVerify>[1],
  options: { issuer: string; audience?: string },
  revocationChecker: TokenRevocationChecker,
): Promise<AuthContext> {
  let payload: ResolveJwtPayload;
  try {
    const result = await jwtVerify<ResolveJwtPayload>(token, keySet, {
      issuer: options.issuer,
      ...(options.audience ? { audience: options.audience } : {}),
      algorithms: ['RS256'],
    });
    payload = result.payload;
  } catch (error) {
    if (error instanceof joseErrors.JOSEError) {
      throw invalidToken();
    }
    throw new AppError(
      503,
      'AUTH_PROVIDER_UNAVAILABLE',
      'Authentication signing keys could not be verified',
    );
  }
  return toAuthContext(payload, revocationChecker);
}

export function createJwksJwtVerifier(options: {
  jwksUrl: string;
  issuer: string;
  audience?: string;
  revocationChecker: TokenRevocationChecker;
}): JwtVerifier {
  const jwks = createRemoteJWKSet(new URL(options.jwksUrl));
  const verifyOptions = {
    issuer: options.issuer,
    ...(options.audience ? { audience: options.audience } : {}),
  };

  return {
    verify: (token) =>
      verifyToken(token, jwks, verifyOptions, options.revocationChecker),
  };
}

export function createLocalJwksJwtVerifier(options: {
  jwks: Parameters<typeof createLocalJWKSet>[0];
  issuer: string;
  audience?: string;
  revocationChecker: TokenRevocationChecker;
}): JwtVerifier {
  const jwks = createLocalJWKSet(options.jwks);
  const verifyOptions = {
    issuer: options.issuer,
    ...(options.audience ? { audience: options.audience } : {}),
  };

  return {
    verify: (token) =>
      verifyToken(token, jwks, verifyOptions, options.revocationChecker),
  };
}
