import { exportJWK, generateKeyPair, SignJWT } from 'jose';
import {
  createJwksJwtVerifier,
  createLocalJwksJwtVerifier,
} from '../../src/security/jwt.js';

const issuer = 'resolve-authentication-service';
const userId = '00000000-0000-4000-8000-000000000001';
const tenantId = '00000000-0000-4000-8000-000000000002';
const tokenId = '00000000-0000-4000-8000-000000000003';
type SigningKey = Awaited<ReturnType<typeof generateKeyPair>>['privateKey'];

describe('Resolve JWT verifier', () => {
  async function verifierFor(
    revocationChecker = { isRevoked: jest.fn().mockResolvedValue(false) },
  ) {
    const { publicKey, privateKey } = await generateKeyPair('RS256');
    const jwk = await exportJWK(publicKey);
    jwk.kid = 'audit-test-key';
    jwk.alg = 'RS256';
    return {
      verifier: createLocalJwksJwtVerifier({
        jwks: { keys: [jwk] },
        issuer,
        revocationChecker,
      }),
      privateKey,
      revocationChecker,
    };
  }

  async function sign(
    privateKey: SigningKey,
    claims: Record<string, unknown> = {
      tenantId,
      tokenId,
      type: 'access',
    },
    expiration = Math.floor(Date.now() / 1000) + 60,
  ) {
    return new SignJWT(claims)
      .setProtectedHeader({ alg: 'RS256', kid: 'audit-test-key' })
      .setIssuer(issuer)
      .setSubject(userId)
      .setExpirationTime(expiration)
      .sign(privateKey);
  }

  it('accepts a valid Resolve access token and checks its revocation state', async () => {
    const { verifier, privateKey, revocationChecker } = await verifierFor();
    const token = await sign(privateKey);

    await expect(verifier.verify(token)).resolves.toEqual({
      userId,
      tenantId,
      tokenId,
    });
    expect(revocationChecker.isRevoked).toHaveBeenCalledWith(tokenId);
  });

  it('rejects an expired token', async () => {
    const { verifier, privateKey } = await verifierFor();
    const token = await sign(
      privateKey,
      undefined,
      Math.floor(Date.now() / 1000) - 60,
    );

    await expect(verifier.verify(token)).rejects.toMatchObject({
      statusCode: 401,
    });
  });

  it('rejects tokens without required Resolve claims', async () => {
    const { verifier, privateKey } = await verifierFor();
    const token = await sign(privateKey, { type: 'access' });

    await expect(verifier.verify(token)).rejects.toMatchObject({
      statusCode: 401,
    });
  });

  it('rejects revoked tokens', async () => {
    const { verifier, privateKey } = await verifierFor({
      isRevoked: jest.fn().mockResolvedValue(true),
    });
    const token = await sign(privateKey);

    await expect(verifier.verify(token)).rejects.toMatchObject({
      statusCode: 401,
    });
  });

  it('surfaces revocation-store outages as unavailable instead of treating them as valid', async () => {
    const { verifier, privateKey } = await verifierFor({
      isRevoked: jest.fn().mockRejectedValue(new Error('Redis unavailable')),
    });
    const token = await sign(privateKey);

    await expect(verifier.verify(token)).rejects.toMatchObject({
      statusCode: 503,
      code: 'AUTH_STATE_UNAVAILABLE',
    });
  });

  it('surfaces JWKS endpoint outages as unavailable instead of rejecting a valid token', async () => {
    const { privateKey } = await verifierFor();
    const verifier = createJwksJwtVerifier({
      jwksUrl: 'http://127.0.0.1:1/.well-known/jwks.json',
      issuer,
      revocationChecker: { isRevoked: jest.fn().mockResolvedValue(false) },
    });
    const token = await sign(privateKey);

    await expect(verifier.verify(token)).rejects.toMatchObject({
      statusCode: 503,
      code: 'AUTH_PROVIDER_UNAVAILABLE',
    });
  });
});
