import type Redis from 'ioredis';
import type { TokenRevocationChecker } from '../../security/auth.types.js';

export function createRedisRevocationChecker(
  redis: Pick<Redis, 'get'>,
): TokenRevocationChecker {
  return {
    async isRevoked(tokenId) {
      return (await redis.get(`session:revoked:${tokenId}`)) !== null;
    },
  };
}
