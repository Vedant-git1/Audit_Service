import type { RequestHandler } from 'express';
import { AppError } from '../shared/errors.js';
import type { JwtVerifier } from '../security/auth.types.js';

export function authenticate(verifier: JwtVerifier): RequestHandler {
  return async (request, response, next) => {
    const authorization = request.header('authorization');
    if (!authorization || !/^Bearer\s+\S+$/i.test(authorization)) {
      next(new AppError(401, 'AUTH_MISSING_TOKEN', 'Authentication is required'));
      return;
    }

    try {
      response.locals.auth = await verifier.verify(authorization.slice(7).trim());
      next();
    } catch (error) {
      next(
        error instanceof AppError
          ? error
          : new AppError(401, 'AUTH_MISSING_TOKEN', 'Authentication token is invalid'),
      );
    }
  };
}
