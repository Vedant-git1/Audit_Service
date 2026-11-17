import type { ErrorRequestHandler, RequestHandler } from 'express';

export class AppError extends Error {
  constructor(
    readonly statusCode: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = 'AppError';
  }
}

export const notFoundHandler: RequestHandler = (_request, _response, next) => {
  next(new AppError(404, 'ROUTE_NOT_FOUND', 'Route not found'));
};

export const errorHandler: ErrorRequestHandler = (error, request, response, _next) => {
  const knownError = error instanceof AppError;
  const status = knownError ? error.statusCode : 500;
  const code = knownError ? error.code : 'INTERNAL_SERVER_ERROR';
  const message = knownError ? error.message : 'Internal server error';
  const path = request.originalUrl.split('?')[0] ?? request.path;

  if (knownError) {
    request.log.warn({ status, errorCode: code }, 'Request rejected');
  } else {
    request.log.error({ err: error, status, errorCode: code }, 'Unhandled request error');
  }

  response.status(status).json({
    timestamp: new Date().toISOString(),
    status,
    code,
    message,
    path,
    requestId: response.locals.requestId,
    traceId: response.locals.traceId,
  });
};
