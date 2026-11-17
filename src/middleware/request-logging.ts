import pinoHttp from 'pino-http';
import type { Logger } from 'pino';

export function createRequestLoggingMiddleware(logger: Logger) {
  return pinoHttp({
    logger,
    redact: ['req.headers.authorization', 'req.headers.cookie'],
    genReqId: (request) => {
      const requestId = request.headers['x-request-id'];
      if (typeof requestId !== 'string') {
        throw new Error('Request ID middleware must run before request logging');
      }
      return requestId;
    },
    customProps: (request) => ({
      requestId: request.headers['x-request-id'],
      traceId: request.headers['x-trace-id'],
    }),
  });
}
