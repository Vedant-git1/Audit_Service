import { randomUUID } from 'node:crypto';
import type { RequestHandler } from 'express';

const requestIdPattern = /^[A-Za-z0-9._:-]{1,100}$/;
const traceIdPattern = /^[0-9a-f]{32}$/i;

export const requestIdMiddleware: RequestHandler = (request, response, next) => {
  const suppliedRequestId = request.header('x-request-id');
  const requestId =
    suppliedRequestId && requestIdPattern.test(suppliedRequestId) ? suppliedRequestId : randomUUID();
  const traceparentId = request.header('traceparent')?.split('-')[1];
  const suppliedTraceId = request.header('x-trace-id');
  const traceId =
    (suppliedTraceId && traceIdPattern.test(suppliedTraceId) ? suppliedTraceId : undefined) ??
    (traceparentId && traceIdPattern.test(traceparentId) ? traceparentId : undefined) ??
    randomUUID().replaceAll('-', '');

  request.headers['x-request-id'] = requestId;
  request.headers['x-trace-id'] = traceId;
  response.locals.requestId = requestId;
  response.locals.traceId = traceId;
  response.setHeader('x-request-id', requestId);
  response.setHeader('x-trace-id', traceId);
  next();
};
