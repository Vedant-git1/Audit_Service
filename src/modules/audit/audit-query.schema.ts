import { z } from 'zod';
import { getAuditEventTypes } from './audit-event.schema.js';
import type { AuditQuery, AuditResourceType, AuditSortField, SortDirection } from './audit-query.types.js';

const uuid = z.string().uuid();
const resourceTypes = ['CASE', 'TASK', 'DOCUMENT', 'APPROVAL'] as const;
const sortFields = ['timestamp', 'createdAt', 'action', 'resourceType', 'actorId'] as const;
const sortDirections = ['asc', 'desc'] as const;
const sortValues = sortFields.flatMap((field) =>
  sortDirections.map((direction) => `${field},${direction}`),
);

const querySchema = z
  .object({
    id: uuid.optional(),
    resourceId: uuid.optional(),
    actorId: uuid.optional(),
    eventType: z.enum(getAuditEventTypes() as [string, ...string[]]).optional(),
    resourceType: z.enum(resourceTypes).optional(),
    from: z.string().datetime({ offset: true }).optional(),
    to: z.string().datetime({ offset: true }).optional(),
    page: z.coerce.number().int().min(0).max(100_000).default(0),
    size: z.coerce.number().int().min(1).max(100).default(20),
    sort: z.enum(sortValues as [string, ...string[]]).default('timestamp,desc'),
  })
  .strict()
  .superRefine((query, context) => {
    if (query.from && query.to && Date.parse(query.from) > Date.parse(query.to)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['from'],
        message: 'from must be earlier than or equal to to',
      });
    }
  });

export function parseAuditQuery(value: unknown): AuditQuery {
  const result = querySchema.safeParse(value);
  if (!result.success) {
    throw result.error;
  }

  const [sortBy, sortDirection] = result.data.sort.split(',') as [
    AuditSortField,
    SortDirection,
  ];
  return {
    ...(result.data.id ? { id: result.data.id } : {}),
    ...(result.data.resourceId ? { resourceId: result.data.resourceId } : {}),
    ...(result.data.actorId ? { actorId: result.data.actorId } : {}),
    ...(result.data.eventType ? { eventType: result.data.eventType as AuditQuery['eventType'] } : {}),
    ...(result.data.resourceType
      ? { resourceType: result.data.resourceType as AuditResourceType }
      : {}),
    ...(result.data.from ? { from: new Date(result.data.from) } : {}),
    ...(result.data.to ? { to: new Date(result.data.to) } : {}),
    page: result.data.page,
    size: result.data.size,
    sortBy,
    sortDirection,
  };
}
