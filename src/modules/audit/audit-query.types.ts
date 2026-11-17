import type { AuditEventType } from './audit-event.schema.js';

export type AuditResourceType = 'CASE' | 'TASK' | 'DOCUMENT' | 'APPROVAL';
export type AuditSortField = 'timestamp' | 'createdAt' | 'action' | 'resourceType' | 'actorId';
export type SortDirection = 'asc' | 'desc';

export interface AuditQuery {
  id?: string;
  resourceId?: string;
  actorId?: string;
  eventType?: AuditEventType;
  resourceType?: AuditResourceType;
  from?: Date;
  to?: Date;
  page: number;
  size: number;
  sortBy: AuditSortField;
  sortDirection: SortDirection;
}

export interface AuditPage<T> {
  content: T[];
  page: number;
  size: number;
  totalElements: number;
  totalPages: number;
}
