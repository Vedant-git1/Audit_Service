export interface AuditEventRecord {
  id: string;
  organizationId: string;
  actorId: string | null;
  action: string;
  resourceType: string;
  resourceId: string;
  oldValue: unknown | null;
  newValue: unknown | null;
  requestId: string | null;
  traceId: string | null;
  createdAt: Date;
}
