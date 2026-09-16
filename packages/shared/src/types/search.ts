/**
 * Elasticsearch Sink Document Contract
 * Represents the searchable current-state document indexed with deterministic ID.
 */

import { AccountTier, RecordStatus } from './source.js';

export interface SearchDocument {
  id: string; // Source ID stringified, mapped directly to ES _id
  source_uuid: string;
  tenant_id: string;
  customer_id: string;
  full_name: string; // Pre-joined for full-text search
  email: string;
  account_tier: AccountTier;
  balance: number;
  status: RecordStatus;
  version: number;
  tags: string[];
  country: string;
  source_created_at: string;
  source_updated_at: string;
  synced_at: string;
}
