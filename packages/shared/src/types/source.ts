/**
 * Source Domain Contracts for PostgreSQL Relational Entities
 */

export type AccountTier = 'STANDARD' | 'PREMIUM' | 'ENTERPRISE';

export type RecordStatus = 'ACTIVE' | 'SUSPENDED' | 'ARCHIVED';

export type SignupChannel = 'WEB' | 'MOBILE_APP' | 'API' | 'PARTNER';

export interface CustomerMetadata {
  signup_channel: SignupChannel;
  country: string;
  tags: string[];
  [key: string]: unknown;
}

export interface CustomerPayload {
  customer_id: string;
  first_name: string;
  last_name: string;
  email: string;
  account_tier: AccountTier;
  balance: number;
  metadata: CustomerMetadata;
}

export interface SourceRecord {
  id: number;
  uuid: string;
  tenant_id: string;
  payload: CustomerPayload;
  version: number;
  status: RecordStatus;
  is_corrupted: boolean;
  created_at: string;
  updated_at: string;
}
