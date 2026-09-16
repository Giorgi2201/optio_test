/**
 * Elasticsearch Index Schema & Lifecycle Provisioner
 */

import { Client } from '@elastic/elasticsearch';

export const SEARCH_INDEX_MAPPING = {
  properties: {
    id: { type: 'keyword' as const },
    source_uuid: { type: 'keyword' as const },
    tenant_id: { type: 'keyword' as const },
    customer_id: { type: 'keyword' as const },
    full_name: {
      type: 'text' as const,
      fields: {
        keyword: { type: 'keyword' as const }
      }
    },
    email: { type: 'keyword' as const },
    account_tier: { type: 'keyword' as const },
    balance: { type: 'double' as const },
    status: { type: 'keyword' as const },
    version: { type: 'long' as const },
    tags: { type: 'keyword' as const },
    country: { type: 'keyword' as const },
    source_created_at: { type: 'date' as const },
    source_updated_at: { type: 'date' as const },
    synced_at: { type: 'date' as const }
  }
};

export const SEARCH_INDEX_SETTINGS = {
  number_of_shards: 1,
  number_of_replicas: 0,
  refresh_interval: '1s'
};

/**
 * Ensures that the target Elasticsearch index exists with production mappings and settings.
 * If the index does not exist, it is created. Returns true if newly created or already exists.
 */
export async function ensureElasticsearchIndex(
  client: Client,
  indexName: string
): Promise<boolean> {
  const exists = await client.indices.exists({ index: indexName });
  if (exists) {
    return true;
  }

  await client.indices.create({
    index: indexName,
    settings: SEARCH_INDEX_SETTINGS,
    mappings: SEARCH_INDEX_MAPPING
  });

  return true;
}
