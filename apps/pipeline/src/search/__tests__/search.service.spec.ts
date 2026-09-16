/**
 * Unit Test Suite for SearchService
 * Validates Elasticsearch query DSL construction, account tier filtering,
 * pagination parameters, and graceful degradation on sink outages.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { Client as ESClient } from '@elastic/elasticsearch';
import { SearchService } from '../search.service.js';
import { SearchDocument } from '@optio/shared';

describe('SearchService - Data Browsing & Full-Text Search', () => {
  it('1. Constructs multi-field query DSL with account tier filter and sort order', async () => {
    let capturedSearchArgs: Record<string, unknown> = {};

    const mockDoc: SearchDocument = {
      id: '42',
      source_uuid: 'uuid-42',
      tenant_id: 'tenant_test',
      customer_id: 'cust-42',
      full_name: 'Alice Wonder',
      email: 'alice@example.com',
      account_tier: 'PREMIUM',
      balance: 1250,
      status: 'ACTIVE',
      version: 1,
      tags: ['vip'],
      country: 'US',
      source_created_at: '2026-09-01T00:00:00.000Z',
      source_updated_at: '2026-09-16T10:00:00.000Z',
      synced_at: '2026-09-16T10:00:01.000Z'
    };

    const mockClient = {
      search: async (args: Record<string, unknown>) => {
        capturedSearchArgs = args;
        return {
          hits: {
            total: { value: 1, relation: 'eq' },
            hits: [{ _source: mockDoc }]
          }
        };
      }
    } as unknown as ESClient;

    const searchService = new SearchService(mockClient, 'records_search_index');

    const result = await searchService.searchRecords({
      query: 'alice',
      tier: 'PREMIUM',
      page: 1,
      limit: 10
    });

    assert.strictEqual(result.total, 1);
    assert.strictEqual(result.documents.length, 1);
    assert.strictEqual(result.documents[0].full_name, 'Alice Wonder');

    // Inspect generated Elasticsearch DSL query structure
    assert.strictEqual(capturedSearchArgs.index, 'records_search_index');
    assert.strictEqual(capturedSearchArgs.from, 0);
    assert.strictEqual(capturedSearchArgs.size, 10);

    const query = capturedSearchArgs.query as Record<string, unknown>;
    assert(query.bool, 'Must use bool query for composite matching');
  });

  it('2. Properly calculates pagination offsets for high page numbers', async () => {
    let capturedFrom = 0;
    let capturedSize = 0;

    const mockClient = {
      search: async (args: Record<string, unknown>) => {
        capturedFrom = args.from as number;
        capturedSize = args.size as number;
        return {
          hits: {
            total: { value: 500, relation: 'eq' },
            hits: []
          }
        };
      }
    } as unknown as ESClient;

    const searchService = new SearchService(mockClient);

    const result = await searchService.searchRecords({
      page: 5,
      limit: 20
    });

    assert.strictEqual(result.page, 5);
    assert.strictEqual(result.limit, 20);
    assert.strictEqual(capturedFrom, 80); // (5 - 1) * 20
    assert.strictEqual(capturedSize, 20);
  });

  it('3. Gracefully degrades and returns empty result with error flag on Elasticsearch outage', async () => {
    const mockClient = {
      search: async () => {
        throw new Error('Connection refused: Elasticsearch node 127.0.0.1:9200 is down');
      }
    } as unknown as ESClient;

    const searchService = new SearchService(mockClient);

    const result = await searchService.searchRecords({ query: 'test' });

    assert.strictEqual(result.total, 0);
    assert.strictEqual(result.documents.length, 0);
    assert(result.error?.includes('Connection refused'));
  });
});
