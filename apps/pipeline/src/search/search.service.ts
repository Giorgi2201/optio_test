/**
 * Search & Data Browsing Service
 * Powers UI Panel 2 for real-time document search, customer lookups, and tier filtering.
 */

import { Client, estypes } from '@elastic/elasticsearch';
import { SearchDocument } from '@optio/shared';

export interface SearchParams {
  query?: string;
  tier?: string;
  page?: number;
  limit?: number;
}

export interface SearchResult {
  total: number;
  page: number;
  limit: number;
  documents: SearchDocument[];
  error?: string;
}

export class SearchService {
  private readonly client: Client;
  private readonly indexName: string;

  constructor(client: Client, indexName = 'records_search_index') {
    this.client = client;
    this.indexName = indexName;
  }

  /**
   * Searches documents with multi-field queries, account tier filters, and keyset-consistent sorting.
   */
  public async searchRecords(params: SearchParams = {}): Promise<SearchResult> {
    const page = Math.max(1, params.page || 1);
    const limit = Math.max(1, Math.min(100, params.limit || 25));
    const from = (page - 1) * limit;

    const mustClauses: estypes.QueryDslQueryContainer[] = [];
    const filterClauses: estypes.QueryDslQueryContainer[] = [];

    if (params.query && params.query.trim()) {
      mustClauses.push({
        multi_match: {
          query: params.query.trim(),
          fields: ['full_name^2', 'email^2', 'customer_id', 'tags'],
          fuzziness: 'AUTO'
        }
      });
    }

    if (params.tier && params.tier !== 'ALL') {
      filterClauses.push({
        term: {
          account_tier: params.tier
        }
      });
    }

    const esQuery: estypes.QueryDslQueryContainer =
      mustClauses.length > 0 || filterClauses.length > 0
        ? {
            bool: {
              must: mustClauses.length > 0 ? mustClauses : [{ match_all: {} }],
              filter: filterClauses
            }
          }
        : { match_all: {} };

    try {
      const response = await this.client.search<SearchDocument>({
        index: this.indexName,
        from,
        size: limit,
        query: esQuery,
        sort: [
          { source_updated_at: { order: 'desc', unmapped_type: 'date' } },
          { id: { order: 'desc', unmapped_type: 'keyword' } }
        ] as unknown as estypes.Sort
      });

      const total =
        typeof response.hits.total === 'number'
          ? response.hits.total
          : response.hits.total?.value ?? 0;

      const documents: SearchDocument[] = response.hits.hits
        .map((hit) => hit._source)
        .filter((doc): doc is SearchDocument => doc !== undefined);

      return {
        total,
        page,
        limit,
        documents
      };
    } catch (err: unknown) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      console.warn(`[SEARCH SERVICE] Elasticsearch search query degraded: ${errorMsg}`);
      return {
        total: 0,
        page,
        limit,
        documents: [],
        error: errorMsg
      };
    }
  }
}
