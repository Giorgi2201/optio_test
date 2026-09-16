/**
 * Typed HTTP client for the pipeline control & observability API.
 *
 * All calls use relative paths (`/api/...`, `/health`) so the Vite dev proxy or a reverse
 * proxy in front of the static bundle decides where the daemon lives. An absolute base can
 * be supplied with VITE_API_BASE_URL. Every request carries a fail-fast timeout.
 */

import type {
  ControlResponse,
  DLQListResponse,
  DLQRetryAllResponse,
  DLQRetryResponse,
  HealthResponse,
  InjectCorruptionResponse,
  MutateSourceResponse,
  PipelineTelemetry,
  RunnerAction,
  SearchQuery,
  SearchResponse,
  SinkName,
  TripBreakerResponse
} from './types';

const DEFAULT_TIMEOUT_MS = 8_000;
const API_BASE = (import.meta.env.VITE_API_BASE_URL ?? '').replace(/\/+$/, '');

export class ApiError extends Error {
  public readonly status: number;
  public readonly path: string;
  public readonly body: unknown;

  constructor(message: string, status: number, path: string, body: unknown) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.path = path;
    this.body = body;
  }
}

function extractErrorMessage(body: unknown, fallback: string): string {
  if (body && typeof body === 'object') {
    const record = body as Record<string, unknown>;
    const candidate = record.message ?? record.error;
    if (typeof candidate === 'string' && candidate.trim()) {
      return candidate;
    }
  }
  return fallback;
}

async function request<T>(
  path: string,
  init: { method?: 'GET' | 'POST'; body?: unknown; timeoutMs?: number } = {}
): Promise<T> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), init.timeoutMs ?? DEFAULT_TIMEOUT_MS);

  try {
    const response = await fetch(`${API_BASE}${path}`, {
      method: init.method ?? 'GET',
      headers: init.body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
      body: init.body !== undefined ? JSON.stringify(init.body) : undefined,
      signal: controller.signal,
      cache: 'no-store'
    });

    const text = await response.text();
    let parsed: unknown = null;
    if (text) {
      try {
        parsed = JSON.parse(text) as unknown;
      } catch {
        parsed = text;
      }
    }

    if (!response.ok) {
      throw new ApiError(
        extractErrorMessage(parsed, `HTTP ${response.status} ${response.statusText}`),
        response.status,
        path,
        parsed
      );
    }

    return parsed as T;
  } catch (err: unknown) {
    if (err instanceof ApiError) {
      throw err;
    }
    if (err instanceof DOMException && err.name === 'AbortError') {
      throw new ApiError(`Request timed out after ${init.timeoutMs ?? DEFAULT_TIMEOUT_MS}ms`, 0, path, null);
    }
    const message = err instanceof Error ? err.message : String(err);
    throw new ApiError(`Network error: ${message}`, 0, path, null);
  } finally {
    window.clearTimeout(timer);
  }
}

function buildQuery(params: Record<string, string | number | undefined>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== '') {
      search.set(key, String(value));
    }
  }
  const encoded = search.toString();
  return encoded ? `?${encoded}` : '';
}

export const api = {
  // --- Observability -------------------------------------------------------
  getTelemetry(): Promise<PipelineTelemetry> {
    return request<PipelineTelemetry>('/api/telemetry', { timeoutMs: 4_000 });
  },

  getHealth(): Promise<HealthResponse> {
    return request<HealthResponse>('/health', { timeoutMs: 4_000 });
  },

  // --- Data browsing -------------------------------------------------------
  searchRecords(params: SearchQuery = {}): Promise<SearchResponse> {
    return request<SearchResponse>(`/api/search${buildQuery({ q: params.q, tier: params.tier, page: params.page, limit: params.limit })}`);
  },

  // --- Dead letter queue ---------------------------------------------------
  getDLQEntries(limit = 100): Promise<DLQListResponse> {
    return request<DLQListResponse>(`/api/dlq${buildQuery({ limit })}`);
  },

  retryDLQ(id: number): Promise<DLQRetryResponse> {
    return request<DLQRetryResponse>(`/api/dlq/${encodeURIComponent(String(id))}/retry`, { method: 'POST' });
  },

  retryAllDLQ(): Promise<DLQRetryAllResponse> {
    return request<DLQRetryAllResponse>('/api/dlq/retry-all', { method: 'POST', timeoutMs: 60_000 });
  },

  // --- Runner control ------------------------------------------------------
  controlBackfill(action: RunnerAction): Promise<ControlResponse> {
    return request<ControlResponse>(`/api/control/backfill/${action}`, { method: 'POST' });
  },

  controlIncremental(action: RunnerAction): Promise<ControlResponse> {
    return request<ControlResponse>(`/api/control/incremental/${action}`, { method: 'POST' });
  },

  // --- Chaos simulation ----------------------------------------------------
  injectCorruption(tenantId?: string): Promise<InjectCorruptionResponse> {
    return request<InjectCorruptionResponse>('/api/simulation/inject-corruption', {
      method: 'POST',
      body: tenantId ? { tenantId } : {}
    });
  },

  generateMutations(count: number): Promise<MutateSourceResponse> {
    return request<MutateSourceResponse>('/api/simulation/mutate-source', {
      method: 'POST',
      body: { count },
      timeoutMs: 30_000
    });
  },

  tripBreaker(sink: SinkName, durationMs: number): Promise<TripBreakerResponse> {
    return request<TripBreakerResponse>('/api/simulation/trip-breaker', {
      method: 'POST',
      body: { sink, durationMs }
    });
  }
};

export type ApiClient = typeof api;
