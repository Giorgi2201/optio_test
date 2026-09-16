import { useCallback, useEffect, useRef, useState, type FormEvent } from 'react';
import { ChevronLeft, ChevronRight, RefreshCw, Search } from 'lucide-react';
import type { AccountTier, SearchDocument } from '@optio/shared';
import { api, ApiError } from '../api/client';
import type { SearchResponse } from '../api/types';
import { PanelContainer } from '../components/PanelContainer';
import { Badge, Button, EmptyState, cx, inputClass, selectClass, type Tone } from '../components/primitives';
import { fmtInt, fmtMoney, fmtUtc, truncate } from '../lib/format';

const PAGE_SIZE = 25;
const LIVE_REFRESH_MS = 5_000;
const TIERS: Array<AccountTier | 'ALL'> = ['ALL', 'STANDARD', 'PREMIUM', 'ENTERPRISE'];

function tierTone(tier: AccountTier): Tone {
  switch (tier) {
    case 'ENTERPRISE':
      return 'ok';
    case 'PREMIUM':
      return 'warn';
    default:
      return 'neutral';
  }
}

function statusTone(status: SearchDocument['status']): Tone {
  switch (status) {
    case 'ACTIVE':
      return 'ok';
    case 'SUSPENDED':
      return 'warn';
    case 'ARCHIVED':
      return 'neutral';
    default:
      return 'neutral';
  }
}

const th = 'sticky top-0 z-10 whitespace-nowrap border-b border-zinc-800 bg-zinc-900 px-2 py-1 text-left font-mono text-2xs font-normal uppercase tracking-wider text-zinc-500';
const td = 'whitespace-nowrap border-b border-zinc-900 px-2 py-1 font-mono text-2xs tabular-nums';

export function DataBrowserPanel({ isConnected }: { isConnected: boolean }) {
  const [draftQuery, setDraftQuery] = useState('');
  const [query, setQuery] = useState('');
  const [tier, setTier] = useState<AccountTier | 'ALL'>('ALL');
  const [page, setPage] = useState(1);
  const [live, setLive] = useState(true);

  const [result, setResult] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fetchedAt, setFetchedAt] = useState<Date | null>(null);

  const requestSeq = useRef(0);

  const runSearch = useCallback(async () => {
    const seq = ++requestSeq.current;
    setLoading(true);
    try {
      const response = await api.searchRecords({
        q: query || undefined,
        tier: tier === 'ALL' ? undefined : tier,
        page,
        limit: PAGE_SIZE
      });
      if (seq !== requestSeq.current) {
        return;
      }
      setResult(response);
      setError(response.error ?? null);
      setFetchedAt(new Date());
    } catch (err: unknown) {
      if (seq !== requestSeq.current) {
        return;
      }
      setError(err instanceof ApiError || err instanceof Error ? err.message : 'Search failed');
    } finally {
      if (seq === requestSeq.current) {
        setLoading(false);
      }
    }
  }, [query, tier, page]);

  // Re-query when parameters change.
  useEffect(() => {
    void runSearch();
  }, [runSearch]);

  // Live mode: refresh the current view periodically so freshly replicated documents show up.
  useEffect(() => {
    if (!live || !isConnected) {
      return;
    }
    const id = window.setInterval(() => {
      void runSearch();
    }, LIVE_REFRESH_MS);
    return () => window.clearInterval(id);
  }, [live, isConnected, runSearch]);

  const submit = (event: FormEvent) => {
    event.preventDefault();
    setPage(1);
    setQuery(draftQuery.trim());
  };

  const total = result?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const documents = result?.documents ?? [];
  const firstRow = total === 0 ? 0 : (page - 1) * PAGE_SIZE + 1;
  const lastRow = Math.min(total, page * PAGE_SIZE);

  return (
    <PanelContainer
      index="02"
      title="DATA_BROWSER"
      subtitle="Elasticsearch index · records_search_index"
      actions={
        <>
          <label className="flex cursor-pointer items-center gap-1.5 font-mono text-2xs uppercase text-zinc-400" title={`Re-run the current query every ${LIVE_REFRESH_MS / 1000}s`}>
            <input type="checkbox" className="h-3 w-3 accent-emerald-500" checked={live} onChange={(e) => setLive(e.target.checked)} />
            Live
          </label>
          <Button size="xs" variant="ghost" onClick={() => void runSearch()} loading={loading} icon={<RefreshCw className="h-3 w-3" aria-hidden />} aria-label="Refresh results">
            Refresh
          </Button>
        </>
      }
      bodyClassName="gap-2"
    >
      <form onSubmit={submit} className="flex items-center gap-2">
        <div className="relative flex-1">
          <Search className="pointer-events-none absolute left-2 top-1/2 h-3 w-3 -translate-y-1/2 text-zinc-600" aria-hidden />
          <input
            className={cx(inputClass, 'pl-6')}
            placeholder="Search name, email, customer_id, tags… (fuzzy)"
            value={draftQuery}
            onChange={(e) => setDraftQuery(e.target.value)}
            aria-label="Full-text search query"
          />
        </div>
        <select className={selectClass} value={tier} onChange={(e) => { setTier(e.target.value as AccountTier | 'ALL'); setPage(1); }} aria-label="Account tier filter">
          {TIERS.map((t) => (
            <option key={t} value={t}>
              {t === 'ALL' ? 'TIER: ALL' : t}
            </option>
          ))}
        </select>
        <Button type="submit" variant="default" icon={<Search className="h-3 w-3" aria-hidden />}>
          Search
        </Button>
      </form>

      <div className="flex items-center justify-between font-mono text-2xs tabular-nums text-zinc-500">
        <span>
          {loading && !result ? 'querying…' : (
            <>
              <span className="text-zinc-200">{fmtInt(total)}</span> docs
              {query && <> · q=<span className="text-zinc-300">&quot;{truncate(query, 32)}&quot;</span></>}
              {tier !== 'ALL' && <> · tier=<span className="text-zinc-300">{tier}</span></>}
            </>
          )}
        </span>
        <span>{fetchedAt ? `fetched ${fmtUtc(fetchedAt, false)}` : ''}</span>
      </div>

      {error && (
        <div className="border border-rose-900/60 bg-rose-950/30 px-2 py-1 font-mono text-2xs text-rose-300">
          SEARCH_DEGRADED — {error}
        </div>
      )}

      <div className="relative min-h-[220px] flex-1 overflow-auto border border-zinc-800 bg-zinc-950/40">
        {documents.length === 0 ? (
          <EmptyState>{loading ? 'Querying index…' : error ? 'Index unavailable' : 'No documents match the current query'}</EmptyState>
        ) : (
          <table className="w-full border-collapse">
            <thead>
              <tr>
                <th className={th}>id</th>
                <th className={th}>customer_id</th>
                <th className={th}>full_name</th>
                <th className={th}>email</th>
                <th className={th}>tier</th>
                <th className={cx(th, 'text-right')}>balance</th>
                <th className={th}>status</th>
                <th className={cx(th, 'text-right')}>ver</th>
                <th className={th}>source_updated_at</th>
                <th className={th}>synced_at</th>
              </tr>
            </thead>
            <tbody>
              {documents.map((doc) => (
                <tr key={doc.id} className="hover:bg-zinc-900/70">
                  <td className={cx(td, 'text-zinc-400')} title={doc.source_uuid}>
                    {doc.id}
                  </td>
                  <td className={cx(td, 'text-zinc-300')}>{truncate(doc.customer_id, 22)}</td>
                  <td className={cx(td, 'font-sans text-xs text-zinc-100')}>{truncate(doc.full_name, 28)}</td>
                  <td className={cx(td, 'text-zinc-400')}>{truncate(doc.email, 30)}</td>
                  <td className={td}>
                    <Badge tone={tierTone(doc.account_tier)}>{doc.account_tier}</Badge>
                  </td>
                  <td className={cx(td, 'text-right text-zinc-200')}>{fmtMoney(doc.balance)}</td>
                  <td className={td}>
                    <Badge tone={statusTone(doc.status)} dot>
                      {doc.status}
                    </Badge>
                  </td>
                  <td className={cx(td, 'text-right text-zinc-400')}>{doc.version}</td>
                  <td className={cx(td, 'text-zinc-400')}>{fmtUtc(doc.source_updated_at)}</td>
                  <td className={cx(td, 'text-zinc-500')}>{fmtUtc(doc.synced_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div className="flex items-center justify-between font-mono text-2xs tabular-nums text-zinc-500">
        <span>
          rows {fmtInt(firstRow)}–{fmtInt(lastRow)} of {fmtInt(total)}
        </span>
        <div className="flex items-center gap-1">
          <Button size="xs" variant="ghost" disabled={page <= 1 || loading} onClick={() => setPage((p) => Math.max(1, p - 1))} icon={<ChevronLeft className="h-3 w-3" aria-hidden />} aria-label="Previous page">
            Prev
          </Button>
          <span className="px-1 text-zinc-300">
            {page} / {totalPages}
          </span>
          <Button size="xs" variant="ghost" disabled={page >= totalPages || loading} onClick={() => setPage((p) => Math.min(totalPages, p + 1))} aria-label="Next page">
            Next
            <ChevronRight className="h-3 w-3" aria-hidden />
          </Button>
        </div>
      </div>
    </PanelContainer>
  );
}
