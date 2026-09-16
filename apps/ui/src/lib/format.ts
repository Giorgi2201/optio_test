/**
 * Deterministic formatting helpers for metrics, identifiers, and timestamps.
 * All output is intended for monospace rendering.
 */

const intFormatter = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
const decimalFormatter = new Intl.NumberFormat('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const moneyFormatter = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

export function fmtInt(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return '—';
  }
  return intFormatter.format(value);
}

export function fmtDecimal(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return '—';
  }
  return decimalFormatter.format(value);
}

export function fmtMoney(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return '—';
  }
  return moneyFormatter.format(value);
}

export function fmtPct(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return '—';
  }
  return `${Math.min(100, Math.max(0, value)).toFixed(1)}%`;
}

/** Compact duration: 850ms, 4.2s, 3m12s, 1h05m. */
export function fmtDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) {
    return '—';
  }
  const abs = Math.abs(ms);
  if (abs < 1_000) {
    return `${Math.round(ms)}ms`;
  }
  if (abs < 60_000) {
    return `${(ms / 1_000).toFixed(1)}s`;
  }
  if (abs < 3_600_000) {
    const minutes = Math.floor(ms / 60_000);
    const seconds = Math.floor((ms % 60_000) / 1_000);
    return `${minutes}m${String(seconds).padStart(2, '0')}s`;
  }
  const hours = Math.floor(ms / 3_600_000);
  const minutes = Math.floor((ms % 3_600_000) / 60_000);
  return `${hours}h${String(minutes).padStart(2, '0')}m`;
}

function pad2(n: number): string {
  return String(n).padStart(2, '0');
}

/** ISO-like UTC timestamp without the T/Z noise: 2026-09-16 07:35:12Z */
export function fmtUtc(input: string | number | Date | null | undefined, withDate = true): string {
  if (input === null || input === undefined) {
    return '—';
  }
  const date = input instanceof Date ? input : new Date(input);
  if (Number.isNaN(date.getTime())) {
    return '—';
  }
  const time = `${pad2(date.getUTCHours())}:${pad2(date.getUTCMinutes())}:${pad2(date.getUTCSeconds())}Z`;
  if (!withDate) {
    return time;
  }
  return `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())} ${time}`;
}

export function truncate(value: string | null | undefined, max: number): string {
  if (!value) {
    return '—';
  }
  return value.length > max ? `${value.slice(0, Math.max(0, max - 1))}…` : value;
}

/** Shorten a UUID to its first block for dense tables: 3f9a1c2e… */
export function shortId(value: string | null | undefined): string {
  if (!value) {
    return '—';
  }
  return value.length > 8 ? `${value.slice(0, 8)}…` : value;
}
