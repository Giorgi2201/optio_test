import { RefreshCw } from 'lucide-react';
import { Badge, Button } from './primitives';
import { fmtUtc } from '../lib/format';

export interface HeaderProps {
  isConnected: boolean;
  loading: boolean;
  lastUpdated: Date | null;
  error: string | null;
  onRefresh: () => void;
  refreshing?: boolean;
}

const PIPELINE_PORT = import.meta.env.VITE_PIPELINE_PORT ?? '3000';

export function Header({ isConnected, loading, lastUpdated, error, onRefresh, refreshing = false }: HeaderProps) {
  const statusTone = loading ? 'neutral' : isConnected ? 'ok' : 'err';
  const statusLabel = loading ? 'CONNECTING' : isConnected ? `ONLINE:${PIPELINE_PORT}` : 'OFFLINE';

  return (
    <header className="sticky top-0 z-20 border-b border-zinc-800 bg-zinc-900/90 backdrop-blur-none">
      <div className="flex h-11 items-center justify-between gap-4 px-4">
        <div className="flex min-w-0 items-center gap-3">
          <span className="font-mono text-xs font-semibold tracking-wider text-zinc-100">
            OPTIO <span className="text-zinc-600">//</span> REPLICATION_ENGINE
          </span>
          <span className="hidden text-2xs uppercase tracking-wider text-zinc-500 sm:inline">Operational Control Console</span>
        </div>

        <div className="flex items-center gap-3">
          <Badge tone={statusTone} dot pulse={isConnected} title={error ?? undefined}>
            {statusLabel}
          </Badge>

          <span className="hidden font-mono text-2xs tabular-nums text-zinc-500 md:inline" title="Last successful telemetry poll (UTC)">
            LAST_POLL <span className="text-zinc-300">{fmtUtc(lastUpdated)}</span>
          </span>

          <Button
            size="xs"
            variant="ghost"
            onClick={onRefresh}
            loading={refreshing}
            icon={<RefreshCw className="h-3 w-3" aria-hidden />}
            aria-label="Refresh telemetry"
            title="Refresh telemetry now"
          >
            Refresh
          </Button>
        </div>
      </div>

      {!loading && !isConnected && (
        <div className="border-t border-rose-900/50 bg-rose-950/30 px-4 py-1 font-mono text-2xs text-rose-300">
          PIPELINE_UNREACHABLE — {error ?? 'no response from daemon'}. Retrying every 1.5s. Displaying last known snapshot.
        </div>
      )}
    </header>
  );
}
