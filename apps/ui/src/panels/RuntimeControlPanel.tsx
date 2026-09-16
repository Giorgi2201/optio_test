import { useCallback, useEffect, useRef, useState } from 'react';
import { Pause, Play, RefreshCw, RotateCcw } from 'lucide-react';
import type { DLQEntry, PipelineTelemetry } from '@optio/shared';
import { api, ApiError } from '../api/client';
import type { RunnerAction } from '../api/types';
import { ActionLog } from '../components/ActionLog';
import { PanelContainer } from '../components/PanelContainer';
import { Badge, Button, EmptyState, SectionLabel, cx, type Tone } from '../components/primitives';
import { useActionLog } from '../hooks/useActionLog';
import { fmtDuration, fmtInt, fmtUtc, shortId, truncate } from '../lib/format';
import { runnerTone } from './PipelineStatusPanel';

const DLQ_FETCH_LIMIT = 50;

type IncrementalLocalState = 'RUNNING' | 'PAUSED' | 'UNKNOWN';

function errorMessage(err: unknown): string {
  return err instanceof ApiError || err instanceof Error ? err.message : String(err);
}

function sinkTone(target: DLQEntry['sink_target']): Tone {
  switch (target) {
    case 'ELASTICSEARCH':
      return 'warn';
    case 'RABBITMQ':
      return 'neutral';
    default:
      return 'err';
  }
}

function RunnerControls({
  name,
  description,
  status,
  tone,
  detail,
  busy,
  onAction
}: {
  name: string;
  description: string;
  status: string;
  tone: Tone;
  detail: string;
  busy: RunnerAction | null;
  onAction: (action: RunnerAction) => void;
}) {
  const isPaused = status === 'PAUSED';
  const isTerminal = status === 'COMPLETED' || status === 'FAILED';
  return (
    <div className="flex flex-col gap-2 border border-zinc-800 bg-zinc-950/40 p-2.5">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <span className="text-xs font-medium text-zinc-100">{name}</span>
            <Badge tone={tone} dot pulse={status === 'RUNNING'}>
              {status}
            </Badge>
          </div>
          <p className="mt-0.5 text-2xs leading-4 text-zinc-500">{description}</p>
        </div>
      </div>
      <div className="flex items-center justify-between gap-2">
        <span className="font-mono text-2xs tabular-nums text-zinc-500">{detail}</span>
        <div className="flex items-center gap-1.5">
          <Button
            size="xs"
            variant="default"
            disabled={isPaused || isTerminal || busy !== null}
            loading={busy === 'pause'}
            onClick={() => onAction('pause')}
            icon={<Pause className="h-3 w-3" aria-hidden />}
            title={`Pause the ${name.toLowerCase()} runner at the next batch boundary (checkpoint preserved)`}
          >
            Pause
          </Button>
          <Button
            size="xs"
            variant="primary"
            disabled={(!isPaused && status !== 'UNKNOWN' && status !== 'INITIALIZED') || busy !== null}
            loading={busy === 'resume'}
            onClick={() => onAction('resume')}
            icon={<Play className="h-3 w-3" aria-hidden />}
            title={`Resume the ${name.toLowerCase()} runner from its last committed checkpoint`}
          >
            Resume
          </Button>
        </div>
      </div>
    </div>
  );
}

export function RuntimeControlPanel({
  telemetry,
  isConnected,
  onTelemetryRefresh
}: {
  telemetry: PipelineTelemetry | null;
  isConnected: boolean;
  onTelemetryRefresh: () => Promise<void>;
}) {
  const { entries: logEntries, log, clear } = useActionLog();

  const [backfillBusy, setBackfillBusy] = useState<RunnerAction | null>(null);
  const [incrementalBusy, setIncrementalBusy] = useState<RunnerAction | null>(null);
  // The telemetry contract does not expose the incremental runner state, so track the last command locally.
  const [incrementalState, setIncrementalState] = useState<IncrementalLocalState>('UNKNOWN');

  const [dlqEntries, setDlqEntries] = useState<DLQEntry[]>([]);
  const [dlqLoading, setDlqLoading] = useState(false);
  const [dlqError, setDlqError] = useState<string | null>(null);
  const [retryingId, setRetryingId] = useState<number | null>(null);
  const [retryingAll, setRetryingAll] = useState(false);
  const lastPendingCountRef = useRef<number | null>(null);

  const loadDlq = useCallback(async () => {
    setDlqLoading(true);
    try {
      const response = await api.getDLQEntries(DLQ_FETCH_LIMIT);
      setDlqEntries(response.entries);
      setDlqError(null);
    } catch (err: unknown) {
      setDlqError(errorMessage(err));
    } finally {
      setDlqLoading(false);
    }
  }, []);

  // Refresh the DLQ list whenever the daemon reports a different pending count.
  useEffect(() => {
    if (!isConnected || !telemetry) {
      return;
    }
    const pending = telemetry.dlq_pending_count;
    if (lastPendingCountRef.current !== pending) {
      lastPendingCountRef.current = pending;
      void loadDlq();
    }
  }, [telemetry, isConnected, loadDlq]);

  const runControl = async (runner: 'backfill' | 'incremental', action: RunnerAction) => {
    const setBusy = runner === 'backfill' ? setBackfillBusy : setIncrementalBusy;
    setBusy(action);
    try {
      const response = runner === 'backfill' ? await api.controlBackfill(action) : await api.controlIncremental(action);
      if (runner === 'incremental') {
        setIncrementalState(response.status);
      }
      log('ok', `${runner}.${action} → ${response.status}`);
      await onTelemetryRefresh();
    } catch (err: unknown) {
      log('error', `${runner}.${action} failed: ${errorMessage(err)}`);
    } finally {
      setBusy(null);
    }
  };

  const retryOne = async (id: number) => {
    setRetryingId(id);
    try {
      const response = await api.retryDLQ(id);
      if (response.success) {
        log('ok', `dlq#${id} replayed and RESOLVED`);
      } else {
        log('warn', `dlq#${id} replay failed: ${response.error ?? 'sink rejected record'}`);
      }
    } catch (err: unknown) {
      log('error', `dlq#${id} replay error: ${errorMessage(err)}`);
    } finally {
      setRetryingId(null);
      await Promise.all([loadDlq(), onTelemetryRefresh()]);
    }
  };

  const retryAll = async () => {
    setRetryingAll(true);
    try {
      const summary = await api.retryAllDLQ();
      const level = summary.failed === 0 ? 'ok' : summary.resolved > 0 ? 'warn' : 'error';
      log(level, `dlq.retry-all → retried=${summary.retried} resolved=${summary.resolved} failed=${summary.failed}${summary.error ? ` (${summary.error})` : ''}`);
    } catch (err: unknown) {
      log('error', `dlq.retry-all error: ${errorMessage(err)}`);
    } finally {
      setRetryingAll(false);
      await Promise.all([loadDlq(), onTelemetryRefresh()]);
    }
  };

  const backfillStatus = telemetry?.backfill_status ?? 'UNKNOWN';
  const pendingCount = telemetry?.dlq_pending_count ?? dlqEntries.length;
  const controlsDisabled = !isConnected;

  return (
    <PanelContainer
      index="03"
      title="RUNTIME_CONTROL"
      subtitle="Pause / resume runners · dead letter queue replay"
      actions={
        <Badge tone={pendingCount > 0 ? 'warn' : 'ok'} dot>
          DLQ:{fmtInt(pendingCount)}
        </Badge>
      }
    >
      <fieldset disabled={controlsDisabled} className="contents">
        <div className="grid gap-2 lg:grid-cols-2">
          <RunnerControls
            name="Backfill"
            description="Historical keyset-paginated replay from PostgreSQL. Pausing stops at the next batch boundary; the committed checkpoint is preserved."
            status={backfillStatus}
            tone={runnerTone(telemetry?.backfill_status)}
            detail={telemetry ? `cursor ${fmtInt(telemetry.backfill_cursor)} · ${fmtInt(telemetry.backfill_total_records)} total` : '—'}
            busy={backfillBusy}
            onAction={(action) => void runControl('backfill', action)}
          />
          <RunnerControls
            name="Incremental CDC"
            description="Watermark poller replicating new mutations. Pausing lets lag accumulate; resuming drains it from the saved watermark."
            status={incrementalState}
            tone={incrementalState === 'RUNNING' ? 'ok' : incrementalState === 'PAUSED' ? 'warn' : 'neutral'}
            detail={telemetry ? `lag ${fmtInt(telemetry.incremental_lag_records)} rows · ${fmtDuration(telemetry.incremental_lag_ms)}` : '—'}
            busy={incrementalBusy}
            onAction={(action) => void runControl('incremental', action)}
          />
        </div>

        <div className="flex min-h-0 flex-1 flex-col gap-1.5">
          <SectionLabel
            right={
              <div className="flex items-center gap-1.5">
                <Button size="xs" variant="ghost" onClick={() => void loadDlq()} loading={dlqLoading} icon={<RefreshCw className="h-3 w-3" aria-hidden />} aria-label="Reload DLQ">
                  Reload
                </Button>
                <Button
                  size="xs"
                  variant="primary"
                  disabled={dlqEntries.length === 0 || retryingId !== null}
                  loading={retryingAll}
                  onClick={() => void retryAll()}
                  icon={<RotateCcw className="h-3 w-3" aria-hidden />}
                  title="Replay every PENDING entry to its target sink"
                >
                  Retry all
                </Button>
              </div>
            }
          >
            Dead letter queue · pending (oldest first, max {DLQ_FETCH_LIMIT})
          </SectionLabel>

          {dlqError && <div className="border border-rose-900/60 bg-rose-950/30 px-2 py-1 font-mono text-2xs text-rose-300">DLQ_UNAVAILABLE — {dlqError}</div>}

          <div className="min-h-[120px] max-h-56 overflow-auto border border-zinc-800 bg-zinc-950/40">
            {dlqEntries.length === 0 ? (
              <EmptyState>{dlqLoading ? 'Loading dead letter queue…' : 'No pending poison records — every batch fully acknowledged'}</EmptyState>
            ) : (
              <table className="w-full border-collapse">
                <thead>
                  <tr>
                    {['id', 'record', 'sink', 'error_code', 'message', 'retries', 'created_at', ''].map((h, i) => (
                      <th
                        key={h || `col-${i}`}
                        className="sticky top-0 z-10 whitespace-nowrap border-b border-zinc-800 bg-zinc-900 px-2 py-1 text-left font-mono text-2xs font-normal uppercase tracking-wider text-zinc-500"
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {dlqEntries.map((entry) => (
                    <tr key={entry.id} className="align-top hover:bg-zinc-900/70">
                      <td className="border-b border-zinc-900 px-2 py-1 font-mono text-2xs text-zinc-400">{entry.id}</td>
                      <td className="border-b border-zinc-900 px-2 py-1 font-mono text-2xs text-zinc-300" title={entry.record_uuid ?? undefined}>
                        {entry.record_id ?? '—'} <span className="text-zinc-600">{shortId(entry.record_uuid)}</span>
                      </td>
                      <td className="border-b border-zinc-900 px-2 py-1">
                        <Badge tone={sinkTone(entry.sink_target)}>{entry.sink_target}</Badge>
                      </td>
                      <td className="border-b border-zinc-900 px-2 py-1 font-mono text-2xs text-rose-400">{truncate(entry.error_code, 24)}</td>
                      <td className="max-w-[260px] border-b border-zinc-900 px-2 py-1 font-mono text-2xs text-zinc-400" title={entry.error_message}>
                        <span className="block truncate">{entry.error_message}</span>
                      </td>
                      <td className={cx('border-b border-zinc-900 px-2 py-1 text-right font-mono text-2xs tabular-nums', entry.retry_count > 0 ? 'text-amber-500' : 'text-zinc-400')}>
                        {entry.retry_count}
                      </td>
                      <td className="border-b border-zinc-900 px-2 py-1 font-mono text-2xs text-zinc-500" title={entry.last_retried_at ? `last retry ${fmtUtc(entry.last_retried_at)}` : undefined}>
                        {fmtUtc(entry.created_at)}
                      </td>
                      <td className="border-b border-zinc-900 px-2 py-1 text-right">
                        <Button
                          size="xs"
                          variant="default"
                          disabled={retryingAll || (retryingId !== null && retryingId !== entry.id)}
                          loading={retryingId === entry.id}
                          onClick={() => void retryOne(entry.id)}
                          icon={<RotateCcw className="h-3 w-3" aria-hidden />}
                        >
                          Retry
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </fieldset>

      {controlsDisabled && <span className="font-mono text-2xs uppercase text-rose-500">controls locked — daemon unreachable</span>}

      <ActionLog entries={logEntries} onClear={clear} />
    </PanelContainer>
  );
}
