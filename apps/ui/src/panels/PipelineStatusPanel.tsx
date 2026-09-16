import type { CircuitBreakerTelemetry, ComponentHealth, PipelineStatus, PipelineTelemetry } from '@optio/shared';
import { PanelContainer } from '../components/PanelContainer';
import { Badge, EmptyState, KeyValue, Metric, SectionLabel, StatusDot, cx, toneText, type Tone } from '../components/primitives';
import { Sparkline } from '../components/Sparkline';
import { useMetricHistory } from '../hooks/useMetricHistory';
import { fmtDecimal, fmtDuration, fmtInt, fmtPct, fmtUtc } from '../lib/format';

const HISTORY_CAPACITY = 48;

export function healthTone(status: ComponentHealth['status'] | PipelineTelemetry['health']['overall'] | undefined): Tone {
  switch (status) {
    case 'UP':
    case 'HEALTHY':
      return 'ok';
    case 'DEGRADED':
      return 'warn';
    case 'DOWN':
      return 'err';
    default:
      return 'neutral';
  }
}

export function runnerTone(status: PipelineStatus | undefined): Tone {
  switch (status) {
    case 'RUNNING':
    case 'COMPLETED':
      return 'ok';
    case 'PAUSED':
    case 'INITIALIZED':
      return 'warn';
    case 'FAILED':
      return 'err';
    default:
      return 'neutral';
  }
}

export function breakerTone(state: CircuitBreakerTelemetry['state'] | undefined): Tone {
  switch (state) {
    case 'CLOSED':
      return 'ok';
    case 'HALF_OPEN':
      return 'warn';
    case 'OPEN':
      return 'err';
    default:
      return 'neutral';
  }
}

function lagTone(lagRecords: number, lagMs: number): Tone {
  if (lagRecords > 5_000 || lagMs > 60_000) {
    return 'err';
  }
  if (lagRecords > 500 || lagMs > 10_000) {
    return 'warn';
  }
  return 'ok';
}

function HealthRow({ name, port, health }: { name: string; port: string; health: ComponentHealth | undefined }) {
  const tone = healthTone(health?.status);
  return (
    <div className="flex items-center justify-between gap-2 border border-zinc-800 bg-zinc-950/40 px-2 py-1.5" title={health?.message}>
      <div className="flex min-w-0 items-center gap-2">
        <StatusDot tone={tone} />
        <span className="truncate text-xs text-zinc-200">{name}</span>
        <span className="font-mono text-2xs text-zinc-600">{port}</span>
      </div>
      <div className="flex items-center gap-2 font-mono text-2xs tabular-nums">
        <span className="text-zinc-500">{health ? fmtDuration(health.latency_ms) : '—'}</span>
        <span className={cx('w-[68px] text-right uppercase', tone === 'neutral' ? 'text-zinc-500' : toneText[tone])}>
          {health?.status ?? 'UNKNOWN'}
        </span>
      </div>
    </div>
  );
}

function BreakerCard({ label, breaker }: { label: string; breaker: CircuitBreakerTelemetry | undefined }) {
  const tone = breakerTone(breaker?.state);
  return (
    <div className="flex flex-col gap-1 border border-zinc-800 bg-zinc-950/40 px-2.5 py-2">
      <div className="flex items-center justify-between gap-2">
        <span className="text-xs text-zinc-200">{label}</span>
        <Badge tone={tone} dot pulse={breaker?.state === 'OPEN'}>
          {breaker?.state ?? 'UNKNOWN'}
        </Badge>
      </div>
      <div className="grid grid-cols-2 gap-x-3">
        <KeyValue k="failures" v={fmtInt(breaker?.consecutiveFailures ?? 0)} tone={(breaker?.consecutiveFailures ?? 0) > 0 ? 'warn' : 'neutral'} />
        <KeyValue k="trips" v={fmtInt(breaker?.totalTrips ?? 0)} />
        <KeyValue k="backoff" v={fmtDuration(breaker?.currentBackoffMs ?? 0)} />
        <KeyValue k="downtime" v={fmtDuration(breaker?.totalDowntimeMs ?? 0)} />
      </div>
      {breaker?.isThrottling && <span className="font-mono text-2xs uppercase text-amber-500">throttling ingestion</span>}
    </div>
  );
}

export function PipelineStatusPanel({ telemetry, isConnected }: { telemetry: PipelineTelemetry | null; isConnected: boolean }) {
  const throughputHistory = useMetricHistory(telemetry?.current_throughput_eps, telemetry?.timestamp, HISTORY_CAPACITY);

  const overall = telemetry?.health.overall;
  const backfillStatus = telemetry?.backfill_status;
  const pct = telemetry?.backfill_completion_pct ?? 0;
  const lag = telemetry ? lagTone(telemetry.incremental_lag_records, telemetry.incremental_lag_ms) : 'neutral';

  return (
    <PanelContainer
      index="01"
      title="PIPELINE_STATUS"
      subtitle="Health, throughput, replication lag, circuit breakers"
      actions={
        <>
          <Badge tone={healthTone(overall)} dot pulse={overall === 'HEALTHY'}>
            {overall ?? 'UNKNOWN'}
          </Badge>
          <Badge tone={telemetry?.status === 'RUNNING' ? 'ok' : telemetry?.status === 'STOPPED' ? 'err' : 'neutral'}>
            COORD:{telemetry?.status ?? '—'}
          </Badge>
        </>
      }
    >
      {!telemetry ? (
        <EmptyState>{isConnected ? 'Awaiting first telemetry snapshot' : 'Pipeline daemon unreachable — no telemetry received yet'}</EmptyState>
      ) : (
        <>
          {/* Backfill progress */}
          <div className="flex flex-col gap-1.5">
            <SectionLabel
              right={
                <Badge tone={runnerTone(backfillStatus)} dot pulse={backfillStatus === 'RUNNING'}>
                  {backfillStatus ?? 'UNKNOWN'}
                </Badge>
              }
            >
              Historical backfill
            </SectionLabel>
            <div className="h-2 w-full border border-zinc-800 bg-zinc-950" role="progressbar" aria-valuenow={Math.round(pct)} aria-valuemin={0} aria-valuemax={100}>
              <div
                className={cx('h-full transition-[width] duration-500', backfillStatus === 'FAILED' ? 'bg-rose-500' : backfillStatus === 'PAUSED' ? 'bg-amber-500' : 'bg-emerald-500')}
                style={{ width: `${Math.min(100, Math.max(0, pct))}%` }}
              />
            </div>
            <div className="flex items-center justify-between font-mono text-2xs tabular-nums text-zinc-500">
              <span>
                cursor <span className="text-zinc-200">{fmtInt(telemetry.backfill_cursor)}</span> / {fmtInt(telemetry.backfill_total_records)} rows
              </span>
              <span className="text-zinc-200">{fmtPct(pct)}</span>
            </div>
          </div>

          {/* Key metrics */}
          <div className="grid grid-cols-2 gap-2 xl:grid-cols-4">
            <Metric label="Throughput" value={fmtDecimal(telemetry.current_throughput_eps)} unit="ev/s" tone={telemetry.current_throughput_eps > 0 ? 'ok' : 'neutral'} />
            <Metric label="CDC lag" value={fmtInt(telemetry.incremental_lag_records)} unit="rows" tone={lag} hint="Rows in source newer than the incremental watermark" />
            <Metric label="Lag time" value={fmtDuration(telemetry.incremental_lag_ms)} tone={lag} hint="Age of the oldest unreplicated mutation" />
            <Metric label="DLQ pending" value={fmtInt(telemetry.dlq_pending_count)} tone={telemetry.dlq_pending_count > 0 ? 'warn' : 'ok'} hint="Poison records isolated to the dead letter queue" />
          </div>

          <div className="flex flex-col gap-1">
            <div className="flex items-center justify-between font-mono text-2xs text-zinc-500">
              <span className="uppercase tracking-wider">throughput · last {HISTORY_CAPACITY} polls</span>
              <span className="tabular-nums">
                peak <span className="text-zinc-300">{fmtDecimal(throughputHistory.length ? Math.max(...throughputHistory) : 0)}</span> ev/s
              </span>
            </div>
            <Sparkline data={throughputHistory} capacity={HISTORY_CAPACITY} className="w-full border border-zinc-800 bg-zinc-950/60" />
          </div>

          {/* Health + breakers */}
          <div className="grid gap-3 lg:grid-cols-2">
            <div className="flex flex-col gap-1.5">
              <SectionLabel>Dependency health</SectionLabel>
              <div className="flex flex-col gap-1">
                <HealthRow name="PostgreSQL" port=":5432" health={telemetry.health.postgres} />
                <HealthRow name="Elasticsearch" port=":9200" health={telemetry.health.elasticsearch} />
                <HealthRow name="RabbitMQ" port=":5672" health={telemetry.health.rabbitmq} />
              </div>
            </div>
            <div className="flex flex-col gap-1.5">
              <SectionLabel>Circuit breakers</SectionLabel>
              <div className="flex flex-col gap-1">
                <BreakerCard label="Elasticsearch sink" breaker={telemetry.circuit_breakers?.elasticsearch} />
                <BreakerCard label="RabbitMQ sink" breaker={telemetry.circuit_breakers?.rabbitmq} />
              </div>
            </div>
          </div>

          <div className="mt-auto flex items-center justify-between font-mono text-2xs text-zinc-600">
            <span>snapshot_ts {fmtUtc(telemetry.timestamp)}</span>
            {!isConnected && <span className="text-rose-500">STALE — daemon unreachable</span>}
          </div>
        </>
      )}
    </PanelContainer>
  );
}
