import { useState, type ReactNode } from 'react';
import { Biohazard, Unplug, Zap } from 'lucide-react';
import type { PipelineTelemetry } from '@optio/shared';
import { api, ApiError } from '../api/client';
import type { SinkName } from '../api/types';
import { ActionLog } from '../components/ActionLog';
import { PanelContainer } from '../components/PanelContainer';
import { Badge, Button, cx, inputClass, selectClass } from '../components/primitives';
import { useActionLog } from '../hooks/useActionLog';
import { fmtDuration, fmtInt } from '../lib/format';
import { breakerTone } from './PipelineStatusPanel';

const OUTAGE_DURATIONS_MS = [5_000, 10_000, 30_000, 60_000] as const;
const MUTATION_PRESETS = [10, 100, 1_000, 5_000] as const;
const MAX_MUTATIONS = 50_000;

function errorMessage(err: unknown): string {
  return err instanceof ApiError || err instanceof Error ? err.message : String(err);
}

function Experiment({
  icon,
  title,
  gate,
  description,
  expect,
  children
}: {
  icon: ReactNode;
  title: string;
  gate: string;
  description: string;
  expect: string;
  children: ReactNode;
}) {
  return (
    <div className="flex flex-col gap-2 border border-zinc-800 bg-zinc-950/40 p-2.5">
      <div className="flex items-start justify-between gap-2">
        <div className="flex min-w-0 items-start gap-2">
          <span className="mt-0.5 text-zinc-500">{icon}</span>
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <span className="text-xs font-medium text-zinc-100">{title}</span>
              <span className="font-mono text-2xs uppercase text-zinc-600">{gate}</span>
            </div>
            <p className="mt-0.5 text-2xs leading-4 text-zinc-500">{description}</p>
            <p className="mt-0.5 text-2xs leading-4 text-zinc-600">
              <span className="font-mono uppercase text-zinc-500">expect</span> {expect}
            </p>
          </div>
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-2">{children}</div>
    </div>
  );
}

export function ChaosStudioPanel({
  telemetry,
  isConnected,
  onTelemetryRefresh
}: {
  telemetry: PipelineTelemetry | null;
  isConnected: boolean;
  onTelemetryRefresh: () => Promise<void>;
}) {
  const { entries: logEntries, log, clear } = useActionLog();

  const [sink, setSink] = useState<SinkName>('elasticsearch');
  const [durationMs, setDurationMs] = useState<number>(OUTAGE_DURATIONS_MS[1]);
  const [tripping, setTripping] = useState(false);

  const [injecting, setInjecting] = useState(false);

  const [mutationCount, setMutationCount] = useState<string>(String(MUTATION_PRESETS[1]));
  const [mutating, setMutating] = useState(false);

  const parsedMutations = Number.parseInt(mutationCount, 10);
  const mutationsValid = Number.isInteger(parsedMutations) && parsedMutations >= 1 && parsedMutations <= MAX_MUTATIONS;

  const breakers = telemetry?.circuit_breakers;
  const selectedBreaker = sink === 'elasticsearch' ? breakers?.elasticsearch : breakers?.rabbitmq;

  const trip = async () => {
    setTripping(true);
    try {
      const response = await api.tripBreaker(sink, durationMs);
      log('warn', `breaker.trip ${response.target} → OPEN for ${fmtDuration(response.durationMs)} (ingestion pauses, no busy-loop)`);
      await onTelemetryRefresh();
    } catch (err: unknown) {
      log('error', `breaker.trip failed: ${errorMessage(err)}`);
    } finally {
      setTripping(false);
    }
  };

  const inject = async () => {
    setInjecting(true);
    try {
      const response = await api.injectCorruption();
      log('warn', `corruption.inject → source record #${response.recordId} (${response.uuid}) — expect DLQ +1, batch continues`);
      await onTelemetryRefresh();
    } catch (err: unknown) {
      log('error', `corruption.inject failed: ${errorMessage(err)}`);
    } finally {
      setInjecting(false);
    }
  };

  const mutate = async () => {
    if (!mutationsValid) {
      return;
    }
    setMutating(true);
    try {
      const response = await api.generateMutations(parsedMutations);
      log('ok', `load.mutate-source → ${fmtInt(response.mutatedCount)} rows updated (version+1, updated_at=now) — watch CDC lag drain`);
      await onTelemetryRefresh();
    } catch (err: unknown) {
      log('error', `load.mutate-source failed: ${errorMessage(err)}`);
    } finally {
      setMutating(false);
    }
  };

  return (
    <PanelContainer
      index="04"
      title="CHAOS_STUDIO"
      subtitle="Outage, corruption and load triggers against the live pipeline"
      actions={
        <>
          <Badge tone={breakerTone(breakers?.elasticsearch.state)} dot pulse={breakers?.elasticsearch.state === 'OPEN'}>
            ES:{breakers?.elasticsearch.state ?? '—'}
          </Badge>
          <Badge tone={breakerTone(breakers?.rabbitmq.state)} dot pulse={breakers?.rabbitmq.state === 'OPEN'}>
            RMQ:{breakers?.rabbitmq.state ?? '—'}
          </Badge>
        </>
      }
    >
      <fieldset disabled={!isConnected} className="contents">
        <div className="grid gap-2">
          <Experiment
            icon={<Unplug className="h-3.5 w-3.5" aria-hidden />}
            title="Sink outage"
            gate="gate 3"
            description="Force a sink circuit breaker OPEN for a fixed window. Ingestion halts with backpressure; no checkpoints advance until the sink acknowledges again."
            expect="breaker → OPEN, throughput → 0, then HALF_OPEN → CLOSED and lag drains with zero duplicates."
          >
            <select className={selectClass} value={sink} onChange={(e) => setSink(e.target.value as SinkName)} aria-label="Target sink">
              <option value="elasticsearch">SINK: ELASTICSEARCH</option>
              <option value="rabbitmq">SINK: RABBITMQ</option>
            </select>
            <select className={selectClass} value={durationMs} onChange={(e) => setDurationMs(Number(e.target.value))} aria-label="Outage duration">
              {OUTAGE_DURATIONS_MS.map((ms) => (
                <option key={ms} value={ms}>
                  {fmtDuration(ms)}
                </option>
              ))}
            </select>
            <Button variant="danger" onClick={() => void trip()} loading={tripping} icon={<Unplug className="h-3 w-3" aria-hidden />}>
              Trip breaker
            </Button>
            <span className="ml-auto font-mono text-2xs text-zinc-500">
              current <span className={cx(breakerTone(selectedBreaker?.state) === 'ok' ? 'text-emerald-500' : breakerTone(selectedBreaker?.state) === 'err' ? 'text-rose-500' : 'text-amber-500')}>{selectedBreaker?.state ?? '—'}</span>
              {selectedBreaker?.totalTrips !== undefined && <> · trips {fmtInt(selectedBreaker.totalTrips)}</>}
            </span>
          </Experiment>

          <Experiment
            icon={<Biohazard className="h-3.5 w-3.5" aria-hidden />}
            title="Poison record"
            gate="gate 4"
            description="Insert one deliberately malformed source row (string balance, invalid tier). The sink rejects it and the record is isolated to the DLQ."
            expect="DLQ pending +1 while the rest of the batch commits. Replay it from [03] RUNTIME_CONTROL."
          >
            <Button variant="danger" onClick={() => void inject()} loading={injecting} icon={<Biohazard className="h-3 w-3" aria-hidden />}>
              Inject corruption
            </Button>
            <span className="ml-auto font-mono text-2xs text-zinc-500">
              dlq pending <span className={cx((telemetry?.dlq_pending_count ?? 0) > 0 ? 'text-amber-500' : 'text-zinc-300')}>{fmtInt(telemetry?.dlq_pending_count)}</span>
            </span>
          </Experiment>

          <Experiment
            icon={<Zap className="h-3.5 w-3.5" aria-hidden />}
            title="Mutation burst"
            gate="gate 5"
            description="Update N random source rows (version+1, new updated_at) to generate real CDC traffic."
            expect="CDC lag rises then drains; throughput spikes; documents in [02] show incremented version."
          >
            <div className="flex items-center gap-1">
              {MUTATION_PRESETS.map((preset) => (
                <Button key={preset} size="xs" variant={mutationCount === String(preset) ? 'default' : 'ghost'} onClick={() => setMutationCount(String(preset))}>
                  {fmtInt(preset)}
                </Button>
              ))}
            </div>
            <input
              type="number"
              min={1}
              max={MAX_MUTATIONS}
              step={1}
              className={cx(inputClass, 'w-24', !mutationsValid && 'border-rose-700')}
              value={mutationCount}
              onChange={(e) => setMutationCount(e.target.value)}
              aria-label="Mutation count"
              title={`1 – ${fmtInt(MAX_MUTATIONS)} rows`}
            />
            <Button variant="primary" onClick={() => void mutate()} loading={mutating} disabled={!mutationsValid} icon={<Zap className="h-3 w-3" aria-hidden />}>
              Generate mutations
            </Button>
            <span className="ml-auto font-mono text-2xs text-zinc-500">
              lag <span className="text-zinc-300">{fmtInt(telemetry?.incremental_lag_records)}</span> rows
            </span>
          </Experiment>
        </div>
      </fieldset>

      {!isConnected && <span className="font-mono text-2xs uppercase text-rose-500">triggers locked — daemon unreachable</span>}

      <ActionLog entries={logEntries} onClear={clear} />
    </PanelContainer>
  );
}
