import { useCallback, useEffect, useRef, useState } from 'react';
import type { PipelineTelemetry } from '@optio/shared';
import { api, ApiError } from '../api/client';

export const TELEMETRY_POLL_INTERVAL_MS = 1_500;

export interface TelemetryState {
  /** Latest successfully fetched telemetry snapshot (retained while disconnected). */
  telemetry: PipelineTelemetry | null;
  /** True until the first poll completes (success or failure). */
  loading: boolean;
  /** False when the last poll failed (daemon unreachable, timeout, or 5xx). */
  isConnected: boolean;
  /** Wall-clock time of the last successful poll. */
  lastUpdated: Date | null;
  /** Human-readable reason for the current disconnected state, if any. */
  error: string | null;
  /** Immediately re-poll, outside the regular interval. */
  refresh: () => Promise<void>;
}

/**
 * Polls `/api/telemetry` on a fixed interval using a self-rescheduling timer so requests never
 * overlap even when the daemon is slow. A failing backend flips `isConnected` to false and keeps
 * the last good snapshot rather than throwing.
 */
export function useTelemetry(intervalMs: number = TELEMETRY_POLL_INTERVAL_MS): TelemetryState {
  const [telemetry, setTelemetry] = useState<PipelineTelemetry | null>(null);
  const [loading, setLoading] = useState(true);
  const [isConnected, setIsConnected] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [error, setError] = useState<string | null>(null);

  const mountedRef = useRef(true);
  const inFlightRef = useRef(false);
  const timerRef = useRef<number | null>(null);

  const poll = useCallback(async () => {
    if (inFlightRef.current) {
      return;
    }
    inFlightRef.current = true;
    try {
      const snapshot = await api.getTelemetry();
      if (!mountedRef.current) {
        return;
      }
      setTelemetry(snapshot);
      setIsConnected(true);
      setError(null);
      setLastUpdated(new Date());
    } catch (err: unknown) {
      if (!mountedRef.current) {
        return;
      }
      setIsConnected(false);
      setError(err instanceof ApiError || err instanceof Error ? err.message : 'Unknown telemetry error');
    } finally {
      inFlightRef.current = false;
      if (mountedRef.current) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;

    const tick = async () => {
      await poll();
      if (mountedRef.current) {
        timerRef.current = window.setTimeout(tick, intervalMs);
      }
    };

    void tick();

    return () => {
      mountedRef.current = false;
      if (timerRef.current !== null) {
        window.clearTimeout(timerRef.current);
        timerRef.current = null;
      }
    };
  }, [poll, intervalMs]);

  const refresh = useCallback(async () => {
    await poll();
  }, [poll]);

  return { telemetry, loading, isConnected, lastUpdated, error, refresh };
}
