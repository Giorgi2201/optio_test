import { useEffect, useRef, useState } from 'react';

/**
 * Keeps a fixed-length ring of recent numeric samples for sparkline rendering.
 * Appends only when `stamp` changes so repeated renders of the same snapshot are ignored.
 */
export function useMetricHistory(value: number | null | undefined, stamp: string | undefined, capacity = 48): number[] {
  const [history, setHistory] = useState<number[]>([]);
  const lastStampRef = useRef<string | undefined>(undefined);

  useEffect(() => {
    if (stamp === undefined || stamp === lastStampRef.current) {
      return;
    }
    lastStampRef.current = stamp;
    const sample = typeof value === 'number' && Number.isFinite(value) ? value : 0;
    setHistory((prev) => {
      const next = prev.length >= capacity ? prev.slice(prev.length - capacity + 1) : prev.slice();
      next.push(sample);
      return next;
    });
  }, [value, stamp, capacity]);

  return history;
}
