import { useCallback, useState } from 'react';

export type ActionLogLevel = 'info' | 'ok' | 'warn' | 'error';

export interface ActionLogEntry {
  id: number;
  at: Date;
  level: ActionLogLevel;
  message: string;
}

const DEFAULT_CAPACITY = 25;

let sequence = 0;

/**
 * Bounded, newest-first log of operator actions and their outcomes. Capacity is fixed so the
 * console never accumulates unbounded state during long sessions.
 */
export function useActionLog(capacity: number = DEFAULT_CAPACITY) {
  const [entries, setEntries] = useState<ActionLogEntry[]>([]);

  const log = useCallback(
    (level: ActionLogLevel, message: string) => {
      sequence += 1;
      const entry: ActionLogEntry = { id: sequence, at: new Date(), level, message };
      setEntries((prev) => [entry, ...prev].slice(0, capacity));
    },
    [capacity]
  );

  const clear = useCallback(() => setEntries([]), []);

  return { entries, log, clear };
}
