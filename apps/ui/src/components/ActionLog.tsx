import { Button, cx, EmptyState, SectionLabel } from './primitives';
import type { ActionLogEntry, ActionLogLevel } from '../hooks/useActionLog';
import { fmtUtc } from '../lib/format';

const levelClass: Record<ActionLogLevel, string> = {
  info: 'text-zinc-400',
  ok: 'text-emerald-500',
  warn: 'text-amber-500',
  error: 'text-rose-500'
};

const levelTag: Record<ActionLogLevel, string> = {
  info: 'INFO',
  ok: ' OK ',
  warn: 'WARN',
  error: 'FAIL'
};

export function ActionLog({ entries, onClear, maxHeightClass = 'max-h-32' }: { entries: ActionLogEntry[]; onClear: () => void; maxHeightClass?: string }) {
  return (
    <div className="flex min-h-0 flex-col gap-1.5">
      <SectionLabel
        right={
          entries.length > 0 && (
            <Button size="xs" variant="ghost" onClick={onClear}>
              Clear
            </Button>
          )
        }
      >
        Action log
      </SectionLabel>
      {entries.length === 0 ? (
        <EmptyState>No actions issued this session</EmptyState>
      ) : (
        <ol className={cx('overflow-y-auto border border-zinc-800 bg-zinc-950/60 font-mono text-2xs leading-5', maxHeightClass)}>
          {entries.map((entry) => (
            <li key={entry.id} className="flex gap-2 border-b border-zinc-900 px-2 last:border-b-0">
              <span className="shrink-0 text-zinc-600">{fmtUtc(entry.at, false)}</span>
              <span className={cx('shrink-0', levelClass[entry.level])}>[{levelTag[entry.level]}]</span>
              <span className="min-w-0 break-words text-zinc-300">{entry.message}</span>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}
