import type { ReactNode } from 'react';
import { cx } from './primitives';

export interface PanelContainerProps {
  /** Two-digit panel index rendered as a monospace badge, e.g. "01". */
  index: string;
  /** Upper-snake panel identifier, e.g. "PIPELINE_STATUS". */
  title: string;
  /** Optional human-readable subtitle. */
  subtitle?: string;
  /** Right-aligned header action slot (buttons, badges, toggles). */
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  bodyClassName?: string;
}

/**
 * Dense, clean panel container. Solid 1px zinc border, muted raised surface, minimal radius.
 */
export function PanelContainer({ index, title, subtitle, actions, children, className, bodyClassName }: PanelContainerProps) {
  return (
    <section
      className={cx('flex min-h-0 flex-col rounded-sm border border-zinc-800 bg-zinc-900/50', className)}
      aria-labelledby={`panel-${index}-title`}
    >
      <header className="flex h-9 shrink-0 items-center justify-between gap-3 border-b border-zinc-800 px-3">
        <div className="flex min-w-0 items-center gap-2">
          <span className="rounded-sm border border-zinc-700 bg-zinc-950 px-1 font-mono text-2xs text-zinc-400">[{index}]</span>
          <h2 id={`panel-${index}-title`} className="truncate font-mono text-xs font-semibold tracking-wider text-zinc-100">
            {title}
          </h2>
          {subtitle && <span className="hidden truncate text-2xs text-zinc-500 lg:inline">{subtitle}</span>}
        </div>
        {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
      </header>
      <div className={cx('flex min-h-0 flex-1 flex-col gap-3 p-3', bodyClassName)}>{children}</div>
    </section>
  );
}
