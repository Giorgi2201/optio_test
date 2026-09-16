import type { ButtonHTMLAttributes, ReactNode } from 'react';
import { Loader2 } from 'lucide-react';

/**
 * Semantic tone → strictly functional colors.
 *  ok      emerald-500  UP / healthy / running
 *  warn    amber-500    degraded / lag / paused / half-open
 *  err     rose-500     down / outage / failed / open breaker
 *  neutral zinc-400     informational
 */
export type Tone = 'ok' | 'warn' | 'err' | 'neutral';

export const toneText: Record<Tone, string> = {
  ok: 'text-emerald-500',
  warn: 'text-amber-500',
  err: 'text-rose-500',
  neutral: 'text-zinc-400'
};

export const toneBg: Record<Tone, string> = {
  ok: 'bg-emerald-500',
  warn: 'bg-amber-500',
  err: 'bg-rose-500',
  neutral: 'bg-zinc-500'
};

export const toneBorder: Record<Tone, string> = {
  ok: 'border-emerald-500/40',
  warn: 'border-amber-500/40',
  err: 'border-rose-500/40',
  neutral: 'border-zinc-700'
};

function cx(...classes: Array<string | false | null | undefined>): string {
  return classes.filter(Boolean).join(' ');
}

export function StatusDot({ tone, pulse = false }: { tone: Tone; pulse?: boolean }) {
  return (
    <span className="relative inline-flex h-1.5 w-1.5 shrink-0" aria-hidden>
      {pulse && tone !== 'neutral' && (
        <span className={cx('absolute inline-flex h-full w-full animate-ping rounded-full opacity-60', toneBg[tone])} />
      )}
      <span className={cx('relative inline-flex h-1.5 w-1.5 rounded-full', toneBg[tone])} />
    </span>
  );
}

export function Badge({
  tone = 'neutral',
  children,
  dot = false,
  pulse = false,
  className,
  title
}: {
  tone?: Tone;
  children: ReactNode;
  dot?: boolean;
  pulse?: boolean;
  className?: string;
  title?: string;
}) {
  return (
    <span
      title={title}
      className={cx(
        'inline-flex items-center gap-1.5 rounded-sm border bg-zinc-950/60 px-1.5 py-0.5 font-mono text-2xs uppercase tracking-wide',
        toneBorder[tone],
        toneText[tone],
        className
      )}
    >
      {dot && <StatusDot tone={tone} pulse={pulse} />}
      {children}
    </span>
  );
}

type ButtonVariant = 'default' | 'primary' | 'danger' | 'ghost';

const buttonVariants: Record<ButtonVariant, string> = {
  default:
    'border-zinc-700 bg-zinc-900 text-zinc-200 hover:border-zinc-600 hover:bg-zinc-800 disabled:hover:bg-zinc-900 disabled:hover:border-zinc-700',
  primary:
    'border-emerald-600/60 bg-emerald-950/40 text-emerald-400 hover:bg-emerald-900/40 hover:border-emerald-500/70 disabled:hover:bg-emerald-950/40 disabled:hover:border-emerald-600/60',
  danger:
    'border-rose-600/60 bg-rose-950/40 text-rose-400 hover:bg-rose-900/40 hover:border-rose-500/70 disabled:hover:bg-rose-950/40 disabled:hover:border-rose-600/60',
  ghost: 'border-transparent bg-transparent text-zinc-400 hover:text-zinc-100 hover:bg-zinc-800'
};

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: 'xs' | 'sm';
  loading?: boolean;
  icon?: ReactNode;
}

export function Button({
  variant = 'default',
  size = 'sm',
  loading = false,
  icon,
  className,
  children,
  disabled,
  type = 'button',
  ...rest
}: ButtonProps) {
  return (
    <button
      type={type}
      disabled={disabled || loading}
      className={cx(
        'inline-flex items-center justify-center gap-1.5 whitespace-nowrap rounded-sm border font-mono uppercase tracking-wide transition-colors',
        'focus:outline-none focus-visible:ring-1 focus-visible:ring-zinc-500',
        'disabled:cursor-not-allowed disabled:opacity-40',
        size === 'xs' ? 'h-6 px-2 text-2xs' : 'h-7 px-2.5 text-xs',
        buttonVariants[variant],
        className
      )}
      {...rest}
    >
      {loading ? <Loader2 className="h-3 w-3 animate-spin" aria-hidden /> : icon}
      {children}
    </button>
  );
}

export function Metric({
  label,
  value,
  unit,
  tone = 'neutral',
  hint,
  className
}: {
  label: string;
  value: ReactNode;
  unit?: string;
  tone?: Tone;
  hint?: string;
  className?: string;
}) {
  return (
    <div className={cx('flex min-w-0 flex-col gap-0.5 border border-zinc-800 bg-zinc-950/40 px-2.5 py-2', className)} title={hint}>
      <span className="truncate text-2xs uppercase tracking-wider text-zinc-500">{label}</span>
      <span className={cx('flex items-baseline gap-1 font-mono text-base leading-tight tabular-nums', tone === 'neutral' ? 'text-zinc-100' : toneText[tone])}>
        <span className="truncate">{value}</span>
        {unit && <span className="text-2xs text-zinc-500">{unit}</span>}
      </span>
    </div>
  );
}

export function SectionLabel({ children, right }: { children: ReactNode; right?: ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-2 border-b border-zinc-800/80 pb-1">
      <span className="font-mono text-2xs uppercase tracking-wider text-zinc-500">{children}</span>
      {right}
    </div>
  );
}

export function KeyValue({ k, v, tone = 'neutral' }: { k: string; v: ReactNode; tone?: Tone }) {
  return (
    <div className="flex items-center justify-between gap-3 py-0.5">
      <span className="text-xs text-zinc-500">{k}</span>
      <span className={cx('font-mono text-xs tabular-nums', tone === 'neutral' ? 'text-zinc-200' : toneText[tone])}>{v}</span>
    </div>
  );
}

export const inputClass =
  'h-7 w-full rounded-sm border border-zinc-700 bg-zinc-950 px-2 font-mono text-xs text-zinc-200 placeholder:text-zinc-600 focus:border-zinc-500 focus:outline-none disabled:opacity-40';

export const selectClass =
  'h-7 rounded-sm border border-zinc-700 bg-zinc-950 px-1.5 font-mono text-xs text-zinc-200 focus:border-zinc-500 focus:outline-none disabled:opacity-40';

export function EmptyState({ children }: { children: ReactNode }) {
  return (
    <div className="flex h-full min-h-[64px] items-center justify-center border border-dashed border-zinc-800 px-3 py-4 text-center font-mono text-2xs uppercase tracking-wider text-zinc-600">
      {children}
    </div>
  );
}

export { cx };
