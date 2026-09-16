import { useId } from 'react';

/**
 * Minimal bar sparkline. Pure SVG, no gradients, no glow — a flat metric strip like a Grafana stat panel.
 */
export function Sparkline({
  data,
  capacity,
  height = 28,
  className,
  colorClass = 'fill-emerald-500'
}: {
  data: number[];
  capacity: number;
  height?: number;
  className?: string;
  colorClass?: string;
}) {
  const titleId = useId();
  const max = Math.max(1, ...data);
  const slotWidth = 100 / capacity;
  const barWidth = Math.max(0.6, slotWidth * 0.7);
  const offset = capacity - data.length;

  return (
    <svg
      viewBox={`0 0 100 ${height}`}
      preserveAspectRatio="none"
      className={className}
      style={{ height }}
      role="img"
      aria-labelledby={titleId}
    >
      <title id={titleId}>Throughput history (last {capacity} samples)</title>
      <line x1="0" y1={height - 0.5} x2="100" y2={height - 0.5} className="stroke-zinc-800" strokeWidth="0.5" />
      {data.map((value, i) => {
        const h = value <= 0 ? 0.75 : Math.max(0.75, (value / max) * (height - 2));
        const x = (offset + i) * slotWidth + (slotWidth - barWidth) / 2;
        return (
          <rect
            key={i}
            x={x}
            y={height - 1 - h}
            width={barWidth}
            height={h}
            className={value <= 0 ? 'fill-zinc-700' : colorClass}
          />
        );
      })}
    </svg>
  );
}
