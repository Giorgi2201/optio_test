const TOPOLOGY = [
  { port: ':5432', label: 'PG' },
  { port: ':3000', label: 'PIPELINE' },
  { port: ':9200 / :5672', label: 'ES / RMQ' },
  { port: ':3001', label: 'CONSUMER' }
];

export function Footer() {
  return (
    <footer className="flex flex-wrap items-center justify-between gap-x-6 gap-y-1 border-t border-zinc-800 bg-zinc-950 px-4 py-2 font-mono text-xs text-zinc-400">
      <div className="flex flex-wrap items-center gap-x-1.5">
        <span className="text-zinc-600">ACTIVE_PORTS</span>
        {TOPOLOGY.map((hop, i) => (
          <span key={hop.label} className="flex items-center gap-1.5">
            {i > 0 && <span className="text-zinc-600">-&gt;</span>}
            <span className="text-zinc-300">{hop.port}</span>
            <span className="text-zinc-500">{hop.label}</span>
          </span>
        ))}
      </div>
      <div className="flex items-center gap-1.5">
        <span className="text-zinc-600">ARCHITECTURE_CONTRACT</span>
        <span className="text-zinc-300">EFFECTIVELY-ONCE (IDEMPOTENT SINKS)</span>
      </div>
    </footer>
  );
}
