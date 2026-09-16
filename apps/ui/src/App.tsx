import { useState } from 'react';
import { Footer } from './components/Footer';
import { Header } from './components/Header';
import { useTelemetry } from './hooks/useTelemetry';
import { ChaosStudioPanel } from './panels/ChaosStudioPanel';
import { DataBrowserPanel } from './panels/DataBrowserPanel';
import { PipelineStatusPanel } from './panels/PipelineStatusPanel';
import { RuntimeControlPanel } from './panels/RuntimeControlPanel';

export default function App() {
  const { telemetry, loading, isConnected, lastUpdated, error, refresh } = useTelemetry();
  const [refreshing, setRefreshing] = useState(false);

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await refresh();
    } finally {
      setRefreshing(false);
    }
  };

  return (
    <div className="flex min-h-screen flex-col bg-zinc-950">
      <Header isConnected={isConnected} loading={loading} lastUpdated={lastUpdated} error={error} onRefresh={() => void handleRefresh()} refreshing={refreshing} />

      <main className="grid flex-1 grid-cols-1 gap-3 p-3 xl:grid-cols-2 xl:grid-rows-2">
        <PipelineStatusPanel telemetry={telemetry} isConnected={isConnected} />
        <DataBrowserPanel isConnected={isConnected} />
        <RuntimeControlPanel telemetry={telemetry} isConnected={isConnected} onTelemetryRefresh={refresh} />
        <ChaosStudioPanel telemetry={telemetry} isConnected={isConnected} onTelemetryRefresh={refresh} />
      </main>

      <Footer />
    </div>
  );
}
