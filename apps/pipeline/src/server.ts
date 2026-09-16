/**
 * Native HTTP Control & Observability Server
 * Exposes Gate 5 aggregated telemetry, health probes, and dynamic runner controls.
 */

import http, { IncomingMessage, ServerResponse, Server } from 'http';
import { PipelineCoordinator } from './coordinator/pipeline.coordinator.js';
import { DLQStore } from './dlq/dlq.store.js';

export function createPipelineServer(
  coordinator: PipelineCoordinator,
  dlqStore: DLQStore
): Server {
  const server = http.createServer(async (req: IncomingMessage, res: ServerResponse) => {
    // Cross-Origin Resource Sharing (CORS) headers for operational UI
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    const pathname = url.pathname;
    const method = req.method?.toUpperCase();

    try {
      // 1. Health Probe
      if (method === 'GET' && pathname === '/health') {
        const telemetry = await coordinator.getTelemetry();
        const statusCode = telemetry.health.overall === 'DOWN' ? 503 : 200;
        res.writeHead(statusCode, { 'Content-Type': 'application/json' });
        res.end(
          JSON.stringify({
            status: telemetry.health.overall,
            health: telemetry.health,
            timestamp: telemetry.timestamp
          })
        );
        return;
      }

      // 2. Real-Time Telemetry & Metrics (Gate 5)
      if (method === 'GET' && (pathname === '/metrics' || pathname === '/api/telemetry')) {
        const telemetry = await coordinator.getTelemetry();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(telemetry));
        return;
      }

      // 3. Dead Letter Queue Inspection
      if (method === 'GET' && pathname === '/api/dlq') {
        const limitParam = url.searchParams.get('limit');
        const limit = limitParam ? parseInt(limitParam, 10) : 100;
        const entries = await dlqStore.getPendingEntries(isNaN(limit) ? 100 : limit);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ entries, count: entries.length }));
        return;
      }

      // 4. Backfill Control Endpoints
      if (method === 'POST' && pathname === '/api/control/backfill/pause') {
        await coordinator.pauseBackfill();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'PAUSED', pipeline: 'backfill' }));
        return;
      }

      if (method === 'POST' && pathname === '/api/control/backfill/resume') {
        await coordinator.resumeBackfill();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'RUNNING', pipeline: 'backfill' }));
        return;
      }

      // 5. Incremental CDC Control Endpoints
      if (method === 'POST' && pathname === '/api/control/incremental/pause') {
        await coordinator.pauseIncremental();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'PAUSED', pipeline: 'incremental' }));
        return;
      }

      if (method === 'POST' && pathname === '/api/control/incremental/resume') {
        await coordinator.resumeIncremental();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'RUNNING', pipeline: 'incremental' }));
        return;
      }

      // 404 Not Found
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Endpoint not found', path: pathname }));
    } catch (err: unknown) {
      console.error('[HTTP SERVER ERROR]', err);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          error: 'Internal Server Error',
          message: err instanceof Error ? err.message : String(err)
        })
      );
    }
  });

  return server;
}
