/**
 * Native HTTP Control & Observability Server
 * Exposes Gate 5 aggregated telemetry, health probes, dynamic runner controls,
 * DLQ retry management, chaos simulation controls, and search browsing API.
 */

import http, { IncomingMessage, ServerResponse, Server } from 'http';
import { PipelineCoordinator } from './coordinator/pipeline.coordinator.js';
import { DLQStore } from './dlq/dlq.store.js';
import { DLQReplayer } from './dlq/dlq.replayer.js';
import { ChaosService } from './simulation/chaos.service.js';
import { SearchService } from './search/search.service.js';

async function parseJsonBody<T = unknown>(req: IncomingMessage): Promise<T> {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk;
      if (body.length > 1e6) {
        req.destroy();
        reject(new Error('Payload too large'));
      }
    });
    req.on('end', () => {
      if (!body.trim()) {
        resolve({} as T);
        return;
      }
      try {
        resolve(JSON.parse(body) as T);
      } catch (err) {
        reject(new Error('Invalid JSON'));
      }
    });
    req.on('error', reject);
  });
}

export function createPipelineServer(
  coordinator: PipelineCoordinator,
  dlqStore: DLQStore,
  dlqReplayer?: DLQReplayer,
  chaosService?: ChaosService,
  searchService?: SearchService
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

      // 4. DLQ Replay Endpoints
      const dlqRetryMatch = pathname.match(/^\/api\/dlq\/(\d+)\/retry$/);
      if (method === 'POST' && dlqRetryMatch) {
        const dlqId = parseInt(dlqRetryMatch[1], 10);
        if (!dlqReplayer) {
          res.writeHead(503, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: 'DLQ replayer not available' }));
          return;
        }
        const result = await dlqReplayer.retryEntry(dlqId);
        res.writeHead(result.success ? 200 : 400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
        return;
      }

      if (method === 'POST' && pathname === '/api/dlq/retry-all') {
        if (!dlqReplayer) {
          res.writeHead(503, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ retried: 0, resolved: 0, failed: 0, error: 'DLQ replayer not available' }));
          return;
        }
        const summary = await dlqReplayer.retryAllPending();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(summary));
        return;
      }

      // 5. Backfill Control Endpoints
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

      // 6. Incremental CDC Control Endpoints
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

      // 7. Chaos Simulation Endpoints
      if (method === 'POST' && pathname === '/api/simulation/inject-corruption') {
        if (!chaosService) {
          res.writeHead(503, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Chaos service not available' }));
          return;
        }
        const body = await parseJsonBody<{ tenantId?: string }>(req);
        const result = await chaosService.injectCorruptedRecord(body.tenantId);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
        return;
      }

      if (method === 'POST' && pathname === '/api/simulation/mutate-source') {
        if (!chaosService) {
          res.writeHead(503, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Chaos service not available' }));
          return;
        }
        const body = await parseJsonBody<{ count?: number }>(req);
        const count = typeof body.count === 'number' ? body.count : 10;
        const result = await chaosService.generateSourceMutations(count);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
        return;
      }

      if (method === 'POST' && pathname === '/api/simulation/trip-breaker') {
        if (!chaosService) {
          res.writeHead(503, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Chaos service not available' }));
          return;
        }
        const body = await parseJsonBody<{
          sink?: 'elasticsearch' | 'rabbitmq';
          target?: 'elasticsearch' | 'rabbitmq';
          durationMs?: number;
        }>(req);
        const targetSink = body.sink || body.target || 'elasticsearch';
        const duration = typeof body.durationMs === 'number' ? body.durationMs : 10000;
        const result = await chaosService.simulateSinkOutage(targetSink, duration);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
        return;
      }

      // 8. Search & Data Browsing (UI Panel 2)
      if (method === 'GET' && pathname === '/api/search') {
        if (!searchService) {
          res.writeHead(503, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ total: 0, page: 1, limit: 25, documents: [], error: 'Search service not available' }));
          return;
        }
        const q = url.searchParams.get('q') || url.searchParams.get('query') || undefined;
        const tier = url.searchParams.get('tier') || undefined;
        const pageParam = url.searchParams.get('page');
        const limitParam = url.searchParams.get('limit');
        const page = pageParam ? Math.max(1, parseInt(pageParam, 10)) : 1;
        const limit = limitParam ? Math.max(1, Math.min(100, parseInt(limitParam, 10))) : 25;

        const result = await searchService.searchRecords({ query: q, tier, page, limit });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
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
