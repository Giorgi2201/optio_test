/**
 * Lightweight Observability HTTP Server for Independent Consumer
 * Native Node.js HTTP server (zero extra npm dependencies).
 */

import http from 'http';
import { EventConsumerService } from './consumer.service.js';

export function createConsumerServer(
  consumer: EventConsumerService,
  port = 3001
): http.Server {
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    const method = req.method?.toUpperCase();

    // CORS and JSON Headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.setHeader('Content-Type', 'application/json');

    if (method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    if (method === 'GET' && url.pathname === '/health') {
      const health = consumer.healthCheck();
      res.writeHead(health.healthy ? 200 : 503);
      res.end(
        JSON.stringify({
          status: health.healthy ? 'UP' : 'DOWN',
          uptime: process.uptime(),
          health
        })
      );
      return;
    }

    if (method === 'GET' && url.pathname === '/metrics') {
      const metrics = consumer.getMetrics();
      res.writeHead(200);
      res.end(JSON.stringify(metrics));
      return;
    }

    if (method === 'POST' && url.pathname === '/reset') {
      consumer.resetMetrics();
      res.writeHead(200);
      res.end(
        JSON.stringify({
          message: 'Consumer metrics and deduplication store reset successfully.',
          metrics: consumer.getMetrics()
        })
      );
      return;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Endpoint not found' }));
  });

  return server;
}
