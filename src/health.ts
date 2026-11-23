import { createServer, type Server } from 'node:http';
import { stats } from './processor';

export type Check = () => Promise<boolean> | boolean;

/** Tiny HTTP server for container health checks: GET /health. */
export function startHealthServer(port: number, checks: Record<string, Check>): Server {
  const server = createServer(async (req, res) => {
    if (req.url !== '/health') {
      res.writeHead(404, { 'content-type': 'application/json' }).end(JSON.stringify({ error: { code: 'NOT_FOUND', message: 'Not found' } }));
      return;
    }
    const results: Record<string, 'up' | 'down'> = {};
    for (const [name, check] of Object.entries(checks)) {
      results[name] = (await Promise.resolve().then(check).catch(() => false)) ? 'up' : 'down';
    }
    const ok = Object.values(results).every((v) => v === 'up');
    res
      .writeHead(ok ? 200 : 503, { 'content-type': 'application/json' })
      .end(JSON.stringify({ status: ok ? 'ok' : 'error', service: 'jobs', checks: results, stats }));
  });
  server.listen(port);
  return server;
}
