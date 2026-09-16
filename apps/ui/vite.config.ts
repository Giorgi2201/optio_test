import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

/**
 * Vite build & dev-server configuration for the OPTIO operational console.
 *
 * The dev server proxies `/api/*` and `/health` to the pipeline HTTP daemon so the
 * browser only ever talks to relative paths (no CORS, no hardcoded hosts in app code).
 * Override the upstream with VITE_API_PROXY_TARGET in a `.env` file when the daemon
 * runs elsewhere.
 */
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const proxyTarget = env.VITE_API_PROXY_TARGET || 'http://localhost:3000';

  return {
    plugins: [react()],
    server: {
      port: 4000,
      strictPort: true,
      host: true,
      proxy: {
        '/api': {
          target: proxyTarget,
          changeOrigin: true
        },
        '/health': {
          target: proxyTarget,
          changeOrigin: true
        }
      }
    },
    preview: {
      port: 4000,
      strictPort: true
    },
    build: {
      sourcemap: false,
      target: 'es2022'
    }
  };
});
