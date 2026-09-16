# syntax=docker/dockerfile:1
#
# OPTIO Operational Console — production image.
#
# Build context MUST be the monorepo root so workspace dependencies resolve:
#   docker build -f apps/ui/Dockerfile -t optio-ui .
#
# Stage 1 compiles @optio/shared and the Vite bundle; Stage 2 serves the static bundle with Nginx
# and proxies /api + /health to the pipeline daemon (see nginx.conf, PIPELINE_UPSTREAM).

# ------------------------------------------------------------------------------------------
# Stage 1: Builder
# ------------------------------------------------------------------------------------------
FROM node:20-alpine AS builder

WORKDIR /app

ENV NODE_ENV=development \
    NPM_CONFIG_UPDATE_NOTIFIER=false \
    NPM_CONFIG_FUND=false \
    NPM_CONFIG_AUDIT=false

# 1. Copy every workspace manifest first so `npm ci` can be layer-cached independently of source edits.
COPY package.json package-lock.json ./
COPY packages/shared/package.json packages/shared/
COPY apps/ui/package.json apps/ui/
COPY apps/pipeline/package.json apps/pipeline/
COPY apps/consumer/package.json apps/consumer/

# 2. Install only the workspaces this image needs (root + shared + ui), reproducibly from the lockfile.
RUN npm ci --include-workspace-root --workspace=@optio/shared --workspace=@optio/ui

# 3. Copy sources for the two workspaces that participate in the build.
COPY packages/shared/ packages/shared/
COPY apps/ui/ apps/ui/

# 4. Build the shared contracts (type source for the UI) and the production bundle.
RUN npm --workspace=@optio/shared run build \
 && npm --workspace=@optio/ui run build

# ------------------------------------------------------------------------------------------
# Stage 2: Production runner
# ------------------------------------------------------------------------------------------
FROM nginx:alpine AS runner

# Runtime-configurable upstream for the pipeline daemon (substituted into nginx.conf at start).
ENV PIPELINE_UPSTREAM=http://optio-pipeline:3000 \
    NGINX_RESOLVER=127.0.0.11

# Static bundle
COPY --from=builder /app/apps/ui/dist /usr/share/nginx/html

# Server block template — the image entrypoint renders it to /etc/nginx/conf.d/default.conf
# via envsubst, replacing the stock default server.
RUN rm -f /etc/nginx/conf.d/default.conf
COPY apps/ui/nginx.conf /etc/nginx/templates/default.conf.template

EXPOSE 4000

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO /dev/null http://127.0.0.1:4000/ || exit 1

CMD ["nginx", "-g", "daemon off;"]
