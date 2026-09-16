# syntax=docker/dockerfile:1
#
# OPTIO Replication Pipeline Daemon — production image.
#
# Build context MUST be the monorepo root:
#   docker build -f apps/pipeline/Dockerfile -t optio-pipeline .

# ------------------------------------------------------------------------------------------
# Stage 1: Builder
# ------------------------------------------------------------------------------------------
FROM node:20-alpine AS builder

WORKDIR /app

ENV NODE_ENV=development \
    NPM_CONFIG_UPDATE_NOTIFIER=false \
    NPM_CONFIG_FUND=false \
    NPM_CONFIG_AUDIT=false

# 1. Copy workspace manifests for layer caching
COPY package.json package-lock.json tsconfig.json ./
COPY packages/shared/package.json packages/shared/
COPY apps/pipeline/package.json apps/pipeline/
COPY apps/consumer/package.json apps/consumer/
COPY apps/ui/package.json apps/ui/

# 2. Install workspace dependencies
RUN npm ci --include-workspace-root --workspace=@optio/shared --workspace=@optio/pipeline

# 3. Copy source files for shared and pipeline
COPY packages/shared/ packages/shared/
COPY apps/pipeline/ apps/pipeline/

# 4. Build @optio/shared and @optio/pipeline
RUN npm --workspace=@optio/shared run build \
 && npm --workspace=@optio/pipeline run build

# ------------------------------------------------------------------------------------------
# Stage 2: Runner
# ------------------------------------------------------------------------------------------
FROM node:20-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production \
    PIPELINE_PORT=3000

# Copy root manifests and node_modules from builder
COPY --from=builder /app/package.json /app/package-lock.json ./
COPY --from=builder /app/node_modules ./node_modules

# Copy compiled shared package and pipeline application
COPY --from=builder /app/packages/shared ./packages/shared
COPY --from=builder /app/apps/pipeline ./apps/pipeline

EXPOSE 3000

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=5 \
  CMD wget -qO /dev/null http://localhost:3000/health || exit 1

CMD ["node", "apps/pipeline/dist/index.js"]
