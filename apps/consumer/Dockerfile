# syntax=docker/dockerfile:1
#
# OPTIO Independent Event Stream Consumer — production image.
#
# Build context MUST be the monorepo root:
#   docker build -f apps/consumer/Dockerfile -t optio-consumer .

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
COPY apps/consumer/package.json apps/consumer/
COPY apps/pipeline/package.json apps/pipeline/
COPY apps/ui/package.json apps/ui/

# 2. Install workspace dependencies
RUN npm ci --include-workspace-root --workspace=@optio/shared --workspace=@optio/consumer

# 3. Copy source files for shared and consumer
COPY packages/shared/ packages/shared/
COPY apps/consumer/ apps/consumer/

# 4. Build @optio/shared and @optio/consumer
RUN npm --workspace=@optio/shared run build \
 && npm --workspace=@optio/consumer run build

# ------------------------------------------------------------------------------------------
# Stage 2: Runner
# ------------------------------------------------------------------------------------------
FROM node:20-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production \
    CONSUMER_PORT=3001

# Copy root manifests and node_modules from builder
COPY --from=builder /app/package.json /app/package-lock.json ./
COPY --from=builder /app/node_modules ./node_modules

# Copy compiled shared package and consumer application
COPY --from=builder /app/packages/shared ./packages/shared
COPY --from=builder /app/apps/consumer ./apps/consumer

EXPOSE 3001

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=5 \
  CMD wget -qO /dev/null http://localhost:3001/health || exit 1

CMD ["node", "apps/consumer/dist/index.js"]
