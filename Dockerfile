# ─────────────────────────────────────────────────────────────────────────────
# ACM Competition MCP — multi-stage Dockerfile
# ─────────────────────────────────────────────────────────────────────────────
# Build:  docker build -t dutch-competition-mcp .
# Run:    docker run --rm -p 3000:3000 dutch-competition-mcp
#
# The image bakes the database at /app/data/acm-comp.db.
# Override with ACM_DB_PATH for a custom location.
# ─────────────────────────────────────────────────────────────────────────────

# --- Stage 1: Build TypeScript + native bindings ---
FROM node:20-slim AS builder

WORKDIR /app

# Install build toolchain for better-sqlite3 native compile
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 make g++ \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json* ./
# Run postinstall so better-sqlite3 fetches/builds its native .node binding.
RUN npm ci
COPY tsconfig.json ./
COPY src/ src/
RUN npm run build

# --- Stage 2: Production ---
FROM node:20-slim AS production

WORKDIR /app
ENV NODE_ENV=production
ENV ACM_DB_PATH=/app/data/acm-comp.db

# Carry forward node_modules (with native bindings) from builder — do NOT
# re-run `npm ci` here, that strips the postinstall-built better-sqlite3 binary.
COPY --from=builder /app/node_modules ./node_modules
COPY package.json package-lock.json* ./
COPY --from=builder /app/dist/ dist/

# Bake the database into the image. The CI workflow downloads
# data/database.db from the GitHub Release asset `database.db.gz`
# (gunzipped to data/database.db) before this build runs.
COPY data/database.db data/acm-comp.db

# Non-root user for security
RUN addgroup --system --gid 1001 mcp && \
    adduser --system --uid 1001 --ingroup mcp mcp && \
    chown -R mcp:mcp /app
USER mcp

# Health check: verify HTTP server responds
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health',r=>{process.exit(r.statusCode===200?0:1)}).on('error',()=>process.exit(1))"

CMD ["node", "dist/src/http-server.js"]
