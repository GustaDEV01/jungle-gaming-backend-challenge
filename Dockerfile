# syntax=docker/dockerfile:1
FROM oven/bun:1.4.5-slim AS deps
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

FROM deps AS build
COPY tsconfig.json tsconfig.build.json bunfig.toml ./
COPY src ./src
COPY scripts ./scripts
COPY test ./test
RUN bun run typecheck && bun run build

# Image used by `docker compose --profile test run tests`: full sources + dev deps.
FROM build AS test
ENV NODE_ENV=test
CMD ["bun", "test", "--timeout", "240000", "test/unit", "test/integration", "test/concurrency", "test/distributed", "test/adversarial"]

FROM oven/bun:1.4.5-slim AS prod-deps
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

FROM oven/bun:1.4.5-slim AS runtime
WORKDIR /app
ENV NODE_ENV=production
COPY --from=prod-deps /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./
USER bun
EXPOSE 3000
HEALTHCHECK --interval=5s --timeout=3s --start-period=20s --retries=10 \
  CMD bun -e "fetch('http://127.0.0.1:' + (process.env.PORT ?? 3000) + '/health/ready').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"
CMD ["bun", "dist/main.js"]
