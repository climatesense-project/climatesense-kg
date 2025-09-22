FROM node:20-alpine AS base

# Step 1. Rebuild the source code only when needed
FROM base AS builder

WORKDIR /app

COPY apps/analytics-ui/package.json apps/analytics-ui/package-lock.json* ./
RUN npm ci

COPY apps/analytics-ui/tsconfig.json ./
COPY apps/analytics-ui/next.config.ts ./
COPY apps/analytics-ui/postcss.config.mjs ./
COPY apps/analytics-ui/eslint.config.mjs ./
COPY apps/analytics-ui/next-env.d.ts ./
COPY apps/analytics-ui/app ./app
COPY apps/analytics-ui/components ./components
COPY apps/analytics-ui/lib ./lib

# https://github.com/vercel/next.js/discussions/14030
ARG NEXT_PUBLIC_ANALYTICS_API_URL
ENV NEXT_PUBLIC_ANALYTICS_API_URL=${NEXT_PUBLIC_ANALYTICS_API_URL}

ENV NEXT_TELEMETRY_DISABLED=1

RUN npm run build

# Step 2. Production image, copy all the files and run next
FROM base AS runner

WORKDIR /app

RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs
USER nextjs

# COPY --from=builder /app/public ./public

# https://nextjs.org/docs/advanced-features/output-file-tracing
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

ARG NEXT_PUBLIC_ANALYTICS_API_URL
ENV NEXT_PUBLIC_ANALYTICS_API_URL=${NEXT_PUBLIC_ANALYTICS_API_URL}

ENV NEXT_TELEMETRY_DISABLED=1

CMD ["node", "server.js"]
