FROM node:20-alpine AS builder

WORKDIR /app

COPY apps/analytics-ui/package.json ./
COPY apps/analytics-ui/tsconfig.json ./
COPY apps/analytics-ui/next.config.ts ./
COPY apps/analytics-ui/postcss.config.mjs ./
COPY apps/analytics-ui/.eslintrc.json ./
COPY apps/analytics-ui/next-env.d.ts ./
COPY apps/analytics-ui/.gitignore ./
COPY apps/analytics-ui/app ./app
COPY apps/analytics-ui/components ./components
COPY apps/analytics-ui/lib ./lib

RUN npm install && npm run build

FROM node:20-alpine AS runner

ENV NODE_ENV=production
ENV PORT=3000

WORKDIR /app

COPY --from=builder /app/.next ./.next
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json

EXPOSE 3000

CMD ["npm", "run", "start"]
