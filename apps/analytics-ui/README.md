# Analytics UI

Next.js dashboard presenting ClimateSense pipeline metrics (success rates, error breakdowns) and knowledge graph statistics.

## Prerequisites

- Node.js 20+
- npm

## Install

```bash
cd apps/analytics-ui
npm install
```

## Scripts

- `npm run dev` – start the development server on `http://localhost:3000`
- `npm run build` – create a production build
- `npm run start` – serve the production build
- `npm run lint` – run ESLint (also executed via pre-commit)

## Environment

Set `NEXT_PUBLIC_ANALYTICS_API_URL` to the analytics API base URL. When using Docker Compose, this is handled automatically; for local dev point it to your FastAPI instance, e.g.

```bash
export NEXT_PUBLIC_ANALYTICS_API_URL=http://localhost:8000
```
