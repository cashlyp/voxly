# Voicednut Mini App (Vercel)

This project is the Telegram Mini App admin console for Voicednut, scaffolded via:

```bash
npx @tma.js/create-mini-app@latest
```

Stack:
- React + TypeScript
- `@tma.js/sdk-react`
- `@telegram-apps/telegram-ui`
- Vite

## Environment

Create `.env` from `.env.example`:

```bash
cp .env.example .env
```

Set:

```bash
VITE_API_BASE_URL=https://your-api-domain.com
```

Or reuse existing projects that already define:

```bash
VITE_API_BASE=https://your-api-domain.com
```

`VITE_API_BASE_URL` (or `VITE_API_BASE`) must point to the API host that exposes:
- `POST /miniapp/session`
- `GET /miniapp/bootstrap`
- `GET /miniapp/jobs/poll`
- `POST /miniapp/action`

## Local Run

```bash
npm install
npm run dev
```

## Production Build

```bash
npm run build
```

Build artifacts are generated in `dist/`.

## Vercel Deploy

This folder includes `vercel.json` with Vite settings.

Recommended Vercel project settings:
- Framework Preset: `Vite`
- Root Directory: `miniapp`
- Build Command: `npm run build`
- Output Directory: `dist`

Required Vercel env vars:
- `VITE_API_BASE_URL`
or
- `VITE_API_BASE`

## Telegram Bot Linking

Set bot/API env to the deployed Vercel URL:

- `MINI_APP_URL=https://your-miniapp.vercel.app`

The bot uses this URL to render the admin launch button (`/admin` and admin menus).

## Security Notes

- The frontend sends Telegram `initDataRaw` to API using `Authorization: tma <initDataRaw>`.
- The API validates Telegram init-data signature server-side and issues short-lived mini-app session tokens.
- Admin actions are capability-gated on the API (`/miniapp/action`).
