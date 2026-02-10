# Project AGENTS.md
# Repo-specific guidance for Codex.

## Repo layout
- `api/`: Express API + SQLite (Twilio, Telegram integrations). Config lives in `api/config.js`.
- `bot/`: Telegram bot (grammy). Config lives in `bot/config.js`.

## Conventions
- Prefer config access via `api/config.js` / `bot/config.js`; avoid new direct `process.env` reads in feature code.
- Preserve existing response shapes and error formats in nearby handlers.
- Keep diffs small and localized; avoid drive-by formatting.

## Testing
- API: `npm test --prefix api`
- Bot: `npm test --prefix bot`
If tests can’t run, explain why and what you would run.

## Env + docs
- New env vars require updates to `api/.env.example` or `bot/.env.example` and any user-facing docs.
- Do not add new dependencies without approval (runtime or dev).

## Database
- Schema is defined in `api/db/db.js`.
- If you add or change tables/columns/indexes, update related cleanup logic and call it out explicitly in the summary.

## No-go areas
- Don’t edit `api/node_modules` or `bot/node_modules`.
- Don’t change CI/workflows unless explicitly requested.
