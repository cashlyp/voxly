# Profile Pack Contract

This directory uses a strict two-file layout per profile.

For each profile id (for example `dating`), create:

1. `api/functions/profiles/<profile-id>/<profile-id>.md`
2. `api/functions/profiles/<profile-id>/profile.md`

## Purpose Of Each File

- `<profile-id>.md`:
  - Primary runtime pack
  - Owns routing, policy, safety boundaries, and operational behavior
  - Should include required frontmatter for required profiles

- `profile.md`:
  - Companion style layer
  - Owns tone, phrasing, delivery style, and voice texture
  - Must not override safety/policy/routing authority from the primary pack

## Handshake Rule (Required)

Companion files must explicitly reference the primary pack filename.

Example for dating:
- companion file includes `dating.md`

This is validated by `profileRegistry` so the pair remains linked by contract.

## Recommended Structure

Use this pattern in companion files:

1. H1 heading
2. `## Purpose` (or `## Compatibility`) section
3. Explicit statement that this is a companion layer
4. Explicit reference to `<profile-id>.md`
5. Clear statement that primary pack wins on conflicts

## Companion Template

```md
# <Profile Name> Companion Style Layer

## Purpose
This file is the companion style layer for `<profile-id>.md`.
It standardizes tone and structure, while routing and policy remain in `<profile-id>.md`.

## Compatibility
Works with:
- `<profile-id>.md`

If there is any conflict, `<profile-id>.md` wins.

## Voice Style
- concise and natural spoken phrasing
- one clear move per turn
- safe, respectful, non-coercive language
```

## Validation

Run profile checks after any profile update:

```bash
npm --prefix api run validate:profiles
```

## Scaffold Command

Create a new profile folder with both required files:

```bash
npm --prefix api run scaffold:profile -- travel_agent
```

Force overwrite existing scaffold files:

```bash
npm --prefix api run scaffold:profile -- travel_agent --force
```
