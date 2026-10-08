# Security Policy

## Scope

This project is an experimental WordPress database drop-in and is explicitly
**not intended for production use**. The security posture below describes what
the engine does defend against, and what is out of scope.

## Current Protections

- **Storage is web-denied by default.** Every storage directory gets an
  `.htaccess` deny-all rule; `_meta.json`, `_schema.json`, `_global.seq`,
  `.tmp` and `.lock` files are additionally matched by name. Browsing the
  storage requires explicitly setting `HTMLDB_BROWSE` to boolean `true`.
- **Passwords are encrypted at rest.** The `user_pass` column is stored as
  `enc:v1:` AES-256-GCM ciphertext (12-byte IV, 16-byte tag). The key comes
  from `HTMLDB_SECRET_KEY` or an auto-generated web-denied `.secret` file
  with `0600` permissions.
- **All output is escaped.** Cell values are HTML-escaped when rendered into
  chunk and WAL files; column names are validated against the table schema.
- **Atomic writes.** Temp-file + `rename()` and `flock()` prevent torn state
  from being read mid-write.

## Known Limitations (by design)

- Flat-file storage has no per-row access control: anyone who can read the
  files on disk can read the data. Filesystem permissions are the boundary.
- No TLS at rest, no key rotation, no hardware-backed key storage.
- The engine inherits WordPress's own attack surface; a compromised WP admin
  can read and write everything the engine stores.

## Reporting a Vulnerability

Please report security issues privately via a
[GitHub security advisory](https://github.com/Mudrava/esoteric-database-in-real-html-tables/security/advisories/new)
instead of a public issue. The maintainer aims to respond within 7 days.
Reports about the experimental nature of the project itself ("do not use this
in production") will be closed as won't-fix, since the README already says so.
