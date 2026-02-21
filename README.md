# Esoteric Database in Real HTML Tables

A WordPress [database drop-in](https://developer.wordpress.org/reference/classes/wpdb/) that replaces MySQL with a flat-file storage engine — where every table is a folder of **real HTML files**.

> ⚠️ This is an esoteric/experimental project. Do **not** use in production.

---

## What it does

Instead of storing data in MySQL, `db.php` intercepts all WordPress database calls and saves rows as HTML `<table>` elements inside `.html` files on disk. You can open any of these files in a browser and browse your database as a styled, navigable web page — complete with a retro green-on-black terminal aesthetic.

```
wp-content/
└── db.php          ← drop this file here to activate

html_db/
├── _index.html     ← database browser (all tables)
├── _style.css      ← retro terminal CSS
├── _global.seq     ← global TX counter
└── wp_posts/
    ├── _index.html     ← table of contents (all chunks)
    ├── _meta.json      ← metadata (pk column, row count, …)
    ├── chunk_0001.html ← rows 1–500 as an HTML table
    ├── chunk_0002.html ← rows 501–1000
    ├── wal.html        ← append-only write-ahead log
    └── .seq            ← auto-increment counter
```

---

## Architecture (v3 — Sharded Storage)

| Concept | Detail |
|---|---|
| **Storage unit** | Each table is a directory; data is split into chunk files (≤ 500 rows each by default) |
| **Write path** | All mutations (INSERT, UPDATE, DELETE) are appended to `wal.html` — O(1), crash-safe |
| **Read path** | SELECTs merge chunk data with WAL entries; WAL entry wins by highest TX id |
| **Shard routing** | PK-equality queries touch one chunk; full-scan queries read all chunks |
| **Compaction** | Background vacuum merges WAL entries into chunks (triggered after 200 WAL entries by default) |
| **Ordering** | Global monotonic TX counter for MVCC ordering across concurrent requests |
| **Crash safety** | All writes use temp-file + `rename()` (POSIX atomic) |
| **Locking** | `flock(LOCK_EX)` on appends, `LOCK_SH` on reads, dedicated `.compact.lock` during vacuum |
| **Security** | `.htaccess` deny-all + `index.html` placeholder in every storage directory |
| **UI** | Retro terminal CSS (green-on-black, monospace, glow), linked prev/next navigation between chunks |

---

## Installation

1. Copy `db.php` into your WordPress `wp-content/` directory:

   ```
   wp-content/db.php
   ```

   WordPress automatically picks up any file named `db.php` in that location as a database drop-in.

2. Make sure the web server has write permission to the `wp-content/` directory (so `html_db/` can be created).

3. That's it. WordPress will now read and write through the HTML database engine instead of MySQL.

---

## Configuration

| Parameter | Default | Description |
|---|---|---|
| Base path | `wp-content/html_db` | Root directory for all HTML storage files (hardcoded) |
| Chunk size | `500` | Maximum rows per chunk file |
| Compact threshold | `200` | WAL entries before background compaction is triggered |

The base path is derived automatically from `WP_CONTENT_DIR` and cannot currently be changed without editing `db.php` directly.

---

## Browsing the database

Every chunk file is a valid HTML page you can open in any browser. Navigate to `html_db/_index.html` for the full database browser:

- **Database index** — lists all tables with row counts and chunk counts
- **Table index** — lists all chunks for a single table
- **Chunk pages** — show actual row data as an HTML `<table>` with prev/next navigation
- **WAL page** — shows the raw append-only mutation journal

All pages share a retro terminal stylesheet (`_style.css`): green text on a black background with monospace font and CRT glow effects.

---

## How queries are mapped

| SQL operation | HTML storage action |
|---|---|
| `INSERT` | Append `<tr>` to `wal.html` with op=`insert` |
| `UPDATE` | Append `<tr>` to `wal.html` with op=`update` (changed columns only) |
| `DELETE` | Append `<tr>` to `wal.html` with op=`delete` (tombstone) |
| `SELECT` | Parse matching chunk files + replay WAL on top |
| `CREATE TABLE` | Create table directory + `_meta.json` |
| Auto-increment | Atomic read-increment-write on per-table `.seq` file |

---

## Limitations

- **No SQL parser** — only a subset of WordPress's `wpdb` query patterns are handled.
- **No joins** — cross-table queries are not supported.
- **No transactions** — each operation is independently atomic; there is no multi-statement rollback.
- **Performance** — full-table scans read all chunk files from disk; this is much slower than MySQL for large tables.
- **Not for production** — this is an esoteric experiment, not a production-ready database.

---

## Requirements

- PHP 8.1+ (uses `readonly` classes and named arguments)
- WordPress 6.x
- A POSIX-compatible filesystem (for `rename()` atomicity and `flock()`)

---

## License

This project is provided as-is for educational and experimental purposes.
