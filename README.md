# Esoteric Database in Real HTML Tables

A WordPress [database drop-in](https://developer.wordpress.org/reference/classes/wpdb/) that replaces MySQL with a flat-file storage engine - where every table is a folder of **real HTML files**.

> ⚠️ This is an esoteric/experimental project. Do **not** use in production.

---

## What it does

Instead of storing data in MySQL, `db.php` intercepts all WordPress database calls and saves rows as HTML `<table>` elements inside `.html` files on disk. You can open any of these files in a browser and browse your database as a styled, navigable web page - complete with a retro green-on-black terminal aesthetic.

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

## Architecture (v3 - Sharded Storage)

| Concept | Detail |
|---|---|
| **Storage unit** | Each table is a directory; data is split into chunk files (≤ 500 rows each by default) |
| **Write path** | All mutations (INSERT, UPDATE, DELETE) are appended to `wal.html` - O(1), crash-safe |
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

All configuration is done via constants in `wp-config.php` (all optional):

| Constant | Default | Description |
|---|---|---|
| `HTMLDB_BASE_PATH` | `wp-content/html_db` | Root directory for all HTML storage files |
| `HTMLDB_CHUNK_SIZE` | `500` | Maximum rows per chunk file for **new** tables |
| `HTMLDB_COMPACT_THRESHOLD` | `200` | WAL entries before background compaction is triggered |
| `HTMLDB_BROWSE` | `false` | When `true`, the storage directory is web-browsable (see Security) |
| `HTMLDB_SECRET_KEY` | auto-generated | Passphrase for the AES-256-GCM key that encrypts secret columns |

`HTMLDB_CHUNK_SIZE` is persisted per-table in `_meta.json` at creation time, so
changing the constant later never misroutes reads against existing chunks.
Without `HTMLDB_SECRET_KEY`, a random key is generated on first run and stored
in a web-denied `.secret` dot-file next to the data.

---

## Browsing the database

The storage directory is **web-denied by default**. To browse it, opt in:

```php
define( 'HTMLDB_BROWSE', true ); // wp-config.php
```

Every chunk file is then a valid HTML page you can open in any browser. Navigate to `html_db/_index.html` for the full database browser:

- **Database index** - lists all tables with row counts and chunk counts
- **Table index** - lists all chunks for a single table
- **Chunk pages** - show actual row data as an HTML `<table>` with prev/next navigation
- **WAL page** - shows the raw append-only mutation journal

All pages share a retro terminal stylesheet (`_style.css`): green text on a black background with monospace font and CRT glow effects.

---

## How queries are mapped

| SQL operation | HTML storage action |
|---|---|
| `INSERT` | Append `<tr>` to `wal.html` with op=`insert` |
| `UPDATE` | Append `<tr>` to `wal.html` with op=`update` (changed columns only) |
| `DELETE` | Append `<tr>` to `wal.html` with op=`delete` (tombstone) |
| `SELECT` | Parse matching chunk files + replay WAL on top |
| `CREATE TABLE` | Create table directory + `_meta.json` + `_schema.json` |
| `ALTER TABLE` | Update `_schema.json` |
| Auto-increment | Atomic read-increment-write on per-table `.seq` file |

Supported SQL surface (the subset WordPress core and popular plugins emit):

- `SELECT` with `WHERE` (`=`, `!=`, `<`, `>`, `LIKE`, `IN`, `NOT IN`, `BETWEEN`, `IS NULL`), `OR` groups, `ORDER BY` (multi-column), `LIMIT`/`OFFSET`, `DISTINCT`, `GROUP BY`, `HAVING`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` (with aliases), `DATE_FORMAT`-style date functions (`YEAR()`, `MONTH()`, `DAY()`, `HOUR()`, `MINUTE()`, `SECOND()`, `WEEK()`, `QUARTER()`) in both `SELECT` and `WHERE`
- `CASE WHEN` in `UPDATE`/`SELECT`
- `JOIN` (INNER/LEFT, comma joins, multi-table) with join-condition routing
- `INSERT` … `ON DUPLICATE KEY UPDATE` (upsert), multi-row `INSERT`
- `INSERT IGNORE` with real unique-key enforcement: `UNIQUE KEY` definitions
  from `CREATE TABLE`/`ALTER TABLE` are parsed into `_schema.json`, and
  colliding rows are skipped with 0 affected rows (core option locks -
  `WP_Upgrader::create_lock()` and friends - depend on this)
- `REPLACE INTO`, `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `TRUNCATE`, `SHOW TABLES`, `DESCRIBE`
- `SET`/`START TRANSACTION`/`COMMIT`/`ROLLBACK` are accepted as no-ops (single-statement atomicity only)

## Security

- **Storage is web-denied by default.** A deny-all `.htaccess` (plus a placeholder `index.html`) is written into every storage directory. The browsable viewer is opt-in via `HTMLDB_BROWSE`.
- **Secret columns are encrypted at rest.** `user_pass` is stored as `enc:v1:base64(iv|tag|ciphertext)` (AES-256-GCM) inside the HTML files and transparently decrypted on read, so even in browse mode no password hash is ever exposed. The key comes from `HTMLDB_SECRET_KEY` or an auto-generated `.secret` file (chmod 0600, web-denied).
- All values are HTML-escaped on write and decoded on read; SQL string literals are parsed with `prepare()`-safe placeholder handling.
- Apache-only protection: on nginx, deny the storage directory via a `location` block.

---

## Limitations

- **No general SQL parser** - only the query patterns WordPress core and common plugins emit are handled; exotic SQL falls through or errors.
- **No subqueries or UNION** - `has_cap()` reports them unsupported so WP falls back to PHP-side paths where it can.
- **No transactions** - each statement is independently atomic; there is no multi-statement rollback.
- **No secondary indexes** - non-PK `WHERE` is a full scan of the table's chunks (PK equality is O(1) via shard routing).
- **Performance** - measured on the Docker bench (PHP 8.1, Docker Desktop): ~2,200 inserts/s, PK point-read ~0.4 ms at any table size, full scan of 20k rows ~25 ms. Much slower than MySQL at scale.
- **Apache-only hardening** - `.htaccess` protection requires Apache; nginx needs a manual `location` deny.
- **Not for production** - this is an esoteric experiment, not a production-ready database.

---

## Requirements

- PHP 8.1+ (`readonly` properties, `match`, `str_starts_with`/`str_ends_with`)
- WordPress 6.x or 7.x (verified live: the bench updates core 6.9 -> 7.1.3
  through the engine, including the `dbDelta` schema migration)
- A POSIX-compatible filesystem (for `rename()` atomicity and `flock()`)

---

## Tests

A Docker bench with a real WordPress (plus ACF and Elementor) runs the full
suite against the engine:

```bash
./tests/run.sh        # all suites (113 checks)
./tests/run.sh t02    # one suite
```

| Suite | Covers |
|---|---|
| `t01_basic` | CRUD, prepare, transients, cron, comments, arithmetic UPDATE, `INSERT IGNORE` locks, dotted option names |
| `t02_content` | posts, taxonomies, menus, templates, UTF-8/emoji round-trip |
| `t03_plugins` | ACF fields, Elementor data, REST API |
| `t04_ddl` | CREATE/ALTER/DROP, DESCRIBE, SHOW TABLES, schema persistence, UNIQUE keys |

`bench_perf.php` / `bench_scale.php` measure insert throughput, PK-read
latency and scan cost. The bench mounts `db.php` live, so engine edits are
picked up without rebuilding the image.

---

## License

MIT - see [LICENSE](LICENSE).
