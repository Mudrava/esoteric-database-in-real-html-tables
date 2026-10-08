<?php
/**
 * Plugin Name: HTML Database Drop-in
 * Description: Esoteric HTML-based database engine for WordPress - replaces MySQL with flat HTML files.
 * Version: 3.0.0
 *
 * Architecture (v3 - Sharded Storage):
 *   - Each table is a folder: html_db/{table}/ with chunk files and an append-only WAL.
 *   - Chunks hold ≤ CHUNK_SIZE rows (default 500) as full HTML pages with retro-terminal CSS.
 *   - All mutations (INSERT, UPDATE, DELETE) append to wal.html - O(1), crash-safe.
 *   - SELECTs route via ShardRouter: PK-based queries touch one chunk + WAL.
 *   - Background compaction merges WAL entries into chunks (triggered by threshold).
 *   - Global monotonic TX counter for MVCC ordering across requests.
 *   - Crash-safe writes: temp-file + rename() pattern (POSIX atomicity).
 *   - flock(LOCK_EX) on appends; LOCK_SH on reads; dedicated .compact.lock for vacuum.
 *   - Security: .htaccess deny-all + index.html in every directory.
 *   - Retro-terminal CSS theme: green-on-black, monospace, glow effects.
 *   - Linked navigation between chunks (prev/next) and table index pages.
 *
 * Drop this file into wp-content/db.php to activate.
 */

declare(strict_types=1);

namespace {
    if (!defined('ABSPATH')) {
        exit;
    }
}

// ---------------------------------------------------------------------------
// Namespace: HtmlDatabase\Core - Configuration, Shard Router, Storage Manager
// ---------------------------------------------------------------------------
namespace HtmlDatabase\Core {

    use SplFileObject;
    use RuntimeException;

    /**
     * Immutable runtime configuration.
     */
    final class Configuration
    {
        /** Maximum rows per chunk file. */
        public readonly int $chunkSize;

        /** Number of WAL entries before inline compaction triggers. */
        public readonly int $compactThreshold;

        /** 32-byte AES key for secret columns (user_pass). */
        public readonly string $secretKey;

        /** When false the storage directory is web-denied (default). */
        public readonly bool $browse;

        public function __construct(
            public string $basePath,
            int           $chunkSize        = 500,
            int           $compactThreshold = 200
        ) {
            $this->chunkSize        = $chunkSize;
            $this->compactThreshold = $compactThreshold;
            $this->browse           = \defined('HTMLDB_BROWSE') && HTMLDB_BROWSE === true;
            $this->secretKey        = $this->resolveSecretKey();
        }

        /**
         * Resolve the AES key: an explicit HTMLDB_SECRET_KEY constant wins,
         * otherwise a per-install key is persisted in a dot-file (web-denied)
         * and reused across requests.
         */
        private function resolveSecretKey(): string
        {
            if (\defined('HTMLDB_SECRET_KEY')) {
                return hash('sha256', (string) HTMLDB_SECRET_KEY, true);
            }
            $keyFile = $this->basePath . '/.secret';
            if (is_readable($keyFile)) {
                $raw = (string) @file_get_contents($keyFile);
                $bin = @base64_decode(trim($raw), true);
                if (is_string($bin) && strlen($bin) === 32) {
                    return $bin;
                }
            }
            $key = random_bytes(32);
            if (is_dir($this->basePath) || @mkdir($this->basePath, 0755, true)) {
                @file_put_contents($keyFile, base64_encode($key));
                @chmod($keyFile, 0600);
            }
            return $key;
        }
    }

    /**
     * Routes queries to the correct chunk file(s) within a table folder.
     *
     * Each table is stored as:
     *   html_db/{table}/
     *     _meta.json        - metadata (pk column, chunk_size, row count, etc.)
     *     _index.html       - human-browsable table-of-contents
     *     chunk_0001.html   - rows with PK 1..chunk_size
     *     chunk_0002.html   - rows with PK chunk_size+1..2*chunk_size
     *     wal.html          - append-only mutation journal
     *     .seq              - auto-increment counter
     */
    final class ShardRouter
    {
        public function __construct(private Configuration $config) {}

        /**
         * Compute which chunk file a given PK value belongs to.
         *
         * $chunkSize overrides the global default when the table's
         * _meta.json was created with a different size.
         *
         * @return string Chunk filename, e.g. "chunk_0003.html"
         */
        public function chunkForPk(int $pkValue, ?int $chunkSize = null): string
        {
            $size = $chunkSize ?? $this->config->chunkSize;
            $num = (int) ceil($pkValue / $size);
            if ($num < 1) $num = 1;
            return sprintf('chunk_%04d.html', $num);
        }

        /**
         * The PK range a given chunk covers.
         *
         * @return array{from: int, to: int}
         */
        public function chunkRange(string $chunkFile, ?int $chunkSize = null): array
        {
            $size = $chunkSize ?? $this->config->chunkSize;
            if (preg_match('/chunk_(\d+)\.html$/', $chunkFile, $m)) {
                $num = (int) $m[1];
                $from = ($num - 1) * $size + 1;
                $to   = $num * $size;
                return ['from' => $from, 'to' => $to];
            }
            return ['from' => 1, 'to' => $size];
        }

        /**
         * Determine which chunk files to scan for given WHERE conditions.
         *
         * If conditions contain a PK equality (e.g. ID = 42), returns only
         * the one relevant chunk. Otherwise returns ALL chunks.
         *
         * @return string[] List of chunk filenames to scan.
         */
        public function resolveChunks(string $tableDir, ?string $pkCol, array $conditions, ?int $chunkSize = null): array
        {
            // If we have a scalar PK equality condition, narrow to one chunk
            if ($pkCol !== null && isset($conditions[$pkCol]) && !is_array($conditions[$pkCol])) {
                $pkVal = (int) $conditions[$pkCol];
                if ($pkVal > 0) {
                    $chunk = $this->chunkForPk($pkVal, $chunkSize);
                    $path  = $tableDir . '/' . $chunk;
                    // If the specific chunk doesn't exist yet (data only in WAL), return empty
                    return file_exists($path) ? [$chunk] : [];
                }
            }

            // Full scan: return all existing chunk files sorted
            return $this->listChunks($tableDir);
        }

        /**
         * List all chunk files in a table directory, sorted.
         *
         * @return string[]
         */
        public function listChunks(string $tableDir): array
        {
            $pattern = $tableDir . '/chunk_*.html';
            $files   = glob($pattern);
            if (!is_array($files) || empty($files)) {
                return [];
            }
            sort($files);
            return array_map('basename', $files);
        }

        /**
         * Path to a table's directory.
         */
        public function tableDir(string $table): string
        {
            $safe = preg_replace('/[^a-zA-Z0-9_]/', '', $table);
            return rtrim($this->config->basePath, '/') . '/' . $safe;
        }

        /**
         * Path to the WAL file for a table.
         */
        public function walPath(string $table): string
        {
            return $this->tableDir($table) . '/wal.html';
        }

        /**
         * Path to the .seq file for a table.
         */
        public function seqPath(string $table): string
        {
            return $this->tableDir($table) . '/.seq';
        }

        /**
         * Path to the _meta.json for a table.
         */
        public function metaPath(string $table): string
        {
            return $this->tableDir($table) . '/_meta.json';
        }

        /**
         * Path to the persisted _schema.json for a table (created from
         * CREATE TABLE / ALTER TABLE statements).
         */
        public function schemaPath(string $table): string
        {
            return $this->tableDir($table) . '/_schema.json';
        }

        /**
         * Ensure the table directory exists with security files.
         *
         * Returns true when the browsable _index.html is missing (freshly
         * created directory, or an older install predating index pages), so
         * callers can regenerate it. Without this, WAL/chunk pages link to a
         * 404 until the table happens to be compacted.
         */
        public function ensureTableDir(string $table): bool
        {
            $dir = $this->tableDir($table);
            if (!is_dir($dir)) {
                mkdir($dir, 0755, true);
                // Create security index.html in the table directory
                $this->writeSecurityIndex($dir, $table);
                return true;
            }
            return !file_exists($dir . '/_index.html');
        }

        /**
         * Write a security index.html in a directory.
         */
        private function writeSecurityIndex(string $dir, string $label): void
        {
            $html = '<!DOCTYPE html><html><head><meta charset="UTF-8">'
                  . '<link rel="stylesheet" href="../_style.css">'
                  . '<title>' . htmlspecialchars($label) . '</title></head>'
                  . '<body><h1>⛔ Access Denied</h1>'
                  . '<p>This is a database storage directory.</p>'
                  . '</body></html>';
            @file_put_contents($dir . '/index.html', $html);
        }
    }

    /**
     * Generates styled HTML pages for chunk files and index pages.
     *
     * Each chunk is a full HTML document with retro-terminal CSS,
     * nav links (prev/next), and the table data rows.
     */
    final class HtmlPageBuilder
    {
        public function __construct(private Configuration $config) {}

        /**
         * Build a complete HTML chunk page.
         *
         * @param string   $table     Table name
         * @param int      $chunkNum  Chunk number (1-based)
         * @param int      $totalChunks Total number of chunks
         * @param string   $tbodyHtml Raw <tr> rows HTML
         * @param string[] $columnNames Column names for <thead>
         * @param int      $rowFrom   First PK in this chunk
         * @param int      $rowTo     Last PK in this chunk
         * @param int      $totalRows Total rows across all chunks
         */
        public function buildChunkPage(
            string $table,
            int    $chunkNum,
            int    $totalChunks,
            string $tbodyHtml,
            array  $columnNames,
            int    $rowFrom,
            int    $rowTo,
            int    $totalRows
        ): string {
            $prevLink = $chunkNum > 1
                ? sprintf('<a href="chunk_%04d.html">◄ Prev</a>', $chunkNum - 1)
                : '<span class="disabled">◄ Prev</span>';
            $nextLink = $chunkNum < $totalChunks
                ? sprintf('<a href="chunk_%04d.html">Next ►</a>', $chunkNum + 1)
                : '<span class="disabled">Next ►</span>';

            $thead = '<tr>';
            foreach ($columnNames as $col) {
                $thead .= '<th>' . htmlspecialchars($col) . '</th>';
            }
            $thead .= '</tr>';

            $nav = "{$prevLink} <span class=\"current\">Page {$chunkNum} of {$totalChunks}</span> {$nextLink}";

            return <<<HTML
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{$table} :: chunk {$chunkNum} of {$totalChunks}</title>
  <link rel="stylesheet" href="../_style.css">
</head>
<body>
  <header>
    <h1>📀 {$table}</h1>
    <nav>
      <a href="_index.html">⌂ Index</a> | {$nav}
    </nav>
    <p class="info">Rows {$rowFrom}–{$rowTo} of {$totalRows} | Chunk size: {$this->config->chunkSize}</p>
  </header>
  <table id="{$table}">
    <thead>{$thead}</thead>
    <tbody>
{$tbodyHtml}
    </tbody>
  </table>
  <footer>
    <nav>{$nav}</nav>
    <p>HtmlDB v3.0 • Generated {$this->timestamp()}</p>
  </footer>
</body>
</html>
HTML;
        }

        /**
         * Build the _index.html table-of-contents page.
         */
        public function buildIndexPage(
            string $table,
            array  $chunks,
            int    $totalRows,
            int    $walPending
        ): string {
            $chunkLinks = '';
            foreach ($chunks as $i => $chunkFile) {
                $num   = $i + 1;
                $range = (($num - 1) * $this->config->chunkSize + 1) . '–' . ($num * $this->config->chunkSize);
                $chunkLinks .= "      <li><a href=\"{$chunkFile}\">Chunk {$num}</a> - rows {$range}</li>\n";
            }

            return <<<HTML
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{$table} :: Index</title>
  <link rel="stylesheet" href="../_style.css">
</head>
<body>
  <header>
    <h1>📀 {$table}</h1>
    <nav><a href="../_index.html">⌂ Database</a> | <a href="wal.html">📝 WAL</a></nav>
  </header>
  <div class="stats">
    <p>Total rows: <strong>{$totalRows}</strong> | WAL pending: <strong>{$walPending}</strong> | Chunk size: <strong>{$this->config->chunkSize}</strong></p>
  </div>
  <h2>Chunks</h2>
  <ul class="chunk-list">
{$chunkLinks}
  </ul>
  <footer>
    <p>HtmlDB v3.0 • Generated {$this->timestamp()}</p>
  </footer>
</body>
</html>
HTML;
        }

        /**
         * Build the root database _index.html page listing all tables.
         */
        public function buildDatabaseIndex(array $tables): string
        {
            $tableLinks = '';
            foreach ($tables as $info) {
                $tableLinks .= sprintf(
                    "      <li><a href=\"%s/_index.html\">%s</a> - %d rows, %d chunks</li>\n",
                    htmlspecialchars($info['name']),
                    htmlspecialchars($info['name']),
                    $info['rows'],
                    $info['chunks']
                );
            }

            return <<<HTML
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>HtmlDB - Database Index</title>
  <link rel="stylesheet" href="_style.css">
</head>
<body>
  <header>
    <h1>🗄️ HtmlDB - Database Browser</h1>
  </header>
  <h2>Tables</h2>
  <ul class="chunk-list">
{$tableLinks}
  </ul>
  <footer>
    <p>HtmlDB v3.0 • Generated {$this->timestamp()}</p>
  </footer>
</body>
</html>
HTML;
        }

        /**
         * Build the WAL HTML skeleton (header portion up to and including <tbody>).
         */
        public function buildWalHeader(string $table): string
        {
            return <<<HTML
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{$table} :: WAL (Write-Ahead Log)</title>
  <link rel="stylesheet" href="../_style.css">
</head>
<body>
  <header>
    <h1>📝 {$table} - WAL</h1>
    <nav>
      <a href="_index.html">⌂ Index</a>
    </nav>
    <p class="info">Append-only mutation journal • Newest entries at the bottom</p>
  </header>
  <table id="{$table}_wal">
    <thead><tr><th>OP</th><th>TX</th><th>PK</th><th>Data columns →</th></tr></thead>
    <tbody>
HTML;
        }

        /**
         * Build the WAL HTML footer (closing tags).
         */
        public function buildWalFooter(): string
        {
            $ts = $this->timestamp();
            return <<<HTML
    </tbody>
  </table>
  <footer>
    <p>HtmlDB v3.0 • WAL snapshot {$ts}</p>
  </footer>
</body>
</html>
HTML;
        }

        private function timestamp(): string
        {
            return gmdate('Y-m-d\TH:i:s\Z');
        }
    }

    /**
     * Manages sharded storage: per-table folders with chunk files and WAL.
     *
     * All mutations (INSERT, UPDATE, DELETE) are append-only to wal.html.
     * SELECTs merge chunk data with WAL entries (WAL wins by highest TX).
     * Compaction periodically folds WAL into chunks.
     */
    final class ShardedStorageManager
    {
        private ShardRouter   $router;
        private HtmlPageBuilder $pageBuilder;

        /** In-memory cache of table metadata (per-request). */
        private array $metaCache = [];

        /** In-memory cache of table schemas (per-request). */
        private array $schemaCache = [];

        public function __construct(private Configuration $config)
        {
            $this->router      = new ShardRouter($config);
            $this->pageBuilder = new HtmlPageBuilder($config);
            $this->ensureStorageExists();
        }

        public function getRouter(): ShardRouter
        {
            return $this->router;
        }

        /**
         * Ensure a table directory exists and its browsable _index.html is
         * present. Fresh or legacy directories get an initial index page
         * immediately, so WAL/chunk navigation never 404s before the first
         * compaction.
         */
        private function ensureTable(string $table): void
        {
            if (!$this->router->ensureTableDir($table)) {
                return;
            }
            $meta      = $this->readMeta($table);
            $tableDir  = $this->router->tableDir($table);
            $chunks    = $this->router->listChunks($tableDir);
            $indexHtml = $this->pageBuilder->buildIndexPage(
                $table,
                $chunks,
                (int) ($meta['total_rows'] ?? 0),
                (int) ($meta['wal_entries'] ?? 0)
            );
            $indexPath = $tableDir . '/_index.html';
            $tmp       = $indexPath . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
            file_put_contents($tmp, $indexHtml, LOCK_EX);
            rename($tmp, $indexPath);
            // New table must appear in the root database index immediately,
            // not only after its first compaction.
            $this->rebuildDatabaseIndex();
        }

        // -- INSERT -----------------------------------------------------------

        /**
         * Append a single row to the WAL.
         */
        public function insert(string $table, array $payload, int $txId): int
        {
            $this->ensureTable($table);
            $html = $this->buildWalEntry($payload, 'insert', $txId, $this->extractPkFromPayload($table, $payload));
            $this->appendToWal($table, $html);
            $this->incrementMeta($table, 'total_rows', 1);
            $this->incrementMeta($table, 'wal_entries', 1);
            $this->maybeCompact($table);
            return 1;
        }

        /**
         * Append multiple rows in one atomic write.
         */
        public function insertBatch(string $table, array $columns, array $rows, int $txId): int
        {
            $this->ensureTable($table);
            $colCount = count($columns);
            $html = '';
            foreach ($rows as $ri => $values) {
                if (count($values) !== $colCount) {
                    error_log("[HtmlDB] BATCH COL MISMATCH table={$table} row={$ri}");
                    continue;
                }
                $payload = array_combine($columns, $values);
                $pk = $this->extractPkFromPayload($table, $payload);
                $html .= $this->buildWalEntry($payload, 'insert', $txId, $pk);
            }
            if ($html !== '') {
                $this->appendToWal($table, $html);
                $this->incrementMeta($table, 'wal_entries', count($rows));
                $this->incrementMeta($table, 'total_rows', count($rows));
                $this->maybeCompact($table);
            }
            return count($rows);
        }

        // -- UPDATE (append-only) ---------------------------------------------

        /**
         * Append an update entry to the WAL.
         * Only the changed columns are stored; merged at read time.
         */
        public function updateRows(string $table, array $setValues, array $conditions, int $txId): int
        {
            $this->ensureTable($table);

            // We need to find matching rows to know which PKs to update.
            // Read from chunks + WAL, apply conditions, get PKs.
            $matchingRows = $this->findMatchingRows($table, $conditions);

            if (empty($matchingRows)) {
                return 0;
            }

            $html = '';
            foreach ($matchingRows as $row) {
                // Resolve arithmetic deltas and CASE expressions against the
                // current row value
                $resolved = [];
                foreach ($setValues as $col => $val) {
                    if (is_array($val) && array_key_exists('__delta__', $val)) {
                        $sum = ((float) ($row[$col] ?? 0)) + $val['__delta__'];
                        $resolved[$col] = (fmod($sum, 1.0) === 0.0)
                            ? (string) (int) $sum
                            : (string) $sum;
                    } elseif (is_array($val) && array_key_exists('__case__', $val)) {
                        $resolved[$col] = $this->resolveCase($val['__case__'], $row);
                    } else {
                        $resolved[$col] = $val;
                    }
                }
                $pk = $this->rowKey($table, $row);
                // Merge: existing row data + set values (set values override)
                $merged = array_merge($row, $resolved);
                $html .= $this->buildWalEntry($merged, 'update', $txId, $pk);
            }

            if ($html !== '') {
                $this->appendToWal($table, $html);
                $this->incrementMeta($table, 'wal_entries', count($matchingRows));
                $this->maybeCompact($table);
            }

            return count($matchingRows);
        }

        // -- DELETE (append-only) ---------------------------------------------

        /**
         * Append tombstone entries to the WAL.
         */
        public function deleteRows(string $table, array $conditions, int $txId): int
        {
            $this->ensureTable($table);
            $matchingRows = $this->findMatchingRows($table, $conditions);

            if (empty($matchingRows)) {
                return 0;
            }

            $html = '';
            foreach ($matchingRows as $row) {
                $pk = $this->rowKey($table, $row);
                $html .= $this->buildWalEntry($row, 'delete', $txId, $pk);
            }

            if ($html !== '') {
                $this->appendToWal($table, $html);
                $this->incrementMeta($table, 'wal_entries', count($matchingRows));
                $this->incrementMeta($table, 'total_rows', -count($matchingRows));
                $this->maybeCompact($table);
            }

            return count($matchingRows);
        }

        // -- READ (merge chunks + WAL) ----------------------------------------

        /**
         * Read all rows from a table, merging chunks with WAL.
         * Optionally narrows to specific chunks via conditions.
         *
         * @param string      $table
         * @param array       $conditions Equality conditions for filtering
         * @param array       $inConditions IN conditions for filtering
         * @param string|null $pkCol      PK column name (for WAL merge)
         * @return array[] Each element is ['data' => [...cols...], 'tx' => int]
         */
        public function readRows(string $table, array $conditions = [], array $inConditions = [], ?string $pkCol = null): array
        {
            $tableDir = $this->router->tableDir($table);
            if (!is_dir($tableDir)) {
                return [];
            }

            if ($pkCol === null) {
                $pkCol = $this->resolvePkColumn($table);
            }

            // 1. Determine which chunks to read
            $chunkFiles = $this->router->resolveChunks($tableDir, $pkCol, $conditions, $this->tableChunkSize($table));

            // 2. Read rows from chunks
            $rows = []; // keyed by PK if available, else sequential
            foreach ($chunkFiles as $chunkFile) {
                $chunkPath = $tableDir . '/' . $chunkFile;
                $chunkRows = $this->parseHtmlRows($chunkPath);
                foreach ($chunkRows as $row) {
                    $key = ($pkCol !== null && isset($row['data'][$pkCol]))
                        ? $row['data'][$pkCol]
                        : count($rows);
                    $rows[$key] = $row;
                }
            }

            // 3. Read and merge WAL entries (WAL wins by higher TX)
            $walPath = $this->router->walPath($table);
            if (file_exists($walPath)) {
                $walEntries = $this->parseWalEntries($walPath);
                foreach ($walEntries as $entry) {
                    $pk  = (string) $entry['pk'];
                    $op  = $entry['op'];
                    $key = ($pk !== '' && $pk !== '0') ? $pk : (string) count($rows);

                    if ($op === 'delete') {
                        unset($rows[$key]);
                    } elseif ($op === 'update') {
                        if (isset($rows[$key])) {
                            // Merge: existing data + WAL update
                            $rows[$key]['data'] = array_merge($rows[$key]['data'], $entry['data']);
                            $rows[$key]['tx']   = $entry['tx'];
                            // No-PK tables: row identity is its content hash,
                            // re-key so later entries find the mutated row.
                            if ($pkCol === null) {
                                $newKey = $this->rowKey($table, $rows[$key]['data']);
                                if ($newKey !== $key) {
                                    $rows[$newKey] = $rows[$key];
                                    unset($rows[$key]);
                                }
                            }
                        } else {
                            // Update for a row not in chunks - treat as full row
                            $rows[$pkCol === null ? $this->rowKey($table, $entry['data']) : $key]
                                = ['data' => $entry['data'], 'tx' => $entry['tx']];
                        }
                    } elseif ($op === 'insert') {
                        $rows[$key] = ['data' => $entry['data'], 'tx' => $entry['tx']];
                    }
                }
            }

            return array_values($rows);
        }

        /**
         * Stable identity for a row. PK column when the table has one;
         * otherwise a content hash so WAL tombstones/updates on no-PK
         * tables (term_relationships, custom tables) hit the right row.
         */
        public function rowKey(string $table, array $data): string
        {
            $pkCol = $this->resolvePkColumn($table);
            if ($pkCol !== null && isset($data[$pkCol]) && (string) $data[$pkCol] !== '') {
                return (string) $data[$pkCol];
            }
            return md5(json_encode($data, JSON_UNESCAPED_UNICODE));
        }

        /**
         * Find rows matching conditions (used internally by update/delete).
         * Returns array of row data arrays.
         */
        public function findMatchingRows(string $table, array $conditions): array
        {
            $allRows = $this->readRows($table, $conditions);
            $matched = [];

            foreach ($allRows as $row) {
                $data = $row['data'];
                $match = true;
                foreach ($conditions as $col => $val) {
                    if (is_array($val) && isset($val['__in__'])) {
                        if (!in_array((string)($data[$col] ?? ''), array_map('strval', $val['__in__']), true)) {
                            $match = false;
                            break;
                        }
                        continue;
                    }
                    if (is_array($val) && isset($val['__cmp__'])) {
                        [$cv, $op] = $val['__cmp__'];
                        $dv = $data[$col] ?? '';
                        if (is_numeric($dv) && is_numeric($cv)) { $a = (float)$dv; $b = (float)$cv; }
                        else { $a = (string)$dv; $b = (string)$cv; }
                        $ok = match ($op) {
                            '>'  => $a > $b,
                            '>=' => $a >= $b,
                            '<'  => $a < $b,
                            '<=' => $a <= $b,
                            default => false,
                        };
                        if (!$ok) { $match = false; break; }
                        continue;
                    }
                    if (is_array($val) && isset($val['__between__'])) {
                        $dv = $data[$col] ?? '';
                        $bt = $val['__between__'];
                        if (is_numeric($dv) && is_numeric($bt['low']) && is_numeric($bt['high'])) {
                            $ok = (float) $dv >= (float) $bt['low'] && (float) $dv <= (float) $bt['high'];
                        } else {
                            $ok = (string) $dv >= (string) $bt['low'] && (string) $dv <= (string) $bt['high'];
                        }
                        if ($bt['not']) $ok = !$ok;
                        if (!$ok) { $match = false; break; }
                        continue;
                    }
                    if (!array_key_exists($col, $data) || (string) $data[$col] !== (string) $val) {
                        $match = false;
                        break;
                    }
                }
                if ($match) {
                    $matched[] = $data;
                }
            }

            return $matched;
        }

        /**
         * Find a single row by PK equality (chunk-routed read + WAL merge).
         */
        public function findRowByPk(string $table, string $pk): ?array
        {
            $pkCol = $this->resolvePkColumn($table);
            if ($pkCol === null) {
                return null;
            }
            $rows = $this->readRows($table, [$pkCol => $pk]);
            foreach ($rows as $row) {
                if ((string) ($row['data'][$pkCol] ?? '') === $pk) {
                    return $row['data'];
                }
            }
            return null;
        }

        /**
         * Append an update WAL entry targeting one PK directly, without
         * scanning the whole table (used by upsert paths).
         */
        public function updateRowsByPk(string $table, string $pk, array $setValues, int $txId): int
        {
            $existing = $this->findRowByPk($table, $pk);
            if ($existing === null) {
                return 0;
            }
            $resolved = [];
            foreach ($setValues as $col => $val) {
                if (is_array($val) && array_key_exists('__delta__', $val)) {
                    $sum = ((float) ($existing[$col] ?? 0)) + $val['__delta__'];
                    $resolved[$col] = (fmod($sum, 1.0) === 0.0)
                        ? (string) (int) $sum
                        : (string) $sum;
                } elseif (is_array($val) && array_key_exists('__case__', $val)) {
                    $resolved[$col] = $this->resolveCase($val['__case__'], $existing);
                } else {
                    $resolved[$col] = $val;
                }
            }
            $merged = array_merge($existing, $resolved);
            $this->appendToWal($table, $this->buildWalEntry($merged, 'update', $txId, $pk));
            $this->incrementMeta($table, 'wal_entries', 1);
            $this->maybeCompact($table);
            return 1;
        }

        /**
         * Evaluate a parsed CASE WHEN expression against a row.
         *
         * @param array{cases:array<int,array{col:string,val:string,then:string}>,else:?string} $case
         */
        private function resolveCase(array $case, array $row): string
        {
            foreach ($case['cases'] as $c) {
                if ((string) ($row[$c['col']] ?? '') === (string) $c['val']) {
                    return (string) $c['then'];
                }
            }
            return $case['else'] ?? '';
        }

        /**
         * Append a tombstone for one PK directly (REPLACE path).
         */
        public function deleteRowsByPk(string $table, string $pk, int $txId): int
        {
            $existing = $this->findRowByPk($table, $pk);
            if ($existing === null) {
                return 0;
            }
            $this->appendToWal($table, $this->buildWalEntry($existing, 'delete', $txId, $pk));
            $this->incrementMeta($table, 'wal_entries', 1);
            $this->incrementMeta($table, 'total_rows', -1);
            $this->maybeCompact($table);
            return 1;
        }

        /**
         * Remove a table directory entirely.
         */
        public function dropTable(string $table): void
        {
            $dir = $this->router->tableDir($table);
            if (!is_dir($dir)) {
                return;
            }
            foreach (scandir($dir) ?: [] as $f) {
                if ($f === '.' || $f === '..') continue;
                @unlink($dir . '/' . $f);
            }
            @rmdir($dir);
            unset($this->metaCache[$table]);
            unset($this->schemaCache[$table]);
        }

        /**
         * Empty a table but keep its directory, sequence and meta skeleton.
         */
        public function truncateTable(string $table): void
        {
            $dir = $this->router->tableDir($table);
            if (!is_dir($dir)) {
                return;
            }
            foreach (scandir($dir) ?: [] as $f) {
                if (preg_match('/^(chunk_.*|wal.*|_index\.html|_meta\.json)$/', $f)) {
                    @unlink($dir . '/' . $f);
                }
            }
            $this->metaCache[$table] = [
                'table'       => $table,
                'pk'          => $this->resolvePkColumn($table),
                'chunk_size'  => $this->config->chunkSize,
                'chunks'      => 0,
                'total_rows'  => 0,
                'wal_entries' => 0,
                'updated_at'  => gmdate('Y-m-d\TH:i:s\Z'),
            ];
            $metaPath = $this->router->metaPath($table);
            $tmpMeta  = $metaPath . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
            @file_put_contents($tmpMeta, json_encode($this->metaCache[$table], JSON_PRETTY_PRINT));
            @rename($tmpMeta, $metaPath);
        }

        /**
         * Column names of a table. Persisted schema (from CREATE TABLE) is
         * authoritative; otherwise sampled from stored rows.
         *
         * @return string[]
         */
        public function listColumns(string $table): array
        {
            $schema = $this->readSchema($table);
            if ($schema !== null && !empty($schema['columns'])) {
                return array_keys($schema['columns']);
            }
            $rows = $this->readRows($table);
            foreach ($rows as $row) {
                if (!empty($row['data'])) {
                    return array_keys($row['data']);
                }
            }
            return [];
        }

        /**
         * Read the persisted _schema.json for a table (cached per-request).
         * Returns null when no schema has been recorded (core tables created
         * before schema persistence, or implicit tables).
         *
         * @return array{pk:?string,auto:?string,columns:array<string,array>,defaults:array<string,string>,unique:string[][]}|null
         */
        public function readSchema(string $table): ?array
        {
            if (array_key_exists($table, $this->schemaCache)) {
                return $this->schemaCache[$table];
            }
            $path = $this->router->schemaPath($table);
            if (file_exists($path)) {
                $decoded = json_decode((string) @file_get_contents($path), true);
                if (is_array($decoded) && isset($decoded['columns'])) {
                    $decoded += ['pk' => null, 'auto' => null, 'defaults' => [], 'unique' => []];
                    $this->schemaCache[$table] = $decoded;
                    return $decoded;
                }
            }
            $this->schemaCache[$table] = null;
            return null;
        }

        /**
         * Persist a table schema to _schema.json (crash-safe temp + rename).
         */
        public function writeSchema(string $table, array $schema): void
        {
            $this->ensureTable($table);
            $schema += ['pk' => null, 'auto' => null, 'columns' => [], 'defaults' => [], 'unique' => []];
            $path = $this->router->schemaPath($table);
            $tmp  = $path . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
            @file_put_contents($tmp, json_encode($schema, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE), LOCK_EX);
            @rename($tmp, $path);
            $this->schemaCache[$table] = $schema;
        }

        // -- Auto-increment ---------------------------------------------------

        /**
         * Persistent per-table auto-increment counter.
         * Protected by exclusive flock for the full read-increment-write cycle.
         */
        public function nextAutoIncrement(string $table): int
        {
            $this->ensureTable($table);
            $seqPath = $this->router->seqPath($table);

            $file = new SplFileObject($seqPath, 'c+');
            if (!$file->flock(LOCK_EX)) {
                throw new RuntimeException("Unable to lock sequence file: {$seqPath}");
            }
            $file->rewind();
            $current = (int) $file->fgets();
            $next = $current + 1;

            // Crash-safe: write to a process-unique temp, then rename
            $tmpPath = $seqPath . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
            file_put_contents($tmpPath, (string) $next);
            rename($tmpPath, $seqPath);

            $file->flock(LOCK_UN);
            return $next;
        }

        /**
         * Raise the per-table sequence to at least the given explicit PK so
         * later auto-increments never collide with explicitly-inserted IDs.
         */
        public function advanceSequence(string $table, int $pk): void
        {
            $this->ensureTable($table);
            $seqPath = $this->router->seqPath($table);

            $file = new SplFileObject($seqPath, 'c+');
            if (!$file->flock(LOCK_EX)) {
                return;
            }
            $file->rewind();
            $current = (int) $file->fgets();
            if ($pk > $current) {
                $tmpPath = $seqPath . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
                file_put_contents($tmpPath, (string) $pk);
                rename($tmpPath, $seqPath);
            }
            $file->flock(LOCK_UN);
        }

        // -- Global TX counter ------------------------------------------------

        /**
         * Get next global transaction ID (monotonic across all requests).
         */
        public function nextTxId(): int
        {
            $seqPath = rtrim($this->config->basePath, '/') . '/_global.seq';

            $file = new SplFileObject($seqPath, 'c+');
            if (!$file->flock(LOCK_EX)) {
                throw new RuntimeException("Unable to lock global TX seq");
            }
            $file->rewind();
            $current = (int) $file->fgets();
            $next = $current + 1;

            $tmpPath = $seqPath . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
            file_put_contents($tmpPath, (string) $next);
            rename($tmpPath, $seqPath);

            $file->flock(LOCK_UN);
            return $next;
        }

        // -- Compaction -------------------------------------------------------

        /**
         * Check if compaction is needed and run it if so.
         * Called opportunistically during reads.
         */
        public function maybeCompact(string $table): void
        {
            $meta = $this->readMeta($table);
            $walEntries = $meta['wal_entries'] ?? 0;
            if ($walEntries < $this->config->compactThreshold) {
                return;
            }
            $this->compact($table);
        }

        /**
         * Fold WAL entries into chunk files.
         *
         * Strategy (full rebuild - simple and correct):
         * 1. Acquire exclusive compact lock
         * 2. Rename wal.html → wal.processing.html (new writes go to fresh wal.html)
         * 3. Read ALL chunks, apply every WAL entry in TX order
         * 4. Re-bucket surviving rows into chunks by PK range (or
         *    sequentially for no-PK tables), rewrite all chunks
         * 5. Delete stale chunk files + wal.processing.html
         * 6. Update _meta.json + _index.html
         */
        public function compact(string $table): void
        {
            $tableDir = $this->router->tableDir($table);
            $lockPath = $tableDir . '/.compact.lock';

            $lockFile = new SplFileObject($lockPath, 'c+');
            if (!$lockFile->flock(LOCK_EX | LOCK_NB)) {
                // Another process is compacting - skip
                return;
            }

            try {
                $walPath = $this->router->walPath($table);
                $processingPath = $tableDir . '/wal.processing.html';

                // Step 1: Rotate WAL
                if (!file_exists($walPath) || filesize($walPath) === 0) {
                    return;
                }

                // Atomic rename so new appends go to a fresh wal.html
                rename($walPath, $processingPath);
                // Touch a new empty wal.html so appends don't fail
                touch($walPath);

                // Step 2: Read processing WAL
                $walEntries = $this->parseWalEntries($processingPath);
                if (empty($walEntries)) {
                    @unlink($processingPath);
                    return;
                }

                $pkCol = $this->resolvePkColumn($table);

                // Step 3: Load every existing chunk into one map
                $rowsByKey = [];
                foreach ($this->router->listChunks($tableDir) as $chunkFile) {
                    foreach ($this->parseHtmlRows($tableDir . '/' . $chunkFile) as $row) {
                        $key = $this->rowKey($table, $row['data']);
                        $rowsByKey[$key] = $row;
                    }
                }

                // Apply WAL entries in file (TX) order
                foreach ($walEntries as $entry) {
                    $pk = (string) $entry['pk'];
                    if ($entry['op'] === 'delete') {
                        if ($pk !== '' && $pk !== '0') {
                            unset($rowsByKey[$pk]);
                        } else {
                            // Hash-keyed tombstone: match by content
                            foreach ($rowsByKey as $k => $r) {
                                if ($this->rowKey($table, $r['data']) === $k
                                    && $this->rowMatches($table, $r['data'], $entry['data'])) {
                                    unset($rowsByKey[$k]);
                                    break;
                                }
                            }
                        }
                    } elseif ($entry['op'] === 'update') {
                        if ($pk !== '' && $pk !== '0' && isset($rowsByKey[$pk])) {
                            $rowsByKey[$pk]['data'] = array_merge($rowsByKey[$pk]['data'], $entry['data']);
                            $rowsByKey[$pk]['tx']   = $entry['tx'];
                            if ($pkCol === null) {
                                $newKey = $this->rowKey($table, $rowsByKey[$pk]['data']);
                                if ($newKey !== $pk) {
                                    $rowsByKey[$newKey] = $rowsByKey[$pk];
                                    unset($rowsByKey[$pk]);
                                }
                            }
                        } else {
                            $rowsByKey[$pk !== '' && $pk !== '0' ? $pk : $this->rowKey($table, $entry['data'])]
                                = ['data' => $entry['data'], 'tx' => $entry['tx']];
                        }
                    } else { // insert
                        $rowsByKey[$pk !== '' && $pk !== '0' ? $pk : $this->rowKey($table, $entry['data'])]
                            = ['data' => $entry['data'], 'tx' => $entry['tx']];
                    }
                }

                // Step 4: Re-bucket rows into chunks
                $buckets = []; // chunkNum => rows[]
                $seq = 0;
                $tableChunk = $this->tableChunkSize($table);
                foreach ($rowsByKey as $row) {
                    if ($pkCol !== null && isset($row['data'][$pkCol]) && (int) $row['data'][$pkCol] > 0) {
                        $num = (int) ceil(((int) $row['data'][$pkCol]) / $tableChunk);
                        if ($num < 1) $num = 1;
                    } else {
                        $num = (int) floor($seq / $tableChunk) + 1;
                    }
                    $buckets[$num][] = $row;
                    $seq++;
                }
                ksort($buckets);
                $totalChunks = max(1, count($buckets));

                // Rewrite all chunks; remove stale ones
                $keepFiles = [];
                foreach ($buckets as $num => $rows) {
                    $chunkFile = sprintf('chunk_%04d.html', $num);
                    $keepFiles[] = $chunkFile;
                    $this->writeChunkFile($table, $chunkFile, $rows, $totalChunks);
                }
                foreach ($this->router->listChunks($tableDir) as $old) {
                    if (!in_array($old, $keepFiles, true)) {
                        @unlink($tableDir . '/' . $old);
                    }
                }
                if (empty($buckets)) {
                    // Table emptied - write a single empty chunk so pages exist
                    $this->writeChunkFile($table, 'chunk_0001.html', [], 1);
                }

                // Step 5: Cleanup
                @unlink($processingPath);

                // Step 6: Update metadata
                $this->rebuildMetaAndIndex($table);

            } finally {
                $lockFile->flock(LOCK_UN);
            }
        }

        /**
         * Loose content match used for hash-keyed tombstones: every column
         * present in the candidate must be equal.
         */
        private function rowMatches(string $table, array $row, array $candidate): bool
        {
            foreach ($candidate as $col => $val) {
                if ((string) ($row[$col] ?? '') !== (string) $val) {
                    return false;
                }
            }
            return true;
        }

        // -- SHOW TABLES support ----------------------------------------------

        /**
         * List all table directories.
         *
         * @return string[] Table names
         */
        public function listTables(): array
        {
            $dir = $this->config->basePath;
            if (!is_dir($dir)) {
                return [];
            }
            $tables = [];
            foreach (scandir($dir) as $entry) {
                if ($entry[0] === '.' || $entry[0] === '_') continue;
                if (is_dir($dir . '/' . $entry)) {
                    $tables[] = $entry;
                }
            }
            sort($tables);
            return $tables;
        }

        // -- Private helpers --------------------------------------------------

        /**
         * Columns whose values are encrypted at rest. WordPress never filters
         * on these in SQL (passwords are verified in PHP via
         * wp_check_password), so ciphertext in the HTML is transparent to the
         * query engine while keeping the browsable files safe to expose.
         */
        private const SECRET_COLUMNS = ['user_pass'];

        private function isSecretColumn(string $col): bool
        {
            return in_array($col, self::SECRET_COLUMNS, true);
        }

        /**
         * Encrypt a secret column value with AES-256-GCM. Output is
         * "enc:v1:base64(iv|tag|ciphertext)". Non-secret, empty, or already
         * encrypted values pass through untouched.
         */
        private function encryptSecret(string $value): string
        {
            if ($value === '' || str_starts_with($value, 'enc:v1:')) {
                return $value;
            }
            $iv  = random_bytes(12);
            $tag = '';
            $ct  = openssl_encrypt($value, 'aes-256-gcm', $this->config->secretKey,
                OPENSSL_RAW_DATA, $iv, $tag, '', 16);
            if ($ct === false) {
                return $value;
            }
            return 'enc:v1:' . base64_encode($iv . $tag . $ct);
        }

        /**
         * Reverse encryptSecret. Values without the enc:v1: prefix (legacy
         * plaintext rows) are returned unchanged.
         */
        private function decryptSecret(string $value): string
        {
            if (!str_starts_with($value, 'enc:v1:')) {
                return $value;
            }
            $raw = base64_decode(substr($value, 7), true);
            if ($raw === false || strlen($raw) < 29) {
                return '';
            }
            $iv  = substr($raw, 0, 12);
            $tag = substr($raw, 12, 16);
            $ct  = substr($raw, 28);
            $pt  = openssl_decrypt($ct, 'aes-256-gcm', $this->config->secretKey,
                OPENSSL_RAW_DATA, $iv, $tag);
            return $pt === false ? '' : $pt;
        }

        /**
         * Render one payload column as an escaped <td>, encrypting secret
         * columns first. Shared by the WAL and chunk row builders.
         */
        private function renderCell(string $column, mixed $value): string
        {
            $safeCol = htmlspecialchars($column, ENT_QUOTES | ENT_HTML5, 'UTF-8');
            $rawVal  = (string) $value;
            if ($this->isSecretColumn($column)) {
                $rawVal = $this->encryptSecret($rawVal);
            }
            $safeVal = htmlspecialchars($rawVal, ENT_QUOTES | ENT_HTML5, 'UTF-8');
            return sprintf('<td data-column="%s">%s</td>', $safeCol, $safeVal);
        }

        /**
         * Build an MVCC WAL entry with operation type and PK.
         */
        private function buildWalEntry(array $payload, string $op, int $txId, string $pk): string
        {
            $html = sprintf(
                '<tr data-op="%s" data-tx="%d" data-pk="%s">',
                htmlspecialchars($op),
                $txId,
                htmlspecialchars($pk)
            );
            foreach ($payload as $column => $value) {
                $html .= $this->renderCell((string) $column, $value);
            }
            $html .= "</tr>\n";
            return $html;
        }

        /**
         * Build a chunk <tr> row (no MVCC attributes - compacted data).
         */
        private function buildChunkRow(array $payload, int $tx): string
        {
            $html = sprintf('<tr data-tx="%d">', $tx);
            foreach ($payload as $column => $value) {
                $html .= $this->renderCell((string) $column, $value);
            }
            $html .= "</tr>\n";
            return $html;
        }

        /**
         * Parse HTML rows from a chunk file (full HTML page with <table>).
         * Returns array of ['data' => [...], 'tx' => int]
         */
        private function parseHtmlRows(string $path): array
        {
            if (!file_exists($path)) {
                return [];
            }

            $content = @file_get_contents($path);
            if ($content === false || trim($content) === '') {
                return [];
            }

            // Extract <tbody>...</tbody> content
            $tbodyStart = strpos($content, '<tbody>');
            $tbodyEnd   = strpos($content, '</tbody>');
            if ($tbodyStart !== false && $tbodyEnd !== false) {
                $content = substr($content, $tbodyStart + 7, $tbodyEnd - $tbodyStart - 7);
            }

            return $this->parseRawTrRows($content);
        }

        /**
         * Parse WAL entries (raw <tr> rows with data-op, data-tx, data-pk).
         * Returns array of ['op' => string, 'tx' => int, 'pk' => string, 'data' => [...]]
         */
        private function parseWalEntries(string $path): array
        {
            if (!file_exists($path)) {
                return [];
            }

            // Use shared lock for reading WAL
            $fh = fopen($path, 'r');
            if (!$fh) return [];
            flock($fh, LOCK_SH);
            $content = stream_get_contents($fh);
            flock($fh, LOCK_UN);
            fclose($fh);

            if ($content === false || trim($content) === '') {
                return [];
            }

            $entries = [];
            $segments = explode('</tr>', $content);

            foreach ($segments as $segment) {
                $trPos = strpos($segment, '<tr ');
                if ($trPos === false) continue;

                $trBlock = substr($segment, $trPos);

                // Extract attributes
                $op = 'insert';
                if (preg_match('/data-op="([^"]*)"/', $trBlock, $m)) {
                    $op = $m[1];
                }

                $tx = 0;
                if (preg_match('/data-tx="(\d+)"/', $trBlock, $m)) {
                    $tx = (int) $m[1];
                }
                $pk = '0';
                if (preg_match('/data-pk="([^"]*)"/', $trBlock, $m)) {
                    $pk = $m[1];
                }

                // Skip rows without TX
                if ($tx === 0) continue;

                // Extract cells
                $data = $this->extractCells($trBlock);
                if (empty($data)) continue;

                // If pk is still '0', try to get it from data
                if ($pk === '0') {
                    $pkCol = null;
                    // Try to detect PK from known columns
                    if (isset($data['ID'])) $pk = $data['ID'];
                    elseif (isset($data['option_id'])) $pk = $data['option_id'];
                    elseif (isset($data['umeta_id'])) $pk = $data['umeta_id'];
                    elseif (isset($data['meta_id'])) $pk = $data['meta_id'];
                    elseif (isset($data['comment_ID'])) $pk = $data['comment_ID'];
                    elseif (isset($data['term_id'])) $pk = $data['term_id'];
                    elseif (isset($data['term_taxonomy_id'])) $pk = $data['term_taxonomy_id'];
                }

                $entries[] = ['op' => $op, 'tx' => $tx, 'pk' => $pk, 'data' => $data];
            }

            return $entries;
        }

        /**
         * Parse raw <tr> rows (from chunk <tbody> or raw file content).
         * Returns array of ['data' => [...], 'tx' => int]
         */
        private function parseRawTrRows(string $content): array
        {
            $rows = [];
            $segments = explode('</tr>', $content);

            foreach ($segments as $segment) {
                $trPos = strpos($segment, '<tr');
                if ($trPos === false) continue;

                $trBlock = substr($segment, $trPos);

                $tx = 0;
                if (preg_match('/data-tx="(\d+)"/', $trBlock, $m)) {
                    $tx = (int) $m[1];
                }
                if ($tx === 0) continue;

                $data = $this->extractCells($trBlock);
                if (!empty($data)) {
                    $rows[] = ['data' => $data, 'tx' => $tx];
                }
            }

            return $rows;
        }

        /**
         * Extract <td data-column="...">...</td> cells from a <tr> block.
         */
        private function extractCells(string $trBlock): array
        {
            $data = [];
            $searchPos = 0;
            while (($tdStart = strpos($trBlock, '<td data-column="', $searchPos)) !== false) {
                $colStart = $tdStart + strlen('<td data-column="');
                $colEnd   = strpos($trBlock, '">', $colStart);
                if ($colEnd === false) break;

                $col = substr($trBlock, $colStart, $colEnd - $colStart);

                $valStart = $colEnd + 2;
                $valEnd   = strpos($trBlock, '</td>', $valStart);
                if ($valEnd === false) break;

                $val = substr($trBlock, $valStart, $valEnd - $valStart);
                $col = htmlspecialchars_decode($col, ENT_QUOTES | ENT_HTML5);
                $val = htmlspecialchars_decode($val, ENT_QUOTES | ENT_HTML5);
                if ($this->isSecretColumn($col)) {
                    $val = $this->decryptSecret($val);
                }
                $data[$col] = $val;

                $searchPos = $valEnd + 5;
            }
            return $data;
        }

        /**
         * Extract PK value from a payload row (PK column or content hash).
         */
        private function extractPkFromPayload(string $table, array $payload): string
        {
            return $this->rowKey($table, $payload);
        }

        /**
         * Resolve the PK column for a table. Persisted schema (from
         * CREATE TABLE) wins; core tables fall back to the static map.
         */
        public function resolvePkColumn(string $table): ?string
        {
            $schema = $this->readSchema($table);
            if ($schema !== null) {
                return isset($schema['pk']) && $schema['pk'] !== '' ? (string) $schema['pk'] : null;
            }

            static $pkMap = [
                'users'              => 'ID',
                'usermeta'           => 'umeta_id',
                'posts'              => 'ID',
                'postmeta'           => 'meta_id',
                'comments'           => 'comment_ID',
                'commentmeta'        => 'meta_id',
                'terms'              => 'term_id',
                'termmeta'           => 'meta_id',
                'term_taxonomy'      => 'term_taxonomy_id',
                'options'            => 'option_id',
                'links'              => 'link_id',
            ];

            foreach ($pkMap as $suffix => $pkCol) {
                if ($table === $suffix || str_ends_with($table, '_' . $suffix)) {
                    return $pkCol;
                }
            }
            return null;
        }

        /**
         * Write a chunk file with styled HTML page (crash-safe: temp + rename).
         */
        private function writeChunkFile(string $table, string $chunkFile, array $rows, ?int $knownTotalChunks = null, ?int $knownTotalRows = null): void
        {
            $tableDir = $this->router->tableDir($table);
            $targetPath = $tableDir . '/' . $chunkFile;
            $tmpPath    = $targetPath . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));

            // Build tbody HTML
            $tbodyHtml = '';
            $columnNames = [];
            foreach ($rows as $row) {
                $tbodyHtml .= $this->buildChunkRow($row['data'], $row['tx']);
                if (empty($columnNames) && !empty($row['data'])) {
                    $columnNames = array_keys($row['data']);
                }
            }

            // Calculate chunk number and range
            preg_match('/chunk_(\d+)\.html$/', $chunkFile, $cm);
            $chunkNum  = (int) ($cm[1] ?? 1);
            $range     = $this->router->chunkRange($chunkFile);

            // Use provided total or detect from disk
            if ($knownTotalChunks !== null) {
                $totalChunks = $knownTotalChunks;
            } else {
                $allChunks = $this->router->listChunks($tableDir);
                if (!in_array($chunkFile, $allChunks, true)) {
                    $allChunks[] = $chunkFile;
                    sort($allChunks);
                }
                $totalChunks = count($allChunks);
            }

            // Exact total when provided by the caller (compaction knows),
            // otherwise fall back to a per-chunk estimate.
            $totalRows = $knownTotalRows ?? count($rows) * $totalChunks;

            $html = $this->pageBuilder->buildChunkPage(
                $table,
                $chunkNum,
                $totalChunks,
                $tbodyHtml,
                $columnNames,
                $range['from'],
                $range['to'],
                $totalRows
            );

            // Crash-safe write
            file_put_contents($tmpPath, $html, LOCK_EX);
            rename($tmpPath, $targetPath);
        }

        /**
         * Rebuild _meta.json and _index.html after compaction or migration.
         */
        private function rebuildMetaAndIndex(string $table): void
        {
            $tableDir = $this->router->tableDir($table);
            $chunks   = $this->router->listChunks($tableDir);
            $totalChunks = count($chunks);

            // Single parse pass: count rows per chunk and overall
            $totalRows = 0;
            $chunkRows = [];
            foreach ($chunks as $chunkFile) {
                $rows = $this->parseHtmlRows($tableDir . '/' . $chunkFile);
                $totalRows += count($rows);
                $chunkRows[$chunkFile] = $rows;
            }

            // Re-stamp chunk navigation with exact totals (chunk files were
            // just rewritten by compaction; this refreshes page counts).
            foreach ($chunks as $chunkFile) {
                $this->writeChunkFile($table, $chunkFile, $chunkRows[$chunkFile], $totalChunks, $totalRows);
            }

            // Count WAL entries
            $walPath = $this->router->walPath($table);
            $walEntries = 0;
            if (file_exists($walPath) && filesize($walPath) > 0) {
                $entries = $this->parseWalEntries($walPath);
                $walEntries = count($entries);
            }

            // Write _meta.json. Preserve the table's original chunk_size so
            // changing HTMLDB_CHUNK_SIZE later cannot misroute reads against
            // chunks already bucketed with the old size.
            $meta = [
                'table'       => $table,
                'pk'          => $this->resolvePkColumn($table),
                'chunk_size'  => $this->tableChunkSize($table),
                'chunks'      => count($chunks),
                'total_rows'  => $totalRows,
                'wal_entries' => $walEntries,
                'updated_at'  => gmdate('Y-m-d\TH:i:s\Z'),
            ];
            $metaPath = $this->router->metaPath($table);
            $tmpMeta  = $metaPath . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
            file_put_contents($tmpMeta, json_encode($meta, JSON_PRETTY_PRINT));
            rename($tmpMeta, $metaPath);

            $this->metaCache[$table] = $meta;

            // Write _index.html
            $indexHtml = $this->pageBuilder->buildIndexPage($table, $chunks, $totalRows, $walEntries);
            $indexPath = $tableDir . '/_index.html';
            $tmpIndex  = $indexPath . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
            file_put_contents($tmpIndex, $indexHtml);
            rename($tmpIndex, $indexPath);

            // Rebuild root database _index.html
            $this->rebuildDatabaseIndex();
        }

        /**
         * Rebuild the root html_db/_index.html listing all tables.
         */
        private function rebuildDatabaseIndex(): void
        {
            $allTables = $this->listTables();
            $tableInfos = [];
            foreach ($allTables as $tbl) {
                $meta = $this->readMeta($tbl);
                $tableInfos[] = [
                    'name'   => $tbl,
                    'rows'   => $meta['total_rows'] ?? 0,
                    'chunks' => $meta['chunks'] ?? 0,
                ];
            }
            $html = $this->pageBuilder->buildDatabaseIndex($tableInfos);
            $path = rtrim($this->config->basePath, '/') . '/_index.html';
            $tmp  = $path . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
            file_put_contents($tmp, $html, LOCK_EX);
            rename($tmp, $path);
        }

        /**
         * Read _meta.json for a table (cached per-request).
         */
        private function readMeta(string $table): array
        {
            if (isset($this->metaCache[$table])) {
                return $this->metaCache[$table];
            }
            $metaPath = $this->router->metaPath($table);
            if (file_exists($metaPath)) {
                $data = json_decode(file_get_contents($metaPath), true);
                if (is_array($data)) {
                    $this->metaCache[$table] = $data;
                    return $data;
                }
            }
            return [];
        }

        /**
         * The chunk size a table was created with. Persisted in _meta.json
         * so changing HTMLDB_CHUNK_SIZE later cannot misroute reads against
         * chunks written with the old size. Falls back to the global default
         * for tables created before the field existed.
         */
        private function tableChunkSize(string $table): int
        {
            $size = (int) ($this->readMeta($table)['chunk_size'] ?? 0);
            return $size > 0 ? $size : $this->config->chunkSize;
        }

        /**
         * Increment a numeric field in _meta.json.
         * Read-modify-write under an exclusive meta lock so concurrent
         * writers cannot lose updates.
         */
        private function incrementMeta(string $table, string $field, int $delta): void
        {
            $metaPath = $this->router->metaPath($table);
            $lockPath = $this->router->tableDir($table) . '/.meta.lock';

            $lf = @fopen($lockPath, 'c');
            if ($lf) {
                flock($lf, LOCK_EX);
            }

            // Re-read from disk (not the request cache) for the RMW cycle
            $meta = [];
            if (file_exists($metaPath)) {
                $decoded = json_decode((string) @file_get_contents($metaPath), true);
                if (is_array($decoded)) {
                    $meta = $decoded;
                }
            }
            $meta[$field]    = ($meta[$field] ?? 0) + $delta;
            $meta['updated_at'] = gmdate('Y-m-d\TH:i:s\Z');

            $tmpMeta = $metaPath . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
            @file_put_contents($tmpMeta, json_encode($meta, JSON_PRETTY_PRINT));
            @rename($tmpMeta, $metaPath);
            $this->metaCache[$table] = $meta;

            // Keep the browsable index page in sync with live counts while we
            // still hold the meta lock (chunk list only changes at compaction,
            // row/WAL counters change on every mutation).
            $chunks      = $this->router->listChunks($this->router->tableDir($table));
            $indexHtml   = $this->pageBuilder->buildIndexPage(
                $table,
                $chunks,
                (int) ($meta['total_rows'] ?? 0),
                (int) ($meta['wal_entries'] ?? 0)
            );
            $indexPath = $this->router->tableDir($table) . '/_index.html';
            $tmpIndex  = $indexPath . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
            @file_put_contents($tmpIndex, $indexHtml, LOCK_EX);
            @rename($tmpIndex, $indexPath);

            if ($lf) {
                flock($lf, LOCK_UN);
                fclose($lf);
            }
        }

        /**
         * Append rows to a WAL file, maintaining a valid HTML document.
         *
         * If the WAL doesn't exist or is empty → create full HTML page.
         * If it exists → insert <tr> rows before </tbody> marker.
         */
        private function appendToWal(string $table, string $trContent): void
        {
            $filePath = $this->router->walPath($table);
            $dir = dirname($filePath);
            if (!is_dir($dir)) {
                mkdir($dir, 0755, true);
            }

            $fh = fopen($filePath, 'c+');
            if (!$fh) {
                throw new RuntimeException("Unable to open WAL: {$filePath}");
            }
            if (!flock($fh, LOCK_EX)) {
                fclose($fh);
                throw new RuntimeException("Unable to lock WAL: {$filePath}");
            }

            $size = filesize($filePath);

            if ($size === false || $size < 50) {
                // New WAL - write full HTML page
                ftruncate($fh, 0);
                rewind($fh);
                $header = $this->pageBuilder->buildWalHeader($table);
                $footer = $this->pageBuilder->buildWalFooter();
                fwrite($fh, $header . $trContent . $footer);
            } else {
                // Existing WAL - find </tbody> and insert before it.
                // Window must comfortably exceed the footer size or the marker
                // gets lost and the file degrades to raw-append mode.
                $tailLen = min($size, 4096);
                fseek($fh, -$tailLen, SEEK_END);
                $tail = fread($fh, $tailLen);

                $marker = '</tbody>';
                $pos = strrpos($tail, $marker);
                if ($pos !== false) {
                    // Calculate absolute position of </tbody>
                    $absPos = $size - $tailLen + $pos;
                    // Read everything from </tbody> onwards
                    fseek($fh, $absPos);
                    $remainder = fread($fh, $size - $absPos);
                    // Seek back and write new rows + remainder
                    fseek($fh, $absPos);
                    fwrite($fh, $trContent . $remainder);
                } else {
                    // Fallback: just append (broken HTML, but data is safe)
                    fseek($fh, 0, SEEK_END);
                    fwrite($fh, $trContent);
                }
            }

            fflush($fh);
            flock($fh, LOCK_UN);
            fclose($fh);
        }

        /**
         * Ensure the base storage directory exists with security files.
         */
        private function ensureStorageExists(): void
        {
            $dir = $this->config->basePath;
            if (!is_dir($dir)) {
                mkdir($dir, 0755, true);
            }

            // Security: .htaccess. Default is deny-all - the storage files
            // (chunks, WAL) contain raw row data and must not be web-readable.
            // Set HTMLDB_BROWSE=true in wp-config.php to expose the browsable
            // HTML browser (index/chunk/WAL pages) for demos; internals stay
            // denied either way. Apache-only: on nginx deny the directory via
            // a location block.
            $htaccessPath = $dir . '/.htaccess';
            $htaccess = $this->config->browse
                ? implode("\n", [
                    '# HtmlDB - browse mode: pages yes, raw internals no',
                    '<IfModule mod_authz_core.c>',
                    '    <FilesMatch "(^\\.|\\.tmp$|\\.lock$|^_meta\\.json$|^_schema\\.json$|^_global\\.seq$)">',
                    '        Require all denied',
                    '    </FilesMatch>',
                    '    Require all granted',
                    '</IfModule>',
                    '<IfModule !mod_authz_core.c>',
                    '    <FilesMatch "(^\\.|\\.tmp$|\\.lock$|^_meta\\.json$|^_schema\\.json$|^_global\\.seq$)">',
                    '        Order allow,deny',
                    '        Deny from all',
                    '    </FilesMatch>',
                    '    Order allow,deny',
                    '    Allow from all',
                    '</IfModule>',
                    '',
                ])
                : implode("\n", [
                    '# HtmlDB - storage is private by default.',
                    '# Set HTMLDB_BROWSE=true in wp-config.php to enable the',
                    '# browsable HTML database viewer.',
                    '<IfModule mod_authz_core.c>',
                    '    Require all denied',
                    '</IfModule>',
                    '<IfModule !mod_authz_core.c>',
                    '    Order allow,deny',
                    '    Deny from all',
                    '</IfModule>',
                    '',
                ]);
            if (!file_exists($htaccessPath) || (string) @file_get_contents($htaccessPath) !== $htaccess) {
                file_put_contents($htaccessPath, $htaccess);
            }

            // Security: index.html
            $indexPath = $dir . '/index.html';
            if (!file_exists($indexPath)) {
                file_put_contents($indexPath, '<!DOCTYPE html><html><head><meta charset="UTF-8">'
                    . '<link rel="stylesheet" href="_style.css"><title>HtmlDB</title></head>'
                    . '<body><h1>⛔ Access Denied</h1>'
                    . '<p>This is a database storage directory. Direct access is not permitted.</p>'
                    . '</body></html>');
            }

            // CSS: retro-terminal theme
            $cssPath = $dir . '/_style.css';
            if (!file_exists($cssPath)) {
                $this->writeRetroCSS($cssPath);
            }

            // Global TX sequence
            $globalSeq = $dir . '/_global.seq';
            if (!file_exists($globalSeq)) {
                file_put_contents($globalSeq, '0');
            }
        }

        /**
         * Write the retro-terminal CSS theme file.
         */
        private function writeRetroCSS(string $path): void
        {
            $css = <<<'CSS'
/* HtmlDB v3.0 - Retro Terminal Theme */

:root {
  --bg: #0a0a0a;
  --fg: #00ff41;
  --fg-dim: #00aa2a;
  --fg-bright: #33ff66;
  --accent: #ff6600;
  --border: #1a3a1a;
  --glow: 0 0 5px #00ff41, 0 0 10px rgba(0,255,65,0.3);
  --font: 'JetBrains Mono', 'Courier New', monospace;
}

* { box-sizing: border-box; margin: 0; padding: 0; }

html {
  font-size: 13px;
  scrollbar-color: var(--fg-dim) var(--bg);
}

body {
  background: var(--bg);
  color: var(--fg);
  font-family: var(--font);
  line-height: 1.5;
  padding: 1.5rem;
  min-height: 100vh;
}

/* Scanline effect */
body::after {
  content: '';
  position: fixed;
  inset: 0;
  background: repeating-linear-gradient(
    0deg,
    rgba(0,0,0,0.15) 0px,
    rgba(0,0,0,0.15) 1px,
    transparent 1px,
    transparent 3px
  );
  pointer-events: none;
  z-index: 9999;
}

h1 {
  font-size: 1.6rem;
  text-shadow: var(--glow);
  margin-bottom: 0.5rem;
  letter-spacing: 0.05em;
}

h2 {
  font-size: 1.2rem;
  color: var(--fg-dim);
  margin: 1rem 0 0.5rem;
  text-transform: uppercase;
  letter-spacing: 0.1em;
}

header {
  border-bottom: 1px solid var(--border);
  padding-bottom: 0.8rem;
  margin-bottom: 1rem;
}

nav {
  margin: 0.5rem 0;
}

nav a, nav span {
  display: inline-block;
  padding: 0.25rem 0.75rem;
  margin-right: 0.25rem;
  text-decoration: none;
  border: 1px solid var(--border);
}

nav a {
  color: var(--fg);
  transition: all 0.2s;
}

nav a:hover {
  background: var(--fg);
  color: var(--bg);
  text-shadow: none;
  box-shadow: var(--glow);
}

nav .current {
  color: var(--accent);
  border-color: var(--accent);
  font-weight: bold;
}

nav .disabled {
  color: #333;
  border-color: #1a1a1a;
}

.info {
  color: var(--fg-dim);
  font-size: 0.85rem;
  margin: 0.25rem 0;
}

.stats {
  background: #0d1a0d;
  border: 1px solid var(--border);
  padding: 0.75rem 1rem;
  margin-bottom: 1rem;
}

.stats strong {
  color: var(--fg-bright);
}

table {
  width: 100%;
  border-collapse: collapse;
  margin: 0.5rem 0;
  font-size: 0.85rem;
}

thead tr {
  background: #0d1a0d;
  border-bottom: 2px solid var(--fg-dim);
}

th {
  text-align: left;
  padding: 0.5rem;
  color: var(--fg-bright);
  font-weight: 700;
  text-transform: uppercase;
  font-size: 0.75rem;
  letter-spacing: 0.08em;
  white-space: nowrap;
}

td {
  padding: 0.35rem 0.5rem;
  border-bottom: 1px solid var(--border);
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--fg-dim);
}

tr:hover td {
  background: #0d1a0d;
  color: var(--fg);
}

/* PK column highlight */
td:first-child {
  color: var(--accent);
  font-weight: 700;
}

footer {
  border-top: 1px solid var(--border);
  margin-top: 1.5rem;
  padding-top: 0.8rem;
  font-size: 0.75rem;
  color: #444;
}

.chunk-list {
  list-style: none;
}

.chunk-list li {
  padding: 0.3rem 0;
  border-bottom: 1px dotted var(--border);
}

.chunk-list a {
  color: var(--fg);
  text-decoration: none;
}

.chunk-list a:hover {
  text-shadow: var(--glow);
}

/* Blinking cursor effect on h1 */
h1::after {
  content: '█';
  animation: blink 1s step-end infinite;
}

@keyframes blink {
  50% { opacity: 0; }
}
CSS;
            file_put_contents($path, $css);
        }
    }
}

// ---------------------------------------------------------------------------
// Namespace: HtmlDatabase\Parser - SQL Tokenizer & XPath Translator
// ---------------------------------------------------------------------------
namespace HtmlDatabase\Parser {

    use HtmlDatabase\Core\ShardedStorageManager;

    /**
     * Strip table/alias prefixes (wp_posts.ID -> ID, t.* -> *) from a SQL
     * fragment WITHOUT touching quoted string literals.
     *
     * A naive regex over the whole statement mangles dotted values that live
     * inside quotes - "option_name='core_updater.lock'" would become
     * "option_name='lock'" and the row could never be found again. So quoted
     * spans are lifted out behind placeholders, prefixes are stripped from
     * the bare skeleton, and the literals are restored verbatim afterwards.
     */
    function stripTablePrefixes(string $sql): string
    {
        $literals = [];
        $skeleton = preg_replace_callback(
            "/'(?:\\\\.|[^'\\\\])*'|\"(?:\\\\.|[^\"\\\\])*\"/",
            static function (array $m) use (&$literals): string {
                $literals[] = $m[0];
                return "\x01" . (count($literals) - 1) . "\x02";
            },
            $sql
        );
        if ($skeleton === null) {
            return $sql;
        }

        $skeleton = preg_replace('/\b[a-zA-Z0-9_]+\.\*/', '*', $skeleton);
        $skeleton = preg_replace('/\b[a-zA-Z0-9_]+\.([a-zA-Z0-9_]+)/', '$1', $skeleton);

        if ($literals !== []) {
            $skeleton = preg_replace_callback(
                '/\x01(\d+)\x02/',
                static fn (array $m): string => $literals[(int) $m[1]] ?? $m[0],
                $skeleton
            );
        }

        return $skeleton;
    }

    /**
     * Character-level state-machine tokenizer for SQL INSERT statements.
     *
     * Handles:
     *  - Multi-row: INSERT INTO t (a,b) VALUES (1,2),(3,4);
     *  - Nested parentheses inside values (serialised PHP arrays, JSON, etc.).
     *  - Single-quote escaping via backslash and doubled single-quotes.
     *  - Strips ON DUPLICATE KEY UPDATE clause on the fly.
     *  - Statement modifiers: IGNORE (duplicate rows skipped), LOW_PRIORITY,
     *    HIGH_PRIORITY, DELAYED (accepted, no semantic effect).
     */
    final class InsertTokenizer
    {
        /**
         * Parse a full INSERT statement.
         *
         * @return array{table: string, columns: string[], rows: array[], onDuplicate: ?array, ignore: bool}|null
         */
        public function tokenize(string $sql): ?array
        {
            $sql = trim($sql);

            // 1. Strip trailing semicolons
            $sql = rtrim($sql, "; \t\n\r");

            // 2. Capture ON DUPLICATE KEY UPDATE clause (state-machine aware)
            $onDup = $this->extractOnDuplicate($sql);
            $sql = $this->stripOnDuplicate($sql);

            // 3. Statement modifiers between INSERT and the table name.
            //    IGNORE carries semantics: rows colliding with a unique key
            //    are skipped instead of failing. Core uses it for option
            //    locks (WP_Upgrader::create_lock, taxonomy/comment locks).
            $ignore = false;
            $sql = preg_replace_callback(
                '/^INSERT\s+((?:(?:LOW_PRIORITY|HIGH_PRIORITY|DELAYED|IGNORE)\s+)+)/i',
                function (array $m) use (&$ignore): string {
                    if (stripos($m[1], 'IGNORE') !== false) $ignore = true;
                    return 'INSERT ';
                },
                $sql
            ) ?? $sql;

            // 4. Extract table name
            if (!preg_match('/INSERT\s+(?:INTO\s+)?[`]?([a-zA-Z0-9_]+)[`]?\s*\(/i', $sql, $m)) {
                return null;
            }
            $table = $m[1];

            // 4. Locate the columns block (first parenthesised group)
            $pos = strpos($sql, '(');
            if ($pos === false) {
                return null;
            }
            $columnsRaw = $this->extractParenGroup($sql, $pos);
            if ($columnsRaw === null) {
                return null;
            }
            $columns = array_map(
                fn(string $c): string => trim($c, " `\t\n\r\0\x0B"),
                explode(',', $columnsRaw['content'])
            );

            // 5. Advance past VALUES keyword
            $rest = ltrim(substr($sql, $columnsRaw['endPos'] + 1));
            if (!preg_match('/^VALUES\s*/i', $rest, $vm)) {
                return null;
            }
            $rest = substr($rest, strlen($vm[0]));

            // 6. Extract every (...) value group
            $rows = [];
            while (($rest = ltrim($rest)) !== '' && $rest[0] === '(') {
                $group = $this->extractParenGroup($rest, 0);
                if ($group === null) {
                    break;
                }
                $rows[] = $this->splitValues($group['content']);
                $rest = ltrim(substr($rest, $group['endPos'] + 1));
                if (isset($rest[0]) && $rest[0] === ',') {
                    $rest = substr($rest, 1);
                }
            }

            if (empty($rows)) {
                return null;
            }

            return ['table' => $table, 'columns' => $columns, 'rows' => $rows, 'onDuplicate' => $onDup, 'ignore' => $ignore];
        }

        private function extractParenGroup(string $sql, int $startPos): ?array
        {
            $len   = strlen($sql);
            $depth = 0;
            $inStr = false;
            $strCh = '';
            $esc   = false;
            $start = null;

            for ($i = $startPos; $i < $len; $i++) {
                $ch = $sql[$i];
                if ($esc) { $esc = false; continue; }
                if ($ch === '\\') { $esc = true; continue; }
                if (($ch === "'" || $ch === '"') && !$inStr) {
                    $inStr = true; $strCh = $ch; continue;
                }
                if ($inStr) {
                    if ($ch === $strCh) {
                        if (isset($sql[$i + 1]) && $sql[$i + 1] === $strCh) { $i++; continue; }
                        $inStr = false;
                    }
                    continue;
                }
                if ($ch === '(') {
                    if ($depth === 0) $start = $i + 1;
                    $depth++;
                } elseif ($ch === ')') {
                    $depth--;
                    if ($depth === 0 && $start !== null) {
                        return ['content' => substr($sql, $start, $i - $start), 'endPos' => $i];
                    }
                }
            }
            return null;
        }

        private function splitValues(string $raw): array
        {
            $values = [];
            $buf    = '';
            $inStr  = false;
            $strCh  = '';
            $esc    = false;
            $depth  = 0;
            $len    = strlen($raw);

            for ($i = 0; $i < $len; $i++) {
                $ch = $raw[$i];
                if ($esc) { $buf .= $ch; $esc = false; continue; }
                if ($ch === '\\') { $esc = true; if ($inStr) $buf .= $ch; continue; }
                if (($ch === "'" || $ch === '"') && !$inStr) {
                    $inStr = true; $strCh = $ch; continue;
                }
                if ($inStr) {
                    if ($ch === $strCh) {
                        if (isset($raw[$i + 1]) && $raw[$i + 1] === $strCh) { $buf .= $ch; $i++; continue; }
                        $inStr = false; $strCh = '';
                        continue;
                    }
                    $buf .= $ch;
                    continue;
                }
                if ($ch === '(') { $depth++; $buf .= $ch; continue; }
                if ($ch === ')') { $depth--; $buf .= $ch; continue; }
                if ($ch === ',' && $depth === 0) { $values[] = $this->cleanValue($buf); $buf = ''; continue; }
                $buf .= $ch;
            }
            $values[] = $this->cleanValue($buf);
            return $values;
        }

        private function cleanValue(string $v): string
        {
            $v = trim($v);
            if (strcasecmp($v, 'NULL') === 0) return '';
            if (preg_match('/^(NOW|CURRENT_TIMESTAMP|CURRENT_DATE|CURDATE)\s*(\(\s*\))?$/i', $v)) {
                return gmdate('Y-m-d H:i:s');
            }
            if (preg_match('/^UNIX_TIMESTAMP\(\)$/i', $v)) {
                return (string) time();
            }
            return stripslashes($v);
        }

        /**
         * Return the raw "col = val, ..." part of an ON DUPLICATE KEY UPDATE
         * clause (without the keyword), or null when absent.
         */
        private function extractOnDuplicate(string $sql): ?array
        {
            $upper = strtoupper($sql);
            $inStr = false;
            $esc   = false;
            $len   = strlen($sql);

            for ($i = 0; $i < $len; $i++) {
                $ch = $sql[$i];
                if ($esc) { $esc = false; continue; }
                if ($ch === '\\') { $esc = true; continue; }
                if ($ch === "'") {
                    if ($inStr && isset($sql[$i + 1]) && $sql[$i + 1] === "'") { $i++; continue; }
                    $inStr = !$inStr;
                    continue;
                }
                if ($inStr) continue;
                if ($upper[$i] === 'O' && substr($upper, $i, 22) === 'ON DUPLICATE KEY UPDATE') {
                    $clause = trim(substr($sql, $i + 21));
                    if ($clause === '') return null;
                    $pairs = [];
                    foreach ($this->splitAssignments($clause) as $assign) {
                        if (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*(.*)$/s', trim($assign), $m)) {
                            $pairs[$m[1]] = trim($m[2]);
                        }
                    }
                    return $pairs ?: null;
                }
            }
            return null;
        }

        /** Split "a = 'x', b = 2" on top-level commas. */
        private function splitAssignments(string $clause): array
        {
            $out = [];
            $buf = '';
            $inStr = false;
            $esc = false;
            $len = strlen($clause);
            for ($i = 0; $i < $len; $i++) {
                $ch = $clause[$i];
                if ($esc) { $buf .= $ch; $esc = false; continue; }
                if ($ch === '\\') { $esc = true; $buf .= $ch; continue; }
                if ($ch === "'") { $inStr = !$inStr; $buf .= $ch; continue; }
                if ($ch === ',' && !$inStr) { $out[] = $buf; $buf = ''; continue; }
                $buf .= $ch;
            }
            if (trim($buf) !== '') $out[] = $buf;
            return $out;
        }

        private function stripOnDuplicate(string $sql): string
        {
            $upper = strtoupper($sql);
            $inStr = false;
            $esc   = false;
            $len   = strlen($sql);

            for ($i = 0; $i < $len; $i++) {
                $ch = $sql[$i];
                if ($esc) { $esc = false; continue; }
                if ($ch === '\\') { $esc = true; continue; }
                if ($ch === "'") {
                    if ($inStr && isset($sql[$i + 1]) && $sql[$i + 1] === "'") { $i++; continue; }
                    $inStr = !$inStr;
                    continue;
                }
                if ($inStr) continue;
                if ($upper[$i] === 'O' && substr($upper, $i, 12) === 'ON DUPLICATE') {
                    return rtrim(substr($sql, 0, $i));
                }
            }
            return $sql;
        }
    }

    /**
     * Translates SQL SELECT statements and executes them against the
     * sharded HTML storage using the ShardedStorageManager.
     *
     * v3: No more DOMDocument - uses string-based parsing from
     * ShardedStorageManager for chunk + WAL merging.
     */
    final class SqlToXpathTranslator
    {
        /** Total filtered rows before LIMIT (for SQL_CALC_FOUND_ROWS). */
        public int $calcFoundRows = 0;

        public function __construct(
            private string $storagePath,
            private ?ShardedStorageManager $storage = null
        ) {
            $this->storagePath = rtrim($storagePath, '/');
        }

        public function setStorage(ShardedStorageManager $storage): void
        {
            $this->storage = $storage;
        }

        /**
         * Execute a SELECT query and return an array of stdClass row objects.
         */
        public function executeSelect(string $sql): array
        {
            $parsed = $this->parseSql($sql);
            if ($parsed === null) {
                return [];
            }

            return $this->queryShardedStorage($parsed);
        }

        /**
         * Parse SQL into structured query components.
         */
        private function parseSql(string $sql): ?array
        {
            // Remove backticks globally
            $sql = str_replace('`', '', $sql);

            // Detect SQL_CALC_FOUND_ROWS flag
            $calcFoundRows = (bool) preg_match('/\bSQL_CALC_FOUND_ROWS\b/i', $sql);
            $sql = preg_replace('/\bSQL_CALC_FOUND_ROWS\b/i', '', $sql);

            // FROM clause: a comma-separated table list up to the first
            // JOIN/WHERE/GROUP/ORDER/LIMIT/HAVING keyword. Comma tables are
            // implicit INNER joins whose ON condition lives in WHERE
            // (wp_update_term_count_now and friends emit this form).
            if (!preg_match('/\bFROM\b\s+(.*?)(?=\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|$)/is', $sql, $fromMatch)) {
                return null;
            }
            $fromClause = trim($fromMatch[1]);

            // Split off explicit JOIN clauses; the prefix is the table list.
            $joinKwOffset = strlen($fromClause);
            if (preg_match('/\b(?:INNER|LEFT|RIGHT|CROSS|JOIN)\b/i', $fromClause, $jk, PREG_OFFSET_CAPTURE)) {
                $joinKwOffset = $jk[0][1];
            }
            $tableList = trim(substr($fromClause, 0, $joinKwOffset));

            $sqlKw = ['WHERE', 'ORDER', 'GROUP', 'LIMIT', 'INNER', 'LEFT', 'RIGHT',
                      'CROSS', 'JOIN', 'ON', 'SET', 'VALUES', 'UNION', 'HAVING', 'FOR'];

            $tableEntries = array_map('trim', explode(',', $tableList));
            $mainRaw = $tableEntries[0] ?? '';
            if ($mainRaw === '') return null;
            $mainTokens = preg_split('/\s+/', trim($mainRaw));
            $table     = array_shift($mainTokens);
            $mainAlias = $table;
            foreach ($mainTokens as $tk) {
                if (strcasecmp($tk, 'AS') === 0) continue;
                if (in_array(strtoupper($tk), $sqlKw, true)) continue;
                $mainAlias = $tk;
                break;
            }

            // Additional comma tables: alias => table (implicit joins).
            $commaTables = [];
            for ($ti = 1; $ti < count($tableEntries); $ti++) {
                $toks = preg_split('/\s+/', trim($tableEntries[$ti]));
                if (empty($toks) || $toks[0] === '') continue;
                $cTable = $toks[0];
                $cAlias = $cTable;
                foreach ($toks as $tk) {
                    if (strcasecmp($tk, 'AS') === 0) continue;
                    if (in_array(strtoupper($tk), $sqlKw, true)) continue;
                    $cAlias = $tk;
                    break;
                }
                $commaTables[$cAlias] = $cTable;
            }

            // JOIN specs: alias => join graph edge toward its parent alias
            $joins = [];
            if (preg_match_all(
                '/(LEFT|RIGHT|INNER|CROSS)?\s*JOIN\s+([a-zA-Z0-9_]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_]+))?\s+ON\s*\(?\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*=\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*\)?/i',
                $sql, $jm, PREG_SET_ORDER)) {
                foreach ($jm as $j) {
                    $joinType = strtoupper($j[1] ?? '');
                    $jTable   = $j[2];
                    $jAlias   = ($j[3] ?? '') !== '' ? $j[3] : $j[2];
                    if ($j[4] === $jAlias) {
                        $parent = $j[6]; $parentCol = $j[7]; $thisCol = $j[5];
                    } elseif ($j[6] === $jAlias) {
                        $parent = $j[4]; $parentCol = $j[5]; $thisCol = $j[7];
                    } else {
                        continue;
                    }
                    $joins[$jAlias] = [
                        'table'     => $jTable,
                        'parent'    => $parent,
                        'parentCol' => $parentCol,
                        'thisCol'   => $thisCol,
                        'left'      => $joinType === 'LEFT' || $joinType === 'RIGHT',
                    ];
                }
            }

            // Raw WHERE - extracted BEFORE prefix stripping so that
            // alias-qualified conditions can be attributed to joined tables.
            $rawWhere = null;
            if (preg_match('/WHERE\s+(.*?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|\s+LIMIT|\s+HAVING|$)/is', $sql, $wm)) {
                $rawWhere = trim($wm[1]);
            }

            // Wire comma tables into the join graph. Their ON condition lives
            // in WHERE as "alias.col = other.col"; extract it and drop it from
            // the residual WHERE so it is not double-applied as a filter.
            if (!empty($commaTables) && $rawWhere !== null) {
                $known = array_merge([$mainAlias => $table], $joins ? array_map(fn($s) => $s['table'], $joins) : []);
                foreach ($commaTables as $cAlias => $cTable) {
                    $known[$cAlias] = $cTable;
                }
                $remaining = $rawWhere;
                foreach ($commaTables as $cAlias => $cTable) {
                    // Find "cAlias.col = other.col" or "other.col = cAlias.col"
                    $pattern = '/\b(' . preg_quote($cAlias, '/') . ')\.([a-zA-Z0-9_]+)\s*=\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b|\b([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*=\s*(' . preg_quote($cAlias, '/') . ')\.([a-zA-Z0-9_]+)\b/i';
                    if (preg_match($pattern, $remaining, $cj, PREG_OFFSET_CAPTURE)) {
                        if (isset($cj[1]) && $cj[1][1] !== -1) {
                            $thisCol   = $cj[2][0];
                            $pAlias    = $cj[3][0];
                            $parentCol = $cj[4][0];
                        } else {
                            $pAlias    = $cj[5][0];
                            $parentCol = $cj[6][0];
                            $thisCol   = $cj[8][0];
                        }
                        if (isset($known[$pAlias]) || $pAlias === $mainAlias) {
                            $joins[$cAlias] = [
                                'table'     => $cTable,
                                'parent'    => $pAlias,
                                'parentCol' => $parentCol,
                                'thisCol'   => $thisCol,
                                'left'      => false,
                            ];
                            // Remove the matched condition (and a trailing/leading AND)
                            $full = $cj[0][0];
                            $off  = $cj[0][1];
                            $before = substr($remaining, 0, $off);
                            $after  = substr($remaining, $off + strlen($full));
                            $before = preg_replace('/\s+AND\s*$/i', '', $before);
                            $after  = preg_replace('/^\s*AND\s+/i', '', $after);
                            $remaining = trim($before . ' ' . $after);
                        }
                    }
                }
                $rawWhere = $remaining !== '' ? $remaining : null;
            }

            // Route alias-qualified conditions on joined tables into joinFilters
            $joinFilters = [];
            if ($rawWhere !== null && !empty($joins)) {
                $kept = [];
                // Flatten first: a parenthesized group like
                // ( pm.meta_key = 'x' AND pm.meta_value = 'y' ) must have its
                // inner ANDs split before each atomic condition can be routed
                // to its join. OR groups are preserved intact by the flattener.
                $flatParts = [];
                foreach ($this->splitOnTopLevelAnd($rawWhere) as $part) {
                    foreach ($this->flattenWherePart($part) as $fp) {
                        $flatParts[] = $fp;
                    }
                }
                foreach ($flatParts as $part) {
                    $p = $this->stripOuterParens(trim($part));

                    // OR group over the same joined column → IN filter
                    if (preg_match('/\bOR\b/i', $p)) {
                        $ops  = preg_split('/\s+OR\s+/i', $p);
                        $a = $c = null; $vals = []; $ok = true;
                        foreach ($ops as $op) {
                            $op = $this->stripOuterParens(trim($op));
                            if (preg_match('/^([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*=\s*[\'"](.*?)[\'"]$/s', $op, $om)
                                && isset($joins[$om[1]])) {
                                if ($a === null) { $a = $om[1]; $c = $om[2]; }
                                elseif ($a !== $om[1] || $c !== $om[2]) { $ok = false; break; }
                                $vals[] = $om[3];
                            } else { $ok = false; break; }
                        }
                        if ($ok && $a !== null) {
                            $joinFilters[] = ['alias' => $a, 'col' => $c, 'op' => 'in', 'vals' => $vals];
                            continue;
                        }
                    }

                    if (preg_match('/^([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*(.*)$/s', $p, $am)
                        && isset($joins[$am[1]])) {
                        $jf = $this->parseJoinFilter($am[1], $am[2], trim($am[3]));
                        if ($jf !== null) { $joinFilters[] = $jf; continue; }
                    }
                    $kept[] = $part;
                }
                $rawWhere = empty($kept) ? null : implode(' AND ', $kept);
            }

            // Raw SELECT list - captured BEFORE prefix stripping so that
            // alias-qualified columns (t.*, tt.count) can drive join fan-out.
            preg_match('/SELECT\s+(DISTINCT\s+)?(.*?)\s+FROM/is', $sql, $colMatch);
            $distinct = !empty($colMatch[1]);
            $rawSelect = trim($colMatch[2] ?? '*');

            $selectQualified = []; // [alias, col|'*'] pairs referencing joins
            if (preg_match_all('/\b([a-zA-Z0-9_]+)\.(\*|[a-zA-Z0-9_]+)/', $rawSelect, $qcm, PREG_SET_ORDER)) {
                foreach ($qcm as $qc) {
                    if (isset($joins[$qc[1]])) {
                        $selectQualified[] = ['alias' => $qc[1], 'col' => $qc[2]];
                    }
                }
            }

            // Strip table/alias prefixes: wp_posts.* → *, wp_posts.ID → ID.
            // Quoted literals are protected so dotted values survive.
            $sql = stripTablePrefixes($sql);
            if ($rawWhere !== null) {
                $rawWhere = stripTablePrefixes($rawWhere);
            }

            // Columns: normalize "expr AS alias" to the alias, drop function
            // expressions (aggregates are computed separately). Capture the
            // select list with a match - a preg_replace here would delete the
            // FROM keyword and glue the rest of the query onto the last column.
            $rawCols = preg_match('/SELECT\s+(?:DISTINCT\s+)?(.*?)\s+FROM/is', $sql, $rcm)
                ? $rcm[1] : '*';
            $columns = [];
            foreach (array_map('trim', explode(',', $rawCols)) as $c) {
                if ($c === '') continue;
                if (preg_match('/\bAS\s+([a-zA-Z0-9_]+)$/i', $c, $am)) {
                    $columns[] = $am[1];
                } elseif (preg_match('/^[a-zA-Z0-9_*]+$/', $c)) {
                    $columns[] = $c;
                }
            }
            if (empty($columns)) $columns = ['*'];

            // Aggregates (per-group when GROUP BY is present, else global)
            $aggregates = [];
            if (preg_match_all(
                '/\b(COUNT|MAX|MIN|SUM|AVG)\s*\(\s*(DISTINCT\s+)?(\*|[a-zA-Z0-9_]+)\s*\)(?:\s+(?:AS\s+)?([a-zA-Z0-9_]+))?/i',
                $rawCols, $agm, PREG_SET_ORDER)) {
                foreach ($agm as $ag) {
                    $aggregates[] = [
                        'fn'       => strtoupper($ag[1]),
                        'distinct' => !empty($ag[2]),
                        'col'      => $ag[3],
                        'alias'    => ($ag[4] ?? '') !== '' ? $ag[4] : trim($ag[0]),
                    ];
                }
            }

            // GROUP BY
            $groupBy = [];
            if (preg_match('/GROUP\s+BY\s+(.*?)(?:\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|$)/is', $sql, $gm)) {
                foreach (explode(',', $gm[1]) as $g) {
                    $g = trim($g);
                    if (preg_match('/^[a-zA-Z0-9_]+$/', $g)) {
                        $groupBy[] = $g;
                    }
                }
            }

            // HAVING (simple col op value against aggregate aliases)
            $having = [];
            if (preg_match('/HAVING\s+(.*?)(?:\s+ORDER\s+BY|\s+LIMIT|$)/is', $sql, $hm)) {
                foreach ($this->splitOnTopLevelAnd($hm[1]) as $hp) {
                    $hp = trim($hp);
                    if (preg_match('/^([a-zA-Z0-9_]+)\s*(>=|<=|!=|<>|>|<|=)\s*(-?\d+(?:\.\d+)?|\'[^\']*\'|"[^"]*")$/s', $hp, $hcm)) {
                        $having[] = ['col' => $hcm[1], 'op' => $hcm[2], 'val' => trim($hcm[3], "'\"")];
                    }
                }
            }

            // LIMIT
            $limit  = null;
            $offset = 0;
            if (preg_match('/LIMIT\s+(\d+)\s*,\s*(\d+)/i', $sql, $limMatch)) {
                $offset = (int) $limMatch[1];
                $limit  = (int) $limMatch[2];
            } elseif (preg_match('/LIMIT\s+(\d+)\s+OFFSET\s+(\d+)/i', $sql, $limMatch)) {
                $limit  = (int) $limMatch[1];
                $offset = (int) $limMatch[2];
            } elseif (preg_match('/LIMIT\s+(\d+)/i', $sql, $limMatch)) {
                $limit = (int) $limMatch[1];
            }

            // ORDER BY (multi-column)
            $orderBy = [];
            if (preg_match('/ORDER\s+BY\s+(.*?)(?:\s+LIMIT|$)/is', $sql, $ordMatch)) {
                foreach (explode(',', $ordMatch[1]) as $term) {
                    $term = trim($term);
                    if (preg_match('/^([a-zA-Z0-9_]+)(?:\s+(ASC|DESC))?$/i', $term, $tm)) {
                        $orderBy[] = ['col' => $tm[1], 'dir' => strtoupper($tm[2] ?? 'ASC')];
                    }
                }
            }

            // Parse conditions
            $conditions       = [];
            $inConditions     = [];
            $notConditions    = [];
            $notInConditions  = [];
            $comparisons      = []; // [ ['col'=>..,'op'=>..,'val'=>..], ... ]
            $likeConditions   = [];
            $notLikeConditions = [];
            $nullConditions   = [];
            $notNullConditions = [];
            $betweenConditions = []; // [ ['col'=>..,'not'=>bool,'low'=>..,'high'=>..], ... ]
            $funcConditions   = []; // [ ['fn'=>YEAR,'col'=>..,'op'=>'=','val'=>N], ... ]
            $orGroups         = []; // list of disjunctions: [ [cond, cond...], ... ]

            if ($rawWhere !== null) {
                // Parenthesis-aware AND splitter: only split on AND at depth 0
                $topParts = $this->splitOnTopLevelAnd($rawWhere);
                // Flatten any parenthesized compound expressions
                $parts = [];
                foreach ($topParts as $tp) {
                    foreach ($this->flattenWherePart($tp) as $flat) {
                        $parts[] = $flat;
                    }
                }
                foreach ($parts as $part) {
                    $part = trim($part);

                    // Tautology / contradiction
                    if (preg_match('/^(\d+)\s*=\s*(\d+)$/', $part, $taut)) {
                        if ($taut[1] !== $taut[2]) {
                            return [
                                'columns' => $columns, 'table' => $table,
                                'conditions' => ['__contradiction__' => '__never__'],
                                'inConditions' => [], 'notConditions' => [],
                                'notInConditions' => [], 'comparisons' => [],
                                'likeConditions' => [], 'notLikeConditions' => [],
                                'nullConditions' => [], 'notNullConditions' => [],
                                'funcConditions' => [],
                                'orGroups' => [], 'joins' => [], 'joinFilters' => [],
                                'aggregates' => [], 'distinct' => false,
                                'mainAlias' => $mainAlias, 'selectQualified' => [],
                                'groupBy' => [], 'having' => [],
                                'limit' => 0, 'offset' => 0,
                                'orderBy' => [],
                                'rawWhere' => $rawWhere,
                                'calcFoundRows' => $calcFoundRows,
                            ];
                        }
                        continue;
                    }

                    // Date function conditions: YEAR(col) = N, MONTH(col) >= N ...
                    // Must be tested before plain "col = N" which would otherwise
                    // capture the column inside the function call.
                    if (preg_match('/^(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND|WEEK|QUARTER)\s*\(\s*([a-zA-Z0-9_]+)\s*\)\s*(=|!=|<>|>=|<=|>|<)\s*(-?\d+)$/si', $part, $fn)) {
                        $funcConditions[] = ['fn' => strtoupper($fn[1]), 'col' => $fn[2], 'op' => $fn[3], 'val' => (int) $fn[4]];
                        continue;
                    }

                    // OR group → IN when same column; generic OR list otherwise.
                    // Never silently drop: a dropped OR over-matches rows.
                    if (preg_match('/\bOR\b/i', $part)) {
                        $inner = $this->stripOuterParens($part);
                        $orParts = preg_split('/\s+OR\s+/i', $inner);
                        $orCol = null;
                        $orVals = [];
                        $orConds = [];
                        $valid = true;
                        foreach ($orParts as $op) {
                            $op = trim($op);
                            while (preg_match('/^\((.+)\)$/s', $op, $pm)) { $op = trim($pm[1]); }
                            if (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*[\'"](.*?)[\'"]$/s', $op, $om)) {
                                if ($orCol === null) $orCol = $om[1];
                                if ($om[1] === $orCol) { $orVals[] = $om[2]; $orConds[] = ['eq', $om[1], $om[2]]; }
                                else { $orConds[] = ['eq', $om[1], $om[2]]; $valid = false; }
                            } elseif (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*(-?\d+(?:\.\d+)?)$/s', $op, $om)) {
                                if ($orCol === null) $orCol = $om[1];
                                if ($om[1] === $orCol) { $orVals[] = $om[2]; $orConds[] = ['eq', $om[1], $om[2]]; }
                                else { $orConds[] = ['eq', $om[1], $om[2]]; $valid = false; }
                            } elseif (preg_match('/^([a-zA-Z0-9_]+)\s*(>=|<=|>|<)\s*(-?\d+(?:\.\d+)?|\'[^\']*\'|"[^"]*")$/s', $op, $om)) {
                                $orConds[] = ['cmp', $om[1], $om[2], trim($om[3], "'\"")];
                                $valid = false;
                            } elseif (preg_match('/^([a-zA-Z0-9_]+)\s+NOT\s+LIKE\s+[\'"](.*?)[\'"]$/is', $op, $om)) {
                                $orConds[] = ['not_like', $om[1], $om[2]];
                                $valid = false;
                            } elseif (preg_match('/^([a-zA-Z0-9_]+)\s+LIKE\s+[\'"](.*?)[\'"]$/is', $op, $om)) {
                                $orConds[] = ['like', $om[1], $om[2]];
                                $valid = false;
                            } else { $valid = false; break; }
                        }
                        if ($valid && $orCol !== null && !empty($orVals)) {
                            $inConditions[$orCol] = $orVals;
                        } elseif (!empty($orConds)) {
                            // Mixed-column OR: keep as disjunction, evaluated at filter time
                            $orGroups[] = $orConds;
                        }
                        // If nothing could be parsed, fall through to no-op:
                        // an unparseable OR must NOT vanish silently.
                        continue;
                    }

                    // IS NOT NULL
                    if (preg_match('/^([a-zA-Z0-9_]+)\s+IS\s+NOT\s+NULL$/i', $part, $nn)) {
                        $notNullConditions[] = $nn[1];
                        continue;
                    }

                    // IS NULL
                    if (preg_match('/^([a-zA-Z0-9_]+)\s+IS\s+NULL$/i', $part, $nl)) {
                        $nullConditions[] = $nl[1];
                        continue;
                    }

                    // col NOT LIKE 'pattern'
                    if (preg_match('/^([a-zA-Z0-9_]+)\s+NOT\s+LIKE\s+[\'"](.*?)[\'"]$/is', $part, $nlk)) {
                        $notLikeConditions[$nlk[1]] = $nlk[2];
                        continue;
                    }

                    // col LIKE 'pattern'
                    if (preg_match('/^([a-zA-Z0-9_]+)\s+LIKE\s+[\'"](.*?)[\'"]$/is', $part, $lk)) {
                        $likeConditions[$lk[1]] = $lk[2];
                        continue;
                    }

                    // col NOT IN ('a','b') or col NOT IN (1,2,3)
                    if (preg_match('/^([a-zA-Z0-9_]+)\s+NOT\s+IN\s*\((.+)\)$/i', $part, $nim)) {
                        $vals = $this->parseInValues($nim[2]);
                        if (!empty($vals)) {
                            $notInConditions[$nim[1]] = $vals;
                        }
                        continue;
                    }

                    // col IN ('a','b') or col IN (1,2,3)
                    if (preg_match('/^([a-zA-Z0-9_]+)\s+IN\s*\((.+)\)$/i', $part, $inm)) {
                        $vals = $this->parseInValues($inm[2]);
                        if (!empty($vals)) {
                            $inConditions[$inm[1]] = $vals;
                        }
                        continue;
                    }

                    // col != 'value'  or  col <> 'value'
                    if (preg_match('/^([a-zA-Z0-9_]+)\s*(?:!=|<>)\s*[\'"](.*?)[\'"]$/s', $part, $neq)) {
                        $notConditions[$neq[1]] = array_merge($notConditions[$neq[1]] ?? [], [$neq[2]]);
                        continue;
                    }

                    // col != 123  or  col <> 123
                    if (preg_match('/^([a-zA-Z0-9_]+)\s*(?:!=|<>)\s*(-?\d+(?:\.\d+)?)$/s', $part, $neq)) {
                        $notConditions[$neq[1]] = array_merge($notConditions[$neq[1]] ?? [], [$neq[2]]);
                        continue;
                    }

                    // col = 'value'
                    if (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*[\'"](.*?)[\'"]$/s', $part, $eq)) {
                        $conditions[$eq[1]] = $eq[2];
                        continue;
                    }

                    // col = 123
                    if (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*(-?\d+(?:\.\d+)?)$/s', $part, $eq)) {
                        $conditions[$eq[1]] = $eq[2];
                        continue;
                    }

                    // col [NOT] BETWEEN a AND b
                    if (preg_match('/^([a-zA-Z0-9_]+)\s+(NOT\s+)?BETWEEN\s+(\'[^\']*\'|"[^"]*"|-?\d+(?:\.\d+)?)\s+AND\s+(\'[^\']*\'|"[^"]*"|-?\d+(?:\.\d+)?)$/is', $part, $bt)) {
                        $betweenConditions[] = [
                            'col'  => $bt[1],
                            'not'  => !empty($bt[2]),
                            'low'  => trim($bt[3], "'\""),
                            'high' => trim($bt[4], "'\""),
                        ];
                        continue;
                    }

                    // col >= N, col <= N, col > N, col < N (numeric)
                    if (preg_match('/^([a-zA-Z0-9_]+)\s*(>=|<=|>|<)\s*(-?\d+(?:\.\d+)?)$/s', $part, $cmp)) {
                        $comparisons[] = ['col' => $cmp[1], 'op' => $cmp[2], 'val' => $cmp[3]];
                        continue;
                    }

                    // col >= 'value', col <= 'value', col > 'value', col < 'value' (string/date)
                    if (preg_match('/^([a-zA-Z0-9_]+)\s*(>=|<=|>|<)\s*[\'"](.*?)[\'"]$/s', $part, $cmp)) {
                        $comparisons[] = ['col' => $cmp[1], 'op' => $cmp[2], 'val' => $cmp[3]];
                        continue;
                    }
                }
            }

            return [
                'columns'           => $columns,
                'table'             => $table,
                'conditions'        => $conditions,
                'inConditions'      => $inConditions,
                'notConditions'     => $notConditions,
                'notInConditions'   => $notInConditions,
                'comparisons'       => $comparisons,
                'likeConditions'    => $likeConditions,
                'notLikeConditions' => $notLikeConditions,
                'nullConditions'    => $nullConditions,
                'notNullConditions' => $notNullConditions,
                'betweenConditions' => $betweenConditions,
                'funcConditions'    => $funcConditions,
                'orGroups'          => $orGroups,
                'joins'             => $joins,
                'joinFilters'       => $joinFilters,
                'mainAlias'         => $mainAlias,
                'selectQualified'   => $selectQualified,
                'groupBy'           => $groupBy,
                'having'            => $having,
                'aggregates'        => $aggregates,
                'distinct'          => $distinct,
                'limit'             => $limit,
                'offset'            => $offset,
                'orderBy'           => $orderBy,
                'rawWhere'          => $rawWhere,
                'calcFoundRows'     => $calcFoundRows,
            ];
        }

        /**
         * Parse the operator part of an alias-qualified condition,
         * e.g. "IN ('a','b')", "= 'x'", "LIKE '%y%'", "!= 3".
         */
        private function parseJoinFilter(string $alias, string $col, string $rest): ?array
        {
            if (preg_match('/^NOT\s+IN\s*\((.+)\)$/is', $rest, $m)) {
                return ['alias' => $alias, 'col' => $col, 'op' => 'not_in', 'vals' => $this->parseInValues($m[1])];
            }
            if (preg_match('/^IN\s*\((.+)\)$/is', $rest, $m)) {
                return ['alias' => $alias, 'col' => $col, 'op' => 'in', 'vals' => $this->parseInValues($m[1])];
            }
            if (preg_match('/^NOT\s+LIKE\s+[\'"](.*?)[\'"]$/is', $rest, $m)) {
                return ['alias' => $alias, 'col' => $col, 'op' => 'not_like', 'vals' => [$m[1]]];
            }
            if (preg_match('/^LIKE\s+[\'"](.*?)[\'"]$/is', $rest, $m)) {
                return ['alias' => $alias, 'col' => $col, 'op' => 'like', 'vals' => [$m[1]]];
            }
            if (preg_match('/^(?:!=|<>)\s*[\'"](.*?)[\'"]$/s', $rest, $m)) {
                return ['alias' => $alias, 'col' => $col, 'op' => 'ne', 'vals' => [$m[1]]];
            }
            if (preg_match('/^(?:!=|<>)\s*(-?\d+(?:\.\d+)?)$/s', $rest, $m)) {
                return ['alias' => $alias, 'col' => $col, 'op' => 'ne', 'vals' => [$m[1]]];
            }
            if (preg_match('/^=\s*[\'"](.*?)[\'"]$/s', $rest, $m)) {
                return ['alias' => $alias, 'col' => $col, 'op' => 'eq', 'vals' => [$m[1]]];
            }
            if (preg_match('/^=\s*(-?\d+(?:\.\d+)?)$/s', $rest, $m)) {
                return ['alias' => $alias, 'col' => $col, 'op' => 'eq', 'vals' => [$m[1]]];
            }
            if (preg_match('/^(>=|<=|>|<)\s*[\'"](.*?)[\'"]$/s', $rest, $m)) {
                return ['alias' => $alias, 'col' => $col, 'op' => $m[1], 'vals' => [$m[2]]];
            }
            if (preg_match('/^(>=|<=|>|<)\s*(-?\d+(?:\.\d+)?)$/s', $rest, $m)) {
                return ['alias' => $alias, 'col' => $col, 'op' => $m[1], 'vals' => [$m[2]]];
            }
            return null;
        }

        /**
         * Parse values from an IN(...) clause with a proper tokenizer:
         * quoted strings (incl. commas/escapes inside) and bare numbers,
         * preserving order.
         */
        private function parseInValues(string $raw): array
        {
            $vals  = [];
            $buf   = '';
            $inStr = false;
            $quote = '';
            $len   = strlen($raw);

            for ($i = 0; $i < $len; $i++) {
                $ch = $raw[$i];
                if ($inStr) {
                    if ($ch === '\\' && $i + 1 < $len) { $buf .= $raw[++$i]; continue; }
                    if ($ch === $quote) {
                        if (($raw[$i + 1] ?? '') === $quote) { $buf .= $quote; $i++; continue; }
                        $inStr = false;
                        continue;
                    }
                    $buf .= $ch;
                    continue;
                }
                if ($ch === "'" || $ch === '"') { $inStr = true; $quote = $ch; continue; }
                if ($ch === ',') {
                    $t = trim($buf);
                    if ($t !== '') $vals[] = $t;
                    $buf = '';
                    continue;
                }
                $buf .= $ch;
            }
            $t = trim($buf);
            if ($t !== '') $vals[] = $t;

            return $vals;
        }

        /**
         * Split a WHERE clause on AND keywords that are NOT inside parentheses.
         * e.g. "1=1 AND ((a = 'x' AND (b = 'y' OR b = 'z')))" → ["1=1", "((a = 'x' AND (b = 'y' OR b = 'z')))"]
         */
        private function splitOnTopLevelAnd(string $where): array
        {
            $parts  = [];
            $curr   = '';
            $depth  = 0;
            $len    = strlen($where);
            $i      = 0;
            // BETWEEN x AND y: the AND belongs to BETWEEN, not to the
            // condition splitter.
            $inBetween = false;

            while ($i < $len) {
                $ch = $where[$i];

                // Track paren depth
                if ($ch === '(') { $depth++; $curr .= $ch; $i++; continue; }
                if ($ch === ')') { $depth--; $curr .= $ch; $i++; continue; }

                // Skip string literals
                if ($ch === "'" || $ch === '"') {
                    $quote = $ch;
                    $curr .= $ch;
                    $i++;
                    while ($i < $len) {
                        $c = $where[$i];
                        $curr .= $c;
                        $i++;
                        if ($c === $quote) break;
                        if ($c === '\\' && $i < $len) { $curr .= $where[$i]; $i++; }
                    }
                    continue;
                }

                // Detect BETWEEN keyword at depth 0
                if ($depth === 0
                    && ($ch === 'B' || $ch === 'b')
                    && strncasecmp(substr($where, $i, 7), 'BETWEEN', 7) === 0
                    && ($i === 0 || ctype_space($where[$i - 1]))
                    && ($i + 7 >= $len || ctype_space($where[$i + 7]))
                ) {
                    $inBetween = true;
                    $curr .= substr($where, $i, 7);
                    $i += 7;
                    continue;
                }

                // At depth 0, check for \bAND\b keyword
                if ($depth === 0
                    && ($ch === 'A' || $ch === 'a')
                    && strtoupper(substr($where, $i, 3)) === 'AND'
                    && ($i === 0 || ctype_space($where[$i - 1]))
                    && ($i + 3 >= $len || ctype_space($where[$i + 3]))
                ) {
                    if ($inBetween) {
                        // Part of BETWEEN … AND …: keep in current part.
                        $inBetween = false;
                        $curr .= ' AND ';
                        $i += 3;
                        continue;
                    }
                    $trimmed = trim($curr);
                    if ($trimmed !== '') {
                        $parts[] = $trimmed;
                    }
                    $curr = '';
                    $i += 3; // skip "AND"
                    continue;
                }

                $curr .= $ch;
                $i++;
            }

            $trimmed = trim($curr);
            if ($trimmed !== '') {
                $parts[] = $trimmed;
            }

            return $parts ?: [$where];
        }

        /**
         * Strip outer layers of parentheses from a string.
         * "((foo))" → "foo", "(foo)" → "foo", "foo" → "foo"
         */
        private function stripOuterParens(string $s): string
        {
            $s = trim($s);
            while (strlen($s) >= 2 && $s[0] === '(' && $s[strlen($s) - 1] === ')') {
                // Check that the opening paren matches the closing one (not just any two)
                $depth = 0;
                $matches = true;
                for ($i = 0; $i < strlen($s) - 1; $i++) {
                    if ($s[$i] === '(') $depth++;
                    elseif ($s[$i] === ')') $depth--;
                    if ($depth === 0) { $matches = false; break; }
                }
                if (!$matches) break;
                $s = trim(substr($s, 1, -1));
            }
            return $s;
        }

        /**
         * Flatten a complex parenthesized WHERE part into individual conditions.
         * Handles patterns like: ((post_type = 'post' AND (status = 'a' OR status = 'b')))
         * Returns flat condition parts by recursively splitting on top-level AND.
         */
        private function flattenWherePart(string $part): array
        {
            $stripped = $this->stripOuterParens($part);
            // Re-split on top-level AND within this stripped part
            $subParts = $this->splitOnTopLevelAnd($stripped);
            if (count($subParts) <= 1) {
                return [$stripped];
            }
            // Recursively flatten each sub-part
            $result = [];
            foreach ($subParts as $sp) {
                foreach ($this->flattenWherePart($sp) as $flat) {
                    $result[] = $flat;
                }
            }
            return $result;
        }

        /**
         * Query the sharded storage, applying filters and projections.
         */
        private function queryShardedStorage(array $parsed): array
        {
            $table = $parsed['table'];

            // Read rows from sharded storage (chunks + WAL merge)
            $pkCol = $this->storage ? $this->storage->resolvePkColumn($table) : null;
            $allRows = $this->storage
                ? $this->storage->readRows($table, $parsed['conditions'], $parsed['inConditions'], $pkCol)
                : [];

            // Pre-build join indexes: alias => [join-key value => joined rows]
            $joinIndex = [];
            $mainAlias = $parsed['mainAlias'] ?? $table;
            if (!empty($parsed['joins']) && $this->storage) {
                foreach ($parsed['joins'] as $alias => $spec) {
                    $rows = $this->storage->readRows($spec['table']);
                    $idx  = [];
                    foreach ($rows as $r) {
                        $idx[(string)($r['data'][$spec['thisCol']] ?? '')][] = $r['data'];
                    }
                    $joinIndex[$alias] = ['idx' => $idx, 'spec' => $spec];
                }
            }

            // Fan-out over the join graph: SQL semantics produce one output
            // row per matching joined row. Filters on joined columns are
            // applied to candidate rows during expansion, not as EXISTS.
            $partials = [];
            foreach ($allRows as $row) { $partials[] = $row['data']; }
            foreach ($parsed['joins'] as $alias => $spec) {
                $next = [];
                foreach ($partials as $prow) {
                    $key   = (string)($prow[$spec['parentCol']] ?? '');
                    $cands = $joinIndex[$alias]['idx'][$key] ?? [];
                    $matched = [];
                    foreach ($cands as $c) {
                        $ok = true;
                        foreach ($parsed['joinFilters'] as $jf) {
                            if ($jf['alias'] === $alias && !$this->joinFilterMatch($c, $jf)) { $ok = false; break; }
                        }
                        if ($ok) $matched[] = $c;
                    }
                    if (empty($matched)) {
                        // A LEFT JOIN keeps the row NULL-extended, but a WHERE
                        // filter on that alias then rejects NULL - matching MySQL,
                        // which effectively turns such a LEFT JOIN into INNER.
                        $filteredAlias = false;
                        foreach ($parsed['joinFilters'] as $jf) {
                            if ($jf['alias'] === $alias) { $filteredAlias = true; break; }
                        }
                        if ($spec['left'] && !$filteredAlias) { $next[] = $prow; }
                        continue;
                    }
                    foreach ($matched as $m) { $next[] = array_merge($prow, $m); }
                }
                $partials = $next;
            }

            // Apply conditions filter
            $filtered = [];
            foreach ($partials as $data) {
                $match = true;

                // Equality conditions
                foreach ($parsed['conditions'] as $col => $expected) {
                    if (($data[$col] ?? null) !== $expected
                        && (string)($data[$col] ?? '') !== (string)$expected) {
                        $match = false;
                        break;
                    }
                }

                // IN conditions
                if ($match) {
                    foreach ($parsed['inConditions'] as $col => $allowedValues) {
                        if (!in_array($data[$col] ?? '', $allowedValues, true)
                            && !in_array((string)($data[$col] ?? ''), $allowedValues, true)) {
                            $match = false;
                            break;
                        }
                    }
                }

                // NOT-equal conditions (col != 'value')
                if ($match) {
                    foreach ($parsed['notConditions'] as $col => $rejectedValues) {
                        $val = (string)($data[$col] ?? '');
                        foreach ($rejectedValues as $rejected) {
                            if ($val === (string)$rejected) {
                                $match = false;
                                break 2;
                            }
                        }
                    }
                }

                // NOT IN conditions
                if ($match) {
                    foreach ($parsed['notInConditions'] as $col => $rejectedValues) {
                        $val = (string)($data[$col] ?? '');
                        foreach ($rejectedValues as $rejected) {
                            if ($val === (string)$rejected) {
                                $match = false;
                                break 2;
                            }
                        }
                    }
                }

                // Comparison conditions (>, >=, <, <=)
                if ($match) {
                    foreach ($parsed['comparisons'] as $cmp) {
                        $val = $data[$cmp['col']] ?? '';
                        $cmpVal = $cmp['val'];
                        // Numeric comparison if both are numeric
                        if (is_numeric($val) && is_numeric($cmpVal)) {
                            $v = (float)$val;
                            $c = (float)$cmpVal;
                        } else {
                            // String/date comparison
                            $v = (string)$val;
                            $c = (string)$cmpVal;
                        }
                        $pass = match ($cmp['op']) {
                            '>'  => $v > $c,
                            '>=' => $v >= $c,
                            '<'  => $v < $c,
                            '<=' => $v <= $c,
                            default => true,
                        };
                        if (!$pass) {
                            $match = false;
                            break;
                        }
                    }
                }

                // Date function conditions: YEAR(post_date) = 2025 etc
                if ($match) {
                    foreach ($parsed['funcConditions'] ?? [] as $fc) {
                        $raw = (string)($data[$fc['col']] ?? '');
                        $ts = $raw === '' ? false : strtotime($raw);
                        if ($ts === false) {
                            $extracted = 0;
                        } elseif ($fc['fn'] === 'QUARTER') {
                            $extracted = (int) ceil(((int) date('n', $ts)) / 3);
                        } else {
                            $extracted = (int) date([
                                'YEAR' => 'Y', 'MONTH' => 'n', 'DAY' => 'j',
                                'HOUR' => 'G', 'MINUTE' => 'i', 'SECOND' => 's',
                                'WEEK' => 'W',
                            ][$fc['fn']], $ts);
                        }
                        $ok = match ($fc['op']) {
                            '='  => $extracted === $fc['val'],
                            '!=', '<>' => $extracted !== $fc['val'],
                            '>'  => $extracted > $fc['val'],
                            '>=' => $extracted >= $fc['val'],
                            '<'  => $extracted < $fc['val'],
                            '<=' => $extracted <= $fc['val'],
                            default => true,
                        };
                        if (!$ok) { $match = false; break; }
                    }
                }

                // BETWEEN / NOT BETWEEN conditions
                if ($match) {
                    foreach ($parsed['betweenConditions'] ?? [] as $bt) {
                        $val = $data[$bt['col']] ?? '';
                        if (is_numeric($val) && is_numeric($bt['low']) && is_numeric($bt['high'])) {
                            $v = (float) $val;
                            $ok = $v >= (float) $bt['low'] && $v <= (float) $bt['high'];
                        } else {
                            $v = (string) $val;
                            $ok = $v >= (string) $bt['low'] && $v <= (string) $bt['high'];
                        }
                        if ($bt['not']) $ok = !$ok;
                        if (!$ok) {
                            $match = false;
                            break;
                        }
                    }
                }

                // LIKE conditions (SQL % → regex .*, _ → regex ., \ escapes honored)
                if ($match) {
                    foreach ($parsed['likeConditions'] as $col => $pattern) {
                        $val = (string)($data[$col] ?? '');
                        if (!preg_match('/^' . $this->likeToRegex($pattern) . '$/is', $val)) {
                            $match = false;
                            break;
                        }
                    }
                }

                // NOT LIKE conditions
                if ($match) {
                    foreach ($parsed['notLikeConditions'] as $col => $pattern) {
                        $val = (string)($data[$col] ?? '');
                        if (preg_match('/^' . $this->likeToRegex($pattern) . '$/is', $val)) {
                            $match = false;
                            break;
                        }
                    }
                }

                // IS NULL conditions
                if ($match) {
                    foreach ($parsed['nullConditions'] as $col) {
                        if (isset($data[$col]) && $data[$col] !== '' && $data[$col] !== null) {
                            $match = false;
                            break;
                        }
                    }
                }

                // IS NOT NULL conditions
                if ($match) {
                    foreach ($parsed['notNullConditions'] as $col) {
                        if (!isset($data[$col]) || $data[$col] === '' || $data[$col] === null) {
                            $match = false;
                            break;
                        }
                    }
                }

                // OR groups: mixed-column disjunctions, at least one must pass
                if ($match && !empty($parsed['orGroups'])) {
                    foreach ($parsed['orGroups'] as $group) {
                        $any = false;
                        foreach ($group as $cond) {
                            if ($cond[0] === 'eq') {
                                if ((string)($data[$cond[1]] ?? '') === (string)$cond[2]) { $any = true; break; }
                            } elseif ($cond[0] === 'like') {
                                $val = (string)($data[$cond[1]] ?? '');
                                if (preg_match('/^' . $this->likeToRegex($cond[2]) . '$/is', $val)) { $any = true; break; }
                            } elseif ($cond[0] === 'not_like') {
                                $val = (string)($data[$cond[1]] ?? '');
                                if (!preg_match('/^' . $this->likeToRegex($cond[2]) . '$/is', $val)) { $any = true; break; }
                            } elseif ($cond[0] === 'cmp') {
                                $val = $data[$cond[1]] ?? '';
                                $cv  = $cond[3];
                                if (is_numeric($val) && is_numeric($cv)) { $v = (float)$val; $c = (float)$cv; }
                                else { $v = (string)$val; $c = (string)$cv; }
                                $ok = match ($cond[2]) {
                                    '>'  => $v > $c,
                                    '>=' => $v >= $c,
                                    '<'  => $v < $c,
                                    '<=' => $v <= $c,
                                    default => false,
                                };
                                if ($ok) { $any = true; break; }
                            }
                        }
                        if (!$any) { $match = false; break; }
                    }
                }

                if ($match) {
                    $filtered[] = $data;
                }
            }

            // GROUP BY: bucket rows, compute aggregates per bucket, HAVING
            if (!empty($parsed['groupBy'])) {
                $groups = [];
                foreach ($filtered as $data) {
                    $keyParts = [];
                    foreach ($parsed['groupBy'] as $gc) { $keyParts[] = (string)($data[$gc] ?? ''); }
                    $gk = implode("\x1F", $keyParts);
                    if (!isset($groups[$gk])) { $groups[$gk] = ['first' => $data, 'rows' => []]; }
                    $groups[$gk]['rows'][] = $data;
                }
                $grouped = [];
                foreach ($groups as $g) {
                    // MySQL (without ONLY_FULL_GROUP_BY) returns every selected
                    // column from an arbitrary row of the group; seed with the
                    // first row so SELECT * keeps all columns, then layer the
                    // group keys and aggregates on top.
                    $row = $g['first'];
                    foreach ($parsed['groupBy'] as $gc) { $row[$gc] = $g['first'][$gc] ?? ''; }
                    foreach ($parsed['aggregates'] as $ag) {
                        $row[$ag['alias']] = $this->computeAggregate($ag, $g['rows']);
                    }
                    $keep = true;
                    foreach ($parsed['having'] as $h) {
                        if (!array_key_exists($h['col'], $row)) { $keep = false; break; }
                        $v = $row[$h['col']];
                        $c = $h['val'];
                        if (is_numeric($v) && is_numeric($c)) { $vv = (float)$v; $cc = (float)$c; }
                        else { $vv = (string)$v; $cc = (string)$c; }
                        $pass = match ($h['op']) {
                            '>'  => $vv > $cc,
                            '>=' => $vv >= $cc,
                            '<'  => $vv < $cc,
                            '<=' => $vv <= $cc,
                            '='  => $vv == $cc,
                            '!=' => $vv != $cc,
                            '<>' => $vv != $cc,
                            default => true,
                        };
                        if (!$pass) { $keep = false; break; }
                    }
                    if ($keep) $grouped[] = $row;
                }
                $filtered = $grouped;
            }

            // ORDER BY (multi-column)
            if (!empty($parsed['orderBy']) && !empty($filtered)) {
                $orderSpec = $parsed['orderBy'];
                usort($filtered, function ($a, $b) use ($orderSpec) {
                    foreach ($orderSpec as $term) {
                        $col = $term['col'];
                        $av  = $a[$col] ?? '';
                        $bv  = $b[$col] ?? '';
                        if (is_numeric($av) && is_numeric($bv)) {
                            $cmp = (float)$av <=> (float)$bv;
                        } else {
                            $cmp = strcmp((string)$av, (string)$bv);
                        }
                        if ($cmp !== 0) {
                            return $term['dir'] === 'DESC' ? -$cmp : $cmp;
                        }
                    }
                    return 0;
                });
            }

            // Save total count before LIMIT for SQL_CALC_FOUND_ROWS
            if ($parsed['calcFoundRows'] ?? false) {
                $this->calcFoundRows = count($filtered);
            }

            // Aggregates without GROUP BY: collapse to a single row
            if (!empty($parsed['aggregates']) && empty($parsed['groupBy'])) {
                $aggRow = [];
                foreach ($parsed['aggregates'] as $ag) {
                    $aggRow[$ag['alias']] = $this->computeAggregate($ag, $filtered);
                }
                return [(object) $aggRow];
            }

            // DISTINCT over projected columns (grouped rows are unique already)
            if (!empty($parsed['distinct']) && empty($parsed['groupBy'])) {
                $star     = in_array('*', $parsed['columns'], true);
                $seenKeys = [];
                $unique   = [];
                foreach ($filtered as $data) {
                    $parts = [];
                    foreach ($data as $col => $val) {
                        if ($star || in_array($col, $parsed['columns'], true)) {
                            $parts[] = (string) $val;
                        }
                    }
                    $key = implode("\x1F", $parts);
                    if (isset($seenKeys[$key])) continue;
                    $seenKeys[$key] = true;
                    $unique[] = $data;
                }
                $filtered = $unique;
            }

            // OFFSET + LIMIT
            if ($parsed['offset'] > 0 || $parsed['limit'] !== null) {
                $offset = $parsed['offset'];
                $length = $parsed['limit'] ?? count($filtered);
                $filtered = array_slice($filtered, $offset, $length);
            }

            // Project columns. Requested-but-absent columns are filled with ''
            // so callers never hit "Undefined property" on partial rows.
            // For SELECT * the persisted schema is authoritative: MySQL
            // always returns every column (defaults for rows predating an
            // ALTER TABLE ADD COLUMN), so fill schema gaps the same way.
            $star = in_array('*', $parsed['columns'], true);
            $schemaCols = [];
            $schemaDefaults = [];
            if ($star && $this->storage !== null) {
                $schema = $this->storage->readSchema($parsed['table']);
                if ($schema !== null && !empty($schema['columns'])) {
                    $schemaCols     = array_keys($schema['columns']);
                    $schemaDefaults = $schema['defaults'] ?? [];
                }
            }
            $results = [];
            foreach ($filtered as $data) {
                $projected = [];
                foreach ($data as $col => $val) {
                    if ($star || in_array($col, $parsed['columns'], true)) {
                        $projected[$col] = $val;
                    }
                }
                if ($star) {
                    foreach ($schemaCols as $col) {
                        if (!array_key_exists($col, $projected)) {
                            $projected[$col] = (string) ($schemaDefaults[$col] ?? '');
                        }
                    }
                } else {
                    foreach ($parsed['columns'] as $col) {
                        if (!array_key_exists($col, $projected) && $col !== '') {
                            $projected[$col] = '';
                        }
                    }
                }
                $results[] = (object) $projected;
            }

            return $results;
        }

        /**
         * Walk the join graph from the main row down to the given alias,
         * returning every reachable joined row.
         */
        private function resolveJoinRows(string $alias, array $mainRow, string $mainAlias, array $joinIndex): array
        {
            if ($alias === $mainAlias || !isset($joinIndex[$alias])) {
                return [$mainRow];
            }
            $spec       = $joinIndex[$alias]['spec'];
            $parentRows = $this->resolveJoinRows($spec['parent'], $mainRow, $mainAlias, $joinIndex);
            $out = [];
            foreach ($parentRows as $pr) {
                $key = (string)($pr[$spec['parentCol']] ?? '');
                foreach ($joinIndex[$alias]['idx'][$key] ?? [] as $crow) {
                    $out[] = $crow;
                }
            }
            return $out;
        }

        /**
         * Evaluate one operator condition against a joined row.
         */
        private function joinFilterMatch(array $row, array $jf): bool
        {
            $val = (string)($row[$jf['col']] ?? '');
            switch ($jf['op']) {
                case 'eq':      return $val === (string)$jf['vals'][0];
                case 'ne':      return $val !== (string)$jf['vals'][0];
                case 'in':      return in_array($val, array_map('strval', $jf['vals']), true);
                case 'not_in':  return !in_array($val, array_map('strval', $jf['vals']), true);
                case 'like':    return (bool)preg_match('/^' . $this->likeToRegex($jf['vals'][0]) . '$/is', $val);
                case 'not_like': return !preg_match('/^' . $this->likeToRegex($jf['vals'][0]) . '$/is', $val);
                case '>': case '>=': case '<': case '<=':
                    $c = $jf['vals'][0];
                    if (is_numeric($val) && is_numeric($c)) { $v = (float)$val; $cc = (float)$c; }
                    else { $v = $val; $cc = (string)$c; }
                    return (bool)match ($jf['op']) {
                        '>'  => $v > $cc,
                        '>=' => $v >= $cc,
                        '<'  => $v < $cc,
                        '<=' => $v <= $cc,
                    };
            }
            return true;
        }

        /**
         * Compile a SQL LIKE pattern to a regex, honoring \% \_ \\ escapes.
         */
        private function likeToRegex(string $pattern): string
        {
            $escaped = '';
            $len = strlen($pattern);
            for ($i = 0; $i < $len; $i++) {
                $ch = $pattern[$i];
                if ($ch === '\\' && $i + 1 < $len
                    && in_array($pattern[$i + 1], ['%', '_', '\\'], true)) {
                    $escaped .= preg_quote($pattern[$i + 1], '/');
                    $i++;
                } elseif ($ch === '%') {
                    $escaped .= '.*';
                } elseif ($ch === '_') {
                    $escaped .= '.';
                } else {
                    $escaped .= preg_quote($ch, '/');
                }
            }
            return $escaped;
        }

        /**
         * Compute one aggregate over the filtered row set.
         */
        private function computeAggregate(array $ag, array $rows): string
        {
            $fn = $ag['fn'];
            if ($fn === 'COUNT') {
                if ($ag['col'] === '*') {
                    return (string) count($rows);
                }
                $n = 0;
                $seen = [];
                foreach ($rows as $r) {
                    $v = $r[$ag['col']] ?? null;
                    if ($v === null || $v === '') continue;
                    if ($ag['distinct']) {
                        $k = (string)$v;
                        if (isset($seen[$k])) continue;
                        $seen[$k] = true;
                    }
                    $n++;
                }
                return (string) $n;
            }

            $vals = [];
            foreach ($rows as $r) {
                $v = $r[$ag['col']] ?? null;
                if ($v === null || $v === '') continue;
                $vals[] = $v;
            }
            if (empty($vals)) return '';

            $allNumeric = true;
            foreach ($vals as $v) {
                if (!is_numeric($v)) { $allNumeric = false; break; }
            }

            return match ($fn) {
                'MAX'   => $allNumeric
                            ? (string) max(array_map('floatval', $vals))
                            : max($vals),
                'MIN'   => $allNumeric
                            ? (string) min(array_map('floatval', $vals))
                            : min($vals),
                'SUM'   => (string) array_sum(array_map('floatval', $vals)),
                'AVG'   => (string) (array_sum(array_map('floatval', $vals)) / count($vals)),
                default => '',
            };
        }

    }

    /**
     * Minimal parser for UPDATE and DELETE statements.
     */
    final class MutationParser
    {
        public function parseUpdate(string $sql): ?array
        {
            $sql = str_replace('`', '', $sql);
            if (!preg_match('/UPDATE\s+([a-zA-Z0-9_]+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*?))?$/is', $sql, $m)) {
                return null;
            }
            $table       = $m[1];
            $setClause   = $m[2];
            $whereClause = $m[3] ?? '';

            $setValues  = $this->parseAssignments($setClause);
            $conditions = $this->parseWhereEquality($whereClause);

            if (trim($whereClause) !== '' && empty($conditions)) {
                return null;
            }
            return ['table' => $table, 'set' => $setValues, 'conditions' => $conditions];
        }

        public function parseDelete(string $sql): ?array
        {
            $sql = str_replace('`', '', $sql);
            if (!preg_match('/DELETE\s+FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+(.*?))?$/is', $sql, $m)) {
                return null;
            }
            $table       = $m[1];
            $whereClause = $m[2] ?? '';
            $conditions  = $this->parseWhereEquality($whereClause);

            if (trim($whereClause) !== '' && empty($conditions)) {
                return null;
            }
            return ['table' => $table, 'conditions' => $conditions];
        }

        private function parseAssignments(string $clause): array
        {
            $pairs = [];
            $buf   = '';
            $inStr = false;
            $esc   = false;
            $len   = strlen($clause);

            for ($i = 0; $i < $len; $i++) {
                $ch = $clause[$i];
                if ($esc) { $buf .= $ch; $esc = false; continue; }
                if ($ch === '\\') { $esc = true; $buf .= $ch; continue; }
                if ($ch === "'") { $inStr = !$inStr; $buf .= $ch; continue; }
                if ($ch === ',' && !$inStr) { $this->addAssignment($buf, $pairs); $buf = ''; continue; }
                $buf .= $ch;
            }
            $this->addAssignment($buf, $pairs);
            return $pairs;
        }

        private function addAssignment(string $expr, array &$pairs): void
        {
            $expr = trim($expr);
            if ($expr === '') return;

            // col = 'value'
            if (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*[\'"](.*)[\'"]$/s', $expr, $m)) {
                $pairs[$m[1]] = stripslashes($m[2]);
                return;
            }

            $rhs = null;
            if (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*(.+)$/s', $expr, $m)) {
                $col = $m[1];
                $rhs = trim($m[2]);
            }
            if ($rhs === null) return;

            // NULL literal
            if (strcasecmp($rhs, 'NULL') === 0) { $pairs[$col] = ''; return; }

            // CASE WHEN col='v' THEN r WHEN ... [ELSE e] END - resolved per
            // row at write time (wp_update_term_count_now / comment counts).
            if (preg_match('/^CASE\b(.*)\bEND$/is', $rhs, $cm)) {
                $cases = [];
                $else  = null;
                $body  = $cm[1];
                // Split leading ELSE off the tail
                if (preg_match('/\bELSE\b(.*)$/is', $body, $em)) {
                    $else = trim($em[1]);
                    $body = trim(substr($body, 0, strlen($body) - strlen($em[0])));
                }
                if (preg_match_all('/WHEN\s+([a-zA-Z0-9_]+)\s*=\s*(\'[^\']*\'|"[^"]*"|-?\d+(?:\.\d+)?)\s+THEN\s+(\'[^\']*\'|"[^"]*"|-?\d+(?:\.\d+)?)/is', $body, $wm, PREG_SET_ORDER)) {
                    foreach ($wm as $w) {
                        $cases[] = [
                            'col'  => $w[1],
                            'val'  => trim($w[2], "'\""),
                            'then' => trim($w[3], "'\""),
                        ];
                    }
                }
                if (!empty($cases)) {
                    $pairs[$col] = ['__case__' => ['cases' => $cases, 'else' => $else !== null ? trim($else, "'\"") : null]];
                    return;
                }
            }

            // Date/time functions evaluated at write time
            if (preg_match('/^(NOW|CURRENT_TIMESTAMP|CURRENT_TIMESTAMP\(\)|CURRENT_DATE|CURDATE)\b\(*$/i', $rhs)) {
                $pairs[$col] = gmdate('Y-m-d H:i:s');
                return;
            }
            if (preg_match('/^UNIX_TIMESTAMP\(\s*\)$/i', $rhs)) {
                $pairs[$col] = (string) time();
                return;
            }

            // Arithmetic on the existing column: col = col + N | col - N
            if (preg_match('/^([a-zA-Z0-9_]+)\s*([+\-])\s*(\d+(?:\.\d+)?)$/', $rhs, $am)
                && $am[1] === $col) {
                $delta = (float) $am[3] * ($am[2] === '-' ? -1 : 1);
                $pairs[$col] = ['__delta__' => $delta];
                return;
            }

            // Bare number
            if (is_numeric($rhs)) { $pairs[$col] = $rhs; return; }

            // Anything else: store as-is (best effort)
            $pairs[$col] = stripslashes($rhs);
        }

        /**
         * Split a WHERE clause on top-level AND, keeping "BETWEEN x AND y"
         * intact (its AND belongs to the operator, not the splitter).
         *
         * @return string[]
         */
        private function splitAndRespectingBetween(string $clause): array
        {
            $parts = [];
            $curr  = '';
            $len   = strlen($clause);
            $i     = 0;
            $inBetween = false;

            while ($i < $len) {
                $ch = $clause[$i];

                if ($ch === "'" || $ch === '"') {
                    $quote = $ch;
                    $curr .= $ch;
                    $i++;
                    while ($i < $len) {
                        $c = $clause[$i];
                        $curr .= $c;
                        $i++;
                        if ($c === $quote) break;
                        if ($c === '\\' && $i < $len) { $curr .= $clause[$i]; $i++; }
                    }
                    continue;
                }

                if (($ch === 'B' || $ch === 'b')
                    && strncasecmp(substr($clause, $i, 7), 'BETWEEN', 7) === 0
                    && ($i === 0 || ctype_space($clause[$i - 1]))
                    && ($i + 7 >= $len || ctype_space($clause[$i + 7]))
                ) {
                    $inBetween = true;
                    $curr .= substr($clause, $i, 7);
                    $i += 7;
                    continue;
                }

                if (($ch === 'A' || $ch === 'a')
                    && strncasecmp(substr($clause, $i, 3), 'AND', 3) === 0
                    && ($i === 0 || ctype_space($clause[$i - 1]))
                    && ($i + 3 >= $len || ctype_space($clause[$i + 3]))
                ) {
                    if ($inBetween) {
                        $inBetween = false;
                        $curr .= ' AND ';
                        $i += 3;
                        continue;
                    }
                    if (trim($curr) !== '') $parts[] = trim($curr);
                    $curr = '';
                    $i += 3;
                    continue;
                }

                $curr .= $ch;
                $i++;
            }

            if (trim($curr) !== '') $parts[] = trim($curr);
            return $parts ?: [$clause];
        }

        private function parseWhereEquality(string $clause): array
        {
            $conditions = [];
            if (trim($clause) === '') return $conditions;

            $clause = stripTablePrefixes($clause);

            $parts = $this->splitAndRespectingBetween($clause);
            foreach ($parts as $part) {
                $part = trim($part);
                if (preg_match('/^\d+\s*=\s*\d+$/', $part)) continue;

                // col IN ('a','b') / col IN (1,2)
                if (preg_match('/^([a-zA-Z0-9_]+)\s+IN\s*\((.+)\)$/i', $part, $m)) {
                    $vals = $this->parseInList($m[2]);
                    if (!empty($vals)) {
                        $conditions[$m[1]] = ['__in__' => $vals];
                    }
                    continue;
                }

                // col [NOT] BETWEEN a AND b
                if (preg_match('/^([a-zA-Z0-9_]+)\s+(NOT\s+)?BETWEEN\s+(\'[^\']*\'|"[^"]*"|-?\d+(?:\.\d+)?)\s+AND\s+(\'[^\']*\'|"[^"]*"|-?\d+(?:\.\d+)?)$/is', $part, $m)) {
                    $conditions[$m[1]] = ['__between__' => [
                        'not'  => !empty($m[2]),
                        'low'  => trim($m[3], "'\""),
                        'high' => trim($m[4], "'\""),
                    ]];
                    continue;
                }

                // col > / >= / < / <= value (numeric or quoted)
                if (preg_match('/^([a-zA-Z0-9_]+)\s*(>=|<=|>|<)\s*(\'[^\']*\'|"[^"]*"|-?\d+(?:\.\d+)?)$/s', $part, $m)) {
                    $conditions[$m[1]] = ['__cmp__' => [trim($m[3], "'\""), $m[2]]];
                    continue;
                }

                if (preg_match('/([a-zA-Z0-9_]+)\s*=\s*[\'"](.*?)[\'"]/', $part, $m)) {
                    $conditions[$m[1]] = $m[2];
                    continue;
                }
                if (preg_match('/([a-zA-Z0-9_]+)\s*=\s*(-?\d+(?:\.\d+)?)/', $part, $m)) {
                    $conditions[$m[1]] = $m[2];
                }
            }
            return $conditions;
        }

        /** Tokenize an IN(...) value list: quoted strings and bare numbers. */
        private function parseInList(string $raw): array
        {
            $vals  = [];
            $buf   = '';
            $inStr = false;
            $quote = '';
            $len   = strlen($raw);
            for ($i = 0; $i < $len; $i++) {
                $ch = $raw[$i];
                if ($inStr) {
                    if ($ch === '\\' && $i + 1 < $len) { $buf .= $raw[++$i]; continue; }
                    if ($ch === $quote) { $inStr = false; continue; }
                    $buf .= $ch;
                    continue;
                }
                if ($ch === "'" || $ch === '"') { $inStr = true; $quote = $ch; continue; }
                if ($ch === ',') {
                    $t = trim($buf);
                    if ($t !== '') $vals[] = $t;
                    $buf = '';
                    continue;
                }
                $buf .= $ch;
            }
            $t = trim($buf);
            if ($t !== '') $vals[] = $t;
            return $vals;
        }
    }
}

// ---------------------------------------------------------------------------
// Global namespace - HtmlDatabase_WPDB adapter (extends wpdb)
// ---------------------------------------------------------------------------
namespace {

    use HtmlDatabase\Core\Configuration;
    use HtmlDatabase\Core\ShardedStorageManager;
    use HtmlDatabase\Parser\InsertTokenizer;
    use HtmlDatabase\Parser\MutationParser;
    use HtmlDatabase\Parser\SqlToXpathTranslator;

    // The drop-in is loaded by wp-db.php before class wpdb itself is defined.
    if (!class_exists('wpdb', false)) {
        require_once ABSPATH . WPINC . '/class-wpdb.php';
    }

    class HtmlDatabase_WPDB extends wpdb
    {
        private ShardedStorageManager $storage;
        private SqlToXpathTranslator  $translator;
        private InsertTokenizer       $insertTokenizer;
        private MutationParser        $mutationParser;

        public function __construct(
            mixed $dbuser,
            mixed $dbpassword,
            mixed $dbname,
            mixed $dbhost
        ) {
            $this->show_errors();

            $storagePath = defined('HTMLDB_BASE_PATH')
                ? HTMLDB_BASE_PATH
                : WP_CONTENT_DIR . '/html_db';
            $chunkSize = defined('HTMLDB_CHUNK_SIZE')
                ? max(10, (int) HTMLDB_CHUNK_SIZE)
                : 500;
            $compactThreshold = defined('HTMLDB_COMPACT_THRESHOLD')
                ? max(10, (int) HTMLDB_COMPACT_THRESHOLD)
                : 200;
            $config      = new Configuration($storagePath, $chunkSize, $compactThreshold);

            $this->storage         = new ShardedStorageManager($config);
            $this->translator      = new SqlToXpathTranslator($storagePath, $this->storage);
            $this->insertTokenizer = new InsertTokenizer();
            $this->mutationParser  = new MutationParser();

            $this->dbuser     = $dbuser;
            $this->dbpassword = $dbpassword;
            $this->dbname     = $dbname;
            $this->dbhost     = $dbhost;

            $this->is_mysql       = true;
            $this->has_connected  = true;
            $this->ready          = true;

            if (empty($this->prefix)) {
                $this->set_prefix($GLOBALS['table_prefix'] ?? 'wp_');
            }

            $this->charset = 'utf8mb4';
            $this->collate = 'utf8mb4_unicode_ci';
        }

        // -- Connection stubs -------------------------------------------------

        public function db_connect($allow_bail = true)
        {
            $this->has_connected = true;
            return true;
        }

        public function check_connection($allow_bail = true) { return true; }
        public function db_version() { return '8.0.32'; }
        public function db_server_info() { return '8.0.32-HtmlDB'; }

        public function _real_escape($data) { return addslashes((string) $data); }

        public function determine_charset($charset, $collate) { return compact('charset', 'collate'); }
        public function set_charset($dbh, $charset = null, $collate = null) { return true; }
        public function set_sql_mode($modes = array()) { return; }
        public function select($db, $dbh = null) { $this->ready = true; return; }

        public function has_cap($db_cap)
        {
            // Only advertise what is genuinely implemented. group_concat and
            // subqueries are NOT supported; WP falls back to slower but
            // correct PHP-side paths when these report false.
            $supported = ['collation', 'set_charset', 'utf8mb4'];
            return is_string($db_cap) && in_array(strtolower($db_cap), $supported, true);
        }

        public function get_col_charset($table, $column) { return 'utf8mb4'; }
        public function get_col_length($table, $column) { return ['type' => 'byte', 'length' => 16777216]; }

        // -- Query router -----------------------------------------------------

        public function query($query)
        {
            if (!$this->ready) return false;

            $this->flush();

            // Contract with wpdb: every statement passes through the 'query'
            // filter (query monitors, debug bars, slow-query loggers).
            $query = apply_filters('query', $query);

            // WordPress prepare() replaces literal % with a unique hash.
            $query = $this->remove_placeholder_escape($query);

            $this->last_query = $query;
            $this->num_queries++;

            $clean = trim($query);
            $verb  = strtoupper(strtok($clean, " \t\n\r"));

            try {
                return match ($verb) {
                    'SELECT' => $this->handleSelect($clean),
                    'INSERT' => $this->handleInsert($clean),
                    'UPDATE' => $this->handleUpdate($clean),
                    'DELETE' => $this->handleDelete($clean),
                    'REPLACE' => $this->handleReplace($clean),
                    'CREATE', 'ALTER' => $this->handleDdl($clean),
                    'DROP', 'TRUNCATE' => $this->handleDropTruncate($clean),
                    'SET', 'START', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE' => true,
                    'SHOW'   => $this->handleShow($clean),
                    'DESCRIBE', 'DESC' => $this->handleDescribe($clean),
                    default => (function () use ($clean, $verb) {
                        // Unknown verb: keep returning true for core
                        // compatibility (WP sends exotic SET/SHOW variants),
                        // but leave a breadcrumb for debugging plugin SQL.
                        error_log('[HtmlDB] UNHANDLED VERB: ' . $verb . ' | SQL: ' . substr($clean, 0, 300));
                        return true;
                    })(),
                };
            } catch (\Throwable $e) {
                $this->last_error = $e->getMessage();
                error_log('[HtmlDB] QUERY EXCEPTION: ' . $e->getMessage() . ' | SQL: ' . substr($clean, 0, 300));
                return false;
            }
        }

        // -- SELECT -----------------------------------------------------------

        private function handleSelect(string $sql): int|false
        {
            // SELECT @@SESSION.sql_mode
            if (preg_match('/SELECT\s+@@/i', $sql)) {
                $this->last_result = [(object) ['@@SESSION.sql_mode' => '']];
                $this->num_rows = 1;
                return 1;
            }

            // SELECT FOUND_ROWS()
            if (preg_match('/SELECT\s+FOUND_ROWS\s*\(\s*\)/i', $sql)) {
                $total = $this->translator->calcFoundRows;
                $this->last_result = [(object) ['FOUND_ROWS()' => $total]];
                $this->num_rows = 1;
                return 1;
            }

            // SELECT VERSION()
            if (preg_match('/SELECT\s+VERSION\s*\(\s*\)/i', $sql)) {
                $this->last_result = [(object) ['VERSION()' => $this->db_server_info()]];
                $this->num_rows = 1;
                return 1;
            }

            // SELECT <literal> (no FROM): e.g. health checks "SELECT 1"
            if (!preg_match('/\bFROM\b/i', $sql)
                && preg_match('/^SELECT\s+(-?\d+(?:\.\d+)?|\'[^\']*\'|"[^"]*")\s*;?\s*$/is', $sql, $lm)) {
                $val = trim($lm[1], "'\"");
                $this->last_result = [(object) [$lm[1] => $val]];
                $this->num_rows = 1;
                return 1;
            }

            // YEAR() / MONTH() / DAY() in the SELECT list only (archive
            // widgets: SELECT DISTINCT YEAR(col) AS y ... FROM ...). When the
            // function appears solely in WHERE, the translator evaluates it
            // via funcConditions instead.
            $selectList = preg_match('/\bSELECT\b(.*?)\bFROM\b/is', $sql, $slm) ? $slm[1] : '';
            if ($selectList !== '' && preg_match('/\b(YEAR|MONTH|DAY)\s*\(/i', $selectList)) {
                return $this->handleDateFunctions($sql);
            }

            // GROUP BY + COUNT(*) - handled natively by the translator now
            // (fan-out joins, HAVING, multi-aggregates).

            // UNION
            if (preg_match('/\bUNION\b/i', $sql)) {
                return $this->handleUnionSelect($sql);
            }

            $results = $this->translator->executeSelect($sql);
            $this->last_result = $results;
            $this->num_rows    = count($results);
            return $this->num_rows;
        }

        /**
         * Handle SELECT with YEAR() / MONTH() / DAY() functions.
         * e.g. SELECT DISTINCT YEAR(post_date) AS year, MONTH(post_date) AS month FROM wp_posts WHERE ...
         */
        private function handleDateFunctions(string $sql): int|false
        {
            // Extract function calls: YEAR(col) AS alias, MONTH(col) AS alias
            preg_match_all('/\b(YEAR|MONTH|DAY)\s*\(\s*([a-zA-Z0-9_.]+)\s*\)\s+AS\s+([a-zA-Z0-9_]+)/i', $sql, $fm, PREG_SET_ORDER);
            if (empty($fm)) {
                // Fallback: no aliased functions found, return empty
                $this->last_result = [];
                $this->num_rows = 0;
                return 0;
            }

            // Strip date functions and DISTINCT, replace with SELECT * to get all rows.
            // LIMIT is lifted off the inner query and re-applied after the
            // DISTINCT collapse, matching MySQL semantics.
            $stripped = preg_replace('/SELECT\s+(DISTINCT\s+)?.*?\s+FROM/is', 'SELECT * FROM', $sql);
            $limit = null;
            $offset = 0;
            if (preg_match('/LIMIT\s+(\d+)\s*,\s*(\d+)/i', $stripped, $x)) {
                $offset = (int)$x[1]; $limit = (int)$x[2];
                $stripped = preg_replace('/\s*LIMIT\s+\d+\s*,\s*\d+\s*$/i', '', $stripped);
            } elseif (preg_match('/LIMIT\s+(\d+)/i', $stripped, $x)) {
                $limit = (int)$x[1];
                $stripped = preg_replace('/\s*LIMIT\s+\d+\s*$/i', '', $stripped);
            }
            $rows = $this->translator->executeSelect($stripped);

            // Build unique combinations
            $seen = [];
            $results = [];
            $isDistinct = (bool) preg_match('/SELECT\s+DISTINCT\b/i', $sql);

            foreach ($rows as $row) {
                $rowArr = (array) $row;
                $combo = [];

                foreach ($fm as $f) {
                    $func = strtoupper($f[1]);
                    $srcCol = $f[2];
                    // Strip table prefix
                    if (str_contains($srcCol, '.')) {
                        $srcCol = substr($srcCol, strpos($srcCol, '.') + 1);
                    }
                    $alias = $f[3];
                    $dateVal = $rowArr[$srcCol] ?? '';

                    // Parse date value
                    $ts = strtotime($dateVal);
                    if ($ts === false) {
                        $combo[$alias] = '0';
                        continue;
                    }
                    $combo[$alias] = match ($func) {
                        'YEAR'  => date('Y', $ts),
                        'MONTH' => (string)(int)date('m', $ts),
                        'DAY'   => (string)(int)date('d', $ts),
                        default => '0',
                    };
                }

                $key = implode('-', $combo);
                if ($isDistinct && isset($seen[$key])) continue;
                $seen[$key] = true;
                $results[] = (object) $combo;
            }

            // ORDER BY - inherit from parsed SQL (typically ORDER BY post_date DESC)
            // Results are already grouped, just preserve the order they appeared.

            if ($limit !== null || $offset > 0) {
                $results = array_slice($results, $offset, $limit ?? count($results));
            }

            $this->last_result = $results;
            $this->num_rows    = count($results);
            return $this->num_rows;
        }

        private function handleGroupByCount(string $sql): int|false
        {
            if (!preg_match('/GROUP\s+BY\s+([a-zA-Z0-9_.`]+)/i', $sql, $gm)) return 0;
            $groupCol = str_replace(['`', ' '], '', $gm[1]);
            if (str_contains($groupCol, '.')) {
                $groupCol = substr($groupCol, strpos($groupCol, '.') + 1);
            }

            $countAlias = 'num_posts';
            if (preg_match('/COUNT\s*\(\s*\*\s*\)\s+AS\s+([a-zA-Z0-9_]+)/i', $sql, $cm)) {
                $countAlias = $cm[1];
            }

            // Strip GROUP BY and COUNT, SELECT *
            $stripped = preg_replace('/GROUP\s+BY\s+[a-zA-Z0-9_.`]+/i', '', $sql);
            $stripped = preg_replace('/,?\s*COUNT\s*\(\s*\*\s*\)\s*(AS\s+[a-zA-Z0-9_]+)?/i', '', $stripped);
            $stripped = preg_replace('/SELECT\s+.*?\s+FROM/is', 'SELECT * FROM', $stripped);

            $rows = $this->translator->executeSelect($stripped);

            $groups = [];
            foreach ($rows as $row) {
                $key = ((array) $row)[$groupCol] ?? '__unknown__';
                $groups[$key] = ($groups[$key] ?? 0) + 1;
            }

            $results = [];
            foreach ($groups as $val => $count) {
                $obj = new \stdClass();
                $obj->{$groupCol}   = (string) $val;
                $obj->{$countAlias} = (string) $count;
                $results[] = $obj;
            }

            $this->last_result = $results;
            $this->num_rows    = count($results);
            return $this->num_rows;
        }

        private function handleUnionSelect(string $sql): int|false
        {
            // Subquery wrapper: SELECT ... FROM (... UNION ALL ...) AS x GROUP BY ...
            if (preg_match('/GROUP\s+BY/i', $sql) && preg_match('/COUNT\s*\(\s*\*\s*\)/i', $sql)) {
                if (preg_match('/FROM\s*\((.+)\)\s*AS\s+/is', $sql, $sub)) {
                    $parts = preg_split('/\bUNION\s+ALL\b/i', $sub[1]);
                    $allRows = [];
                    foreach ($parts as $part) {
                        $part = trim($part);
                        if (stripos($part, 'SELECT') === 0) {
                            foreach ($this->translator->executeSelect($part) as $r) $allRows[] = $r;
                        }
                    }

                    if (preg_match('/GROUP\s+BY\s+([a-zA-Z0-9_.`]+)/i', $sql, $gm)) {
                        $groupCol = str_replace(['`', ' '], '', $gm[1]);
                        if (str_contains($groupCol, '.')) $groupCol = substr($groupCol, strpos($groupCol, '.') + 1);

                        $countAlias = 'num_posts';
                        if (preg_match('/COUNT\s*\(\s*\*\s*\)\s+AS\s+([a-zA-Z0-9_]+)/i', $sql, $cm)) $countAlias = $cm[1];

                        $groups = [];
                        foreach ($allRows as $row) {
                            $key = ((array) $row)[$groupCol] ?? '__unknown__';
                            $groups[$key] = ($groups[$key] ?? 0) + 1;
                        }

                        $results = [];
                        foreach ($groups as $val => $count) {
                            $results[] = (object) [$groupCol => (string) $val, $countAlias => (string) $count];
                        }

                        $this->last_result = $results;
                        $this->num_rows    = count($results);
                        return $this->num_rows;
                    }

                    $this->last_result = $allRows;
                    $this->num_rows    = count($allRows);
                    return $this->num_rows;
                }
            }

            // Simple UNION - plain UNION deduplicates, UNION ALL does not
            $isAll = (bool) preg_match('/\bUNION\s+ALL\b/i', $sql);
            $parts = preg_split('/\bUNION\s+(ALL\s+)?/i', $sql);
            $allRows = [];
            $seen = [];
            foreach ($parts as $part) {
                $part = trim($part);
                if (stripos($part, 'SELECT') === 0) {
                    foreach ($this->translator->executeSelect($part) as $r) {
                        if (!$isAll) {
                            $k = serialize($r);
                            if (isset($seen[$k])) continue;
                            $seen[$k] = true;
                        }
                        $allRows[] = $r;
                    }
                }
            }
            $this->last_result = $allRows;
            $this->num_rows    = count($allRows);
            return $this->num_rows;
        }

        // -- INSERT -----------------------------------------------------------

        private const AUTO_PK_MAP = [
            'users'              => 'ID',
            'usermeta'           => 'umeta_id',
            'posts'              => 'ID',
            'postmeta'           => 'meta_id',
            'comments'           => 'comment_ID',
            'commentmeta'        => 'meta_id',
            'terms'              => 'term_id',
            'termmeta'           => 'meta_id',
            'term_taxonomy'      => 'term_taxonomy_id',
            'options'            => 'option_id',
            'links'              => 'link_id',
            'blogs'              => 'blog_id',
            'blogmeta'           => 'meta_id',
            'signups'            => 'signup_id',
            'site'               => 'id',
            'sitemeta'           => 'meta_id',
            'registration_log'   => 'ID',
        ];

        private const COLUMN_DEFAULTS = [
            'posts' => [
                'post_status'    => 'publish',
                'post_type'      => 'post',
                'comment_status' => 'open',
                'ping_status'    => 'open',
                'post_password'  => '',
                'post_parent'    => '0',
                'menu_order'     => '0',
                'post_mime_type' => '',
            ],
            'users' => ['user_status' => '0'],
            'term_taxonomy' => ['parent' => '0', 'count' => '0'],
            'comments' => ['comment_approved' => '1', 'comment_type' => 'comment', 'comment_parent' => '0'],
            'options' => ['autoload' => 'yes'],
        ];

        /**
         * Unique keys of WordPress core tables, mirroring the UNIQUE
         * declarations in wp-admin/includes/schema.php. Core tables are
         * created by wp_install_defaults through this engine without a
         * CREATE TABLE we can parse (dbDelta path varies by version), so
         * the contract is mirrored here. Plugin tables get their unique
         * sets from parsed CREATE TABLE / ALTER TABLE instead.
         */
        private const CORE_UNIQUE = [
            'options'       => [['option_name']],
            'term_taxonomy' => [['term_id', 'taxonomy']],
        ];

        /**
         * Unique key column sets enforced for a table: parsed schema first,
         * core mirror as fallback.
         *
         * @return string[][]
         */
        private function uniqueKeysFor(string $table): array
        {
            $schema = $this->storage->readSchema($table);
            $sets = $schema['unique'] ?? [];
            if ($sets === []) {
                foreach (self::CORE_UNIQUE as $suffix => $coreSets) {
                    if ($table === $suffix || str_ends_with($table, '_' . $suffix)) {
                        return $coreSets;
                    }
                }
            }
            return $sets;
        }

        private function resolveColumnDefaults(string $table): array
        {
            $defaults = [];
            $schema = $this->storage->readSchema($table);
            if ($schema !== null && !empty($schema['defaults'])) {
                $defaults = $schema['defaults'];
            }
            foreach (self::COLUMN_DEFAULTS as $suffix => $tableDefaults) {
                if ($table === $suffix || str_ends_with($table, '_' . $suffix)) {
                    $defaults = array_merge($tableDefaults, $defaults);
                    break;
                }
            }
            return $defaults;
        }

        private function resolveAutoPkColumn(string $table): ?string
        {
            $schema = $this->storage->readSchema($table);
            if ($schema !== null) {
                return isset($schema['auto']) && $schema['auto'] !== null
                    ? (string) $schema['auto']
                    : (isset($schema['pk']) && $schema['pk'] !== null ? (string) $schema['pk'] : null);
            }
            foreach (self::AUTO_PK_MAP as $suffix => $pkCol) {
                if ($table === $suffix || str_ends_with($table, '_' . $suffix)) return $pkCol;
            }
            return null;
        }

        private function handleInsert(string $sql): int|false
        {
            $parsed = $this->insertTokenizer->tokenize($sql);
            if ($parsed === null) {
                error_log('[HtmlDB] INSERT PARSE FAILED: ' . substr($sql, 0, 500));
                return false;
            }

            $table   = $parsed['table'];
            $columns = $parsed['columns'];
            $rows    = $parsed['rows'];
            $onDup   = $parsed['onDuplicate'] ?? null;
            $ignore  = (bool) ($parsed['ignore'] ?? false);

            // URL Safeguard
            $rows = $this->applySiteurlSafeguard($table, $columns, $rows);

            // Column defaults
            $defaults = $this->resolveColumnDefaults($table);
            if (!empty($defaults)) {
                foreach ($defaults as $defCol => $defVal) {
                    if (!in_array($defCol, $columns, true)) {
                        $columns[] = $defCol;
                        foreach ($rows as &$row) { $row[] = $defVal; }
                        unset($row);
                    }
                }
            }

            // INSERT IGNORE: drop rows that collide with an existing unique
            // key (PRIMARY KEY, schema-declared UNIQUE, or core-mirrored).
            // MySQL skips them with no error and returns 0 affected rows,
            // which is exactly what WP_Upgrader::create_lock() and the
            // taxonomy/comment locks test to detect that another process
            // already holds the option lock. Runs before auto-increment so
            // skipped rows never burn sequence values.
            if ($ignore) {
                $uniqueSets = $this->uniqueKeysFor($table);
                $pkForCheck = $this->resolveAutoPkColumn($table);
                if ($pkForCheck !== null) {
                    $uniqueSets[] = [$pkForCheck];
                }
                if ($uniqueSets !== []) {
                    $kept = [];
                    foreach ($rows as $row) {
                        $conflict = false;
                        foreach ($uniqueSets as $set) {
                            $conds = [];
                            $complete = true;
                            foreach ($set as $uc) {
                                $idx = array_search($uc, $columns, true);
                                if ($idx === false) { $complete = false; break; }
                                $conds[$uc] = (string) ($row[$idx] ?? '');
                            }
                            if (!$complete) continue;
                            if ($this->storage->findMatchingRows($table, $conds) !== []) {
                                $conflict = true;
                                break;
                            }
                        }
                        if (!$conflict) $kept[] = $row;
                    }
                    $rows = $kept;
                    if ($rows === []) {
                        $this->rows_affected = 0;
                        $this->insert_id = 0;
                        return 0;
                    }
                }
            }

            // Auto-increment PK
            $pkCol = $this->resolveAutoPkColumn($table);
            $firstGeneratedId = 0;

            if ($pkCol !== null && !in_array($pkCol, $columns, true)) {
                array_unshift($columns, $pkCol);
                foreach ($rows as $idx => &$row) {
                    $newId = $this->storage->nextAutoIncrement($table);
                    if ($idx === 0) $firstGeneratedId = $newId;
                    array_unshift($row, (string) $newId);
                }
                unset($row);
            } elseif ($pkCol !== null) {
                $pkIdx = array_search($pkCol, $columns, true);
                if ($pkIdx !== false) {
                    $maxPk = 0;
                    foreach ($rows as $r) {
                        if (isset($r[$pkIdx]) && (int) $r[$pkIdx] > $maxPk) {
                            $maxPk = (int) $r[$pkIdx];
                        }
                    }
                    $firstGeneratedId = $maxPk;
                    // Keep the sequence ahead of explicit IDs so future
                    // auto-increments never collide.
                    if ($maxPk > 0) {
                        $this->storage->advanceSequence($table, $maxPk);
                    }
                }
            }

            // ON DUPLICATE KEY UPDATE: rows whose PK already exists are
            // updated with the duplicate clause instead of inserted.
            if ($onDup !== null && $pkCol !== null) {
                $pkIdx = array_search($pkCol, $columns, true);
                $insertRows = [];
                $affected = 0;
                foreach ($rows as $row) {
                    $pk = (string) ($row[$pkIdx] ?? '');
                    $existing = $pk !== ''
                        ? $this->storage->findRowByPk($table, $pk)
                        : null;
                    if ($existing !== null) {
                        $setValues = [];
                        foreach ($onDup as $col => $expr) {
                            if (preg_match('/^VALUES\(\s*([a-zA-Z0-9_]+)\s*\)$/i', $expr, $vm)) {
                                $srcIdx = array_search($vm[1], $columns, true);
                                $setValues[$col] = $srcIdx !== false ? (string) ($row[$srcIdx] ?? '') : '';
                            } elseif (preg_match('/^([a-zA-Z0-9_]+)\s*\+\s*(\d+(?:\.\d+)?)$/', $expr, $dm)
                                && $dm[1] === $col) {
                                $setValues[$col] = ['__delta__' => (float) $dm[2]];
                            } elseif (strcasecmp($expr, 'VALUES') !== 0) {
                                $setValues[$col] = trim($expr, "'\"");
                            }
                        }
                        $tx = $this->storage->nextTxId();
                        $affected += $this->storage->updateRowsByPk($table, $pk, $setValues, $tx);
                    } else {
                        $insertRows[] = $row;
                    }
                }
                $rows = $insertRows;
                if (empty($rows)) {
                    $this->rows_affected = $affected > 0 ? $affected : 0;
                    $this->insert_id = $firstGeneratedId;
                    return $this->rows_affected;
                }
            }

            // Get global TX ID
            $txId = $this->storage->nextTxId();

            if (count($rows) === 1) {
                $payload = array_combine($columns, $rows[0]);
                $this->rows_affected = $this->storage->insert($table, $payload, $txId);
            } else {
                $this->rows_affected = $this->storage->insertBatch($table, $columns, $rows, $txId);
            }

            $this->insert_id = $firstGeneratedId;
            return $this->rows_affected;
        }

        // -- REPLACE ----------------------------------------------------------

        private function handleReplace(string $sql): int|false
        {
            // REPLACE = delete conflicting row, then insert. Convert the
            // statement and pre-delete the PKs it carries.
            $converted = preg_replace('/^REPLACE\s+/i', 'INSERT ', $sql);
            $parsed = $this->insertTokenizer->tokenize((string) $converted);
            if ($parsed !== null) {
                $pkCol = $this->resolveAutoPkColumn($parsed['table']);
                if ($pkCol !== null) {
                    $pkIdx = array_search($pkCol, $parsed['columns'], true);
                    if ($pkIdx !== false) {
                        $tx = $this->storage->nextTxId();
                        foreach ($parsed['rows'] as $row) {
                            $pk = (string) ($row[$pkIdx] ?? '');
                            if ($pk !== '') {
                                $this->storage->deleteRowsByPk($parsed['table'], $pk, $tx);
                            }
                        }
                    }
                }
            }
            return $this->handleInsert((string) $converted);
        }

        // -- UPDATE -----------------------------------------------------------

        private function handleUpdate(string $sql): int|false
        {
            $parsed = $this->mutationParser->parseUpdate($sql);
            if ($parsed === null) return 0;

            $setValues = $this->applySiteurlSafeguardToSet($parsed['table'], $parsed['set']);

            $txId = $this->storage->nextTxId();
            $this->rows_affected = $this->storage->updateRows(
                $parsed['table'], $setValues, $parsed['conditions'], $txId
            );
            return $this->rows_affected;
        }

        // -- DELETE -----------------------------------------------------------

        private function handleDelete(string $sql): int|false
        {
            $parsed = $this->mutationParser->parseDelete($sql);
            if ($parsed === null) return 0;

            $txId = $this->storage->nextTxId();
            $this->rows_affected = $this->storage->deleteRows(
                $parsed['table'], $parsed['conditions'], $txId
            );
            return $this->rows_affected;
        }

        // -- DDL --------------------------------------------------------------

        /**
         * CREATE TABLE / ALTER TABLE are persisted as a per-table
         * _schema.json (columns, PK, auto-increment column, defaults).
         * Without this, plugin tables (WooCommerce, ACF custom tables, …)
         * silently lose their PK and AUTO_INCREMENT: inserts get md5 row
         * keys and UPDATE/DELETE by id match nothing.
         */
        private function handleDdl(string $sql): bool
        {
            $sql = trim($sql);

            if (preg_match('/^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)\s*\(/is', $sql, $m)) {
                $table = $m[1];
                // Locate the matching close paren for the column list
                $open = strpos($sql, '(', strlen($m[0]) - 1);
                if ($open === false) return true;
                $depth = 0;
                $close = -1;
                $inStr = false; $strCh = '';
                $len = strlen($sql);
                for ($i = $open; $i < $len; $i++) {
                    $ch = $sql[$i];
                    if ($inStr) {
                        if ($ch === '\\') { $i++; continue; }
                        if ($ch === $strCh) $inStr = false;
                        continue;
                    }
                    if ($ch === "'" || $ch === '"') { $inStr = true; $strCh = $ch; continue; }
                    if ($ch === '(') $depth++;
                    elseif ($ch === ')') {
                        $depth--;
                        if ($depth === 0) { $close = $i; break; }
                    }
                }
                if ($close === -1) return true;
                $body = substr($sql, $open + 1, $close - $open - 1);
                $schema = $this->parseCreateTableBody($body);
                // Re-creating an existing table (dbDelta re-runs, plugin
                // reactivation): keep schema authoritative but data was
                // already dropped/recreated per plugin expectations only
                // when the plugin issued DROP first. MySQL CREATE TABLE
                // without IF NOT EXISTS on an existing table errors; the
                // plugin then runs dbDelta ALTERs. Safer: if the table
                // already has stored rows, merge new columns into schema
                // instead of clobbering PK/rows.
                $existing = $this->storage->readSchema($table);
                if ($existing !== null) {
                    $schema['columns']  = $existing['columns'] + $schema['columns'];
                    $schema['defaults'] = $schema['defaults'] + $existing['defaults'];
                    if (($existing['pk'] ?? null) !== null && $schema['pk'] === null) {
                        $schema['pk'] = $existing['pk'];
                    }
                    if (($existing['auto'] ?? null) !== null && $schema['auto'] === null) {
                        $schema['auto'] = $existing['auto'];
                    }
                    // Union unique sets: dbDelta re-runs must not lose keys
                    // declared earlier (or added via ALTER).
                    $mergedUnique = $existing['unique'] ?? [];
                    foreach ($schema['unique'] ?? [] as $uset) {
                        $known = false;
                        foreach ($mergedUnique as $eu) {
                            if (implode(',', $eu) === implode(',', $uset)) { $known = true; break; }
                        }
                        if (!$known) $mergedUnique[] = $uset;
                    }
                    $schema['unique'] = $mergedUnique;
                }
                $this->storage->writeSchema($table, $schema);
                return true;
            }

            if (preg_match('/^ALTER\s+TABLE\s+([a-zA-Z0-9_]+)\s+(.+)$/is', $sql, $m)) {
                $this->applyAlterTable($m[1], $m[2]);
                return true;
            }

            // CREATE INDEX / DATABASE / USER etc: accepted, no-op.
            return true;
        }

        /**
         * Split a CREATE TABLE body into top-level definitions (commas at
         * paren depth 0, quotes respected).
         *
         * @return string[]
         */
        private function splitTopLevel(string $body): array
        {
            $parts = [];
            $cur = '';
            $depth = 0;
            $inStr = false; $strCh = '';
            $len = strlen($body);
            for ($i = 0; $i < $len; $i++) {
                $ch = $body[$i];
                if ($inStr) {
                    $cur .= $ch;
                    if ($ch === '\\') { if ($i + 1 < $len) { $cur .= $body[++$i]; } continue; }
                    if ($ch === $strCh) $inStr = false;
                    continue;
                }
                if ($ch === "'" || $ch === '"') { $inStr = true; $strCh = $ch; $cur .= $ch; continue; }
                if ($ch === '(') { $depth++; $cur .= $ch; continue; }
                if ($ch === ')') { $depth--; $cur .= $ch; continue; }
                if ($ch === ',' && $depth === 0) { $parts[] = $cur; $cur = ''; continue; }
                $cur .= $ch;
            }
            if (trim($cur) !== '') $parts[] = $cur;
            return $parts;
        }

        /**
         * Parse the column/constraint list of a CREATE TABLE statement.
         *
         * @return array{pk:?string,auto:?string,columns:array<string,array>,defaults:array<string,string>}
         */
        private function parseCreateTableBody(string $body): array
        {
            $schema = ['pk' => null, 'auto' => null, 'columns' => [], 'defaults' => [], 'unique' => []];

            foreach ($this->splitTopLevel($body) as $def) {
                $def = trim($def);
                if ($def === '') continue;
                $def = preg_replace('/^\`|\`$/', '', trim($def));

                if (preg_match('/^PRIMARY\s+KEY\s*\(([^)]*)\)/i', $def, $pm)) {
                    $cols = array_map(fn($c) => trim($c, " \t`"), explode(',', $pm[1]));
                    $schema['pk'] = $cols[0] ?? null; // composite: first column drives rowKey
                    continue;
                }
                // UNIQUE KEY name (cols) / UNIQUE (cols): drives INSERT
                // IGNORE duplicate detection. Core declares option_name
                // and (term_id,taxonomy) this way.
                if (preg_match('/^UNIQUE\s+(?:KEY\s+|INDEX\s+)?(?:`?[a-zA-Z0-9_]+`?\s*)?\(([^)]*)\)/i', $def, $um)) {
                    $cols = array_map(fn($c) => trim($c, " \t`"), explode(',', $um[1]));
                    $cols = array_values(array_filter($cols, fn($c) => $c !== ''));
                    if ($cols !== []) $schema['unique'][] = $cols;
                    continue;
                }
                if (preg_match('/^(UNIQUE\s+)?(KEY|INDEX|FULLTEXT|SPATIAL|CONSTRAINT|FOREIGN\s+KEY|CHECK)\b/i', $def)) {
                    continue;
                }

                // Column definition: name type [modifiers]
                if (!preg_match('/^`?([a-zA-Z0-9_]+)`?\s+([a-zA-Z]+(?:\s*\([^)]*\))?(?:\s+unsigned)?(?:\s+zerofill)?)/i', $def, $cm)) {
                    continue;
                }
                $col  = $cm[1];
                $type = strtolower($cm[2]);
                $rest = substr($def, strlen($cm[0]));

                $schema['columns'][$col] = ['type' => $type];

                if (preg_match('/\bUNIQUE\b/i', $rest) && $col !== ($schema['pk'] ?? '')) {
                    $schema['unique'][] = [$col]; // inline column UNIQUE
                }
                if (preg_match('/\bAUTO_INCREMENT\b/i', $rest)) {
                    $schema['auto'] = $col;
                    if ($schema['pk'] === null) $schema['pk'] = $col;
                }
                if (preg_match("/DEFAULT\\s+('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|[a-zA-Z0-9_.+-]+)/i", $rest, $dm)) {
                    $val = $dm[1];
                    if (preg_match('/^CURRENT_TIMESTAMP|NOW\(|^NULL$/i', $val)) {
                        // function defaults resolved at insert time; NULL default = no default
                    } elseif (preg_match("/^'((?:[^']|'')*)'$/s", $val, $qm)) {
                        $schema['defaults'][$col] = str_replace("''", "'", $qm[1]);
                    } elseif (preg_match('/^"((?:[^"]|"")*)"$/s', $val, $qm)) {
                        $schema['defaults'][$col] = str_replace('""', '"', $qm[1]);
                    } else {
                        $schema['defaults'][$col] = $val;
                    }
                }
            }

            return $schema;
        }

        /**
         * Apply ALTER TABLE actions (ADD/MODIFY/CHANGE/DROP COLUMN, ADD
         * PRIMARY KEY) to the persisted schema.
         */
        private function applyAlterTable(string $table, string $actions): void
        {
            $schema = $this->storage->readSchema($table);
            if ($schema === null) {
                // Core or implicit table: seed schema from stored columns so
                // the ALTER has something to modify. PK stays from the map.
                $cols = [];
                foreach ($this->storage->listColumns($table) as $c) {
                    $cols[$c] = ['type' => 'longtext'];
                }
                $pk = $this->storage->resolvePkColumn($table);
                $schema = ['pk' => $pk, 'auto' => $pk, 'columns' => $cols, 'defaults' => [], 'unique' => []];
            }

            foreach ($this->splitTopLevel($actions) as $action) {
                $action = trim($action);
                if ($action === '') continue;

                // ADD [UNIQUE] KEY/INDEX must be consumed before the
                // ADD COLUMN branch, otherwise the index name is parsed
                // as a column definition.
                if (preg_match('/^ADD\s+(UNIQUE\s+)?(?:KEY|INDEX)\s*(?:`?[a-zA-Z0-9_]+`?\s*)?\(([^)]*)\)/i', $action, $um)) {
                    if (!empty($um[1])) {
                        $cols = array_map(fn($c) => trim($c, " \t`"), explode(',', $um[2]));
                        $cols = array_values(array_filter($cols, fn($c) => $c !== ''));
                        if ($cols !== []) $schema['unique'][] = $cols;
                    }
                    continue;
                }

                if (preg_match('/^ADD\s+(?:COLUMN\s+)?\(?`?([a-zA-Z0-9_]+)`?\s+([a-zA-Z]+(?:\s*\([^)]*\))?)/i', $action, $am)) {
                    $col = $am[1];
                    $schema['columns'][$col] = ['type' => strtolower($am[2])];
                    $rest = substr($action, strlen($am[0]));
                    if (preg_match('/\bAUTO_INCREMENT\b/i', $rest)) {
                        $schema['auto'] = $col;
                        if ($schema['pk'] === null) $schema['pk'] = $col;
                    }
                    if (preg_match("/DEFAULT\\s+('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|[a-zA-Z0-9_.+-]+)/i", $rest, $dm)
                        && !preg_match('/^CURRENT_TIMESTAMP|NOW\(|^NULL$/i', $dm[1])) {
                        $v = $dm[1];
                        if (preg_match("/^'((?:[^']|'')*)'$/s", $v, $qm)) {
                            $schema['defaults'][$col] = str_replace("''", "'", $qm[1]);
                        } elseif (preg_match('/^"((?:[^"]|"")*)"$/s', $v, $qm)) {
                            $schema['defaults'][$col] = str_replace('""', '"', $qm[1]);
                        } else {
                            $schema['defaults'][$col] = $v;
                        }
                    }
                    continue;
                }
                if (preg_match('/^DROP\s+(?:INDEX|KEY)\b/i', $action)) {
                    continue; // index names are not tracked; nothing to remove
                }
                if (preg_match('/^DROP\s+(?:COLUMN\s+)?`?([a-zA-Z0-9_]+)`?/i', $action, $dm)) {
                    unset($schema['columns'][$dm[1]], $schema['defaults'][$dm[1]]);
                    if (($schema['pk'] ?? null) === $dm[1]) $schema['pk'] = null;
                    if (($schema['auto'] ?? null) === $dm[1]) $schema['auto'] = null;
                    $schema['unique'] = array_values(array_filter(
                        $schema['unique'] ?? [],
                        fn(array $set): bool => !in_array($dm[1], $set, true)
                    ));
                    continue;
                }
                if (preg_match('/^CHANGE\s+(?:COLUMN\s+)?`?([a-zA-Z0-9_]+)`?\s+`?([a-zA-Z0-9_]+)`?/i', $action, $cm)) {
                    [$old, $new] = [$cm[1], $cm[2]];
                    if (isset($schema['columns'][$old])) {
                        $schema['columns'][$new] = $schema['columns'][$old];
                        unset($schema['columns'][$old]);
                    }
                    if (array_key_exists($old, $schema['defaults'])) {
                        $schema['defaults'][$new] = $schema['defaults'][$old];
                        unset($schema['defaults'][$old]);
                    }
                    if (($schema['pk'] ?? null) === $old) $schema['pk'] = $new;
                    if (($schema['auto'] ?? null) === $old) $schema['auto'] = $new;
                    continue;
                }
                if (preg_match('/^ADD\s+PRIMARY\s+KEY\s*\(([^)]*)\)/i', $action, $pm)) {
                    $cols = array_map(fn($c) => trim($c, " \t`"), explode(',', $pm[1]));
                    $schema['pk'] = $cols[0] ?? $schema['pk'];
                    continue;
                }
                // MODIFY/other ALTER actions: accepted, no schema change needed.
            }

            $this->storage->writeSchema($table, $schema);
        }

        /**
         * DROP TABLE / TRUNCATE TABLE must actually remove data, otherwise
         * re-created tables resurrect stale rows.
         */
        private function handleDropTruncate(string $sql): bool
        {
            if (preg_match('/^DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(.+)$/is', trim($sql), $m)) {
                foreach (explode(',', $m[1]) as $t) {
                    $t = trim($t, " \t\n\r`;");
                    if ($t !== '') $this->storage->dropTable($t);
                }
                return true;
            }
            if (preg_match('/^TRUNCATE\s+(?:TABLE\s+)?([a-zA-Z0-9_]+)/is', trim($sql), $m)) {
                $this->storage->truncateTable($m[1]);
                return true;
            }
            if (preg_match('/^DROP\s+DATABASE/i', trim($sql))) {
                return true; // never nuke the whole storage root
            }
            return true;
        }

        // -- SHOW / DESCRIBE --------------------------------------------------

        private function handleShow(string $sql): int
        {
            $this->last_result = [];
            $this->num_rows    = 0;

            if (preg_match('/SHOW\s+COLUMNS\s+FROM\s+([a-zA-Z0-9_]+)/i', $sql, $cm)) {
                $schema = $this->storage->readSchema($cm[1]);
                $cols = $this->storage->listColumns($cm[1]);
                $uniqueCols = [];
                foreach ($schema['unique'] ?? [] as $uset) {
                    foreach ($uset as $uc) $uniqueCols[$uc] = true;
                }
                foreach ($cols as $col) {
                    $type  = $schema['columns'][$col]['type'] ?? 'longtext';
                    $extra = (($schema['auto'] ?? null) === $col) ? 'auto_increment' : '';
                    $key   = (($schema['pk'] ?? null) === $col) ? 'PRI' : (isset($uniqueCols[$col]) ? 'UNI' : '');
                    $def   = $schema['defaults'][$col] ?? null;
                    $this->last_result[] = (object) [
                        'Field' => $col, 'Type' => $type, 'Null' => 'YES',
                        'Key' => $key, 'Default' => $def, 'Extra' => $extra,
                    ];
                }
                $this->num_rows = count($this->last_result);
            }

            if (preg_match('/SHOW\s+TABLES/i', $sql)) {
                $likePattern = null;
                if (preg_match('/LIKE\s+[\'"](.+?)[\'"]/i', $sql, $lm)) {
                    $likePattern = '/^' . str_replace(['\%', '\_'], ['.*', '.'], preg_quote($lm[1], '/')) . '$/i';
                }

                foreach ($this->storage->listTables() as $tbl) {
                    if ($likePattern !== null && !preg_match($likePattern, $tbl)) continue;
                    $key = 'Tables_in_' . ($this->dbname ?: 'htmldb');
                    $this->last_result[] = (object) [$key => $tbl];
                }
                $this->num_rows = count($this->last_result);
            }

            return $this->num_rows;
        }

        private function handleDescribe(string $sql): int
        {
            $this->last_result = [];
            $this->num_rows    = 0;

            if (preg_match('/^(?:DESCRIBE|DESC)\s+([a-zA-Z0-9_]+)/i', trim($sql), $m)) {
                $schema = $this->storage->readSchema($m[1]);
                $uniqueCols = [];
                foreach ($schema['unique'] ?? [] as $uset) {
                    foreach ($uset as $uc) $uniqueCols[$uc] = true;
                }
                foreach ($this->storage->listColumns($m[1]) as $col) {
                    $type  = $schema['columns'][$col]['type'] ?? 'longtext';
                    $extra = (($schema['auto'] ?? null) === $col) ? 'auto_increment' : '';
                    $key   = (($schema['pk'] ?? null) === $col) ? 'PRI' : (isset($uniqueCols[$col]) ? 'UNI' : '');
                    $def   = $schema['defaults'][$col] ?? null;
                    $this->last_result[] = (object) [
                        'Field' => $col, 'Type' => $type, 'Null' => 'YES',
                        'Key' => $key, 'Default' => $def, 'Extra' => $extra,
                    ];
                }
                $this->num_rows = count($this->last_result);
            }

            return $this->num_rows;
        }

        // -- WordPress compatibility hacks ------------------------------------

        private function applySiteurlSafeguard(string $table, array $columns, array $rows): array
        {
            if (!str_ends_with($table, 'options')) return $rows;
            $nameIdx  = array_search('option_name',  $columns, true);
            $valueIdx = array_search('option_value', $columns, true);
            if ($nameIdx === false || $valueIdx === false) return $rows;

            foreach ($rows as &$row) {
                $name  = $row[$nameIdx]  ?? '';
                $value = trim($row[$valueIdx] ?? '');
                if (in_array($name, ['siteurl', 'home'], true) && $value === '') {
                    $row[$valueIdx] = $this->inferSiteUrl();
                }
            }
            unset($row);
            return $rows;
        }

        private function applySiteurlSafeguardToSet(string $table, array $set): array
        {
            if (!str_ends_with($table, 'options')) return $set;
            if (isset($set['option_value'])
                && trim($set['option_value']) === ''
                && isset($set['option_name'])
                && in_array($set['option_name'], ['siteurl', 'home'], true)
            ) {
                $set['option_value'] = $this->inferSiteUrl();
            }
            return $set;
        }

        private function inferSiteUrl(): string
        {
            $scheme = (!empty($_SERVER['HTTPS']) && $_SERVER['HTTPS'] !== 'off') ? 'https' : 'http';
            $host = $_SERVER['HTTP_HOST'] ?? 'localhost';
            return $scheme . '://' . $host;
        }
    }

    // Bootstrap
    $GLOBALS['wpdb'] = new HtmlDatabase_WPDB(
        defined('DB_USER')     ? DB_USER     : '',
        defined('DB_PASSWORD') ? DB_PASSWORD : '',
        defined('DB_NAME')     ? DB_NAME     : '',
        defined('DB_HOST')     ? DB_HOST     : ''
    );
}
