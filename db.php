<?php
/**
 * Plugin Name: HTML Database Drop-in
 * Description: Esoteric HTML-based database engine for WordPress — replaces MySQL with flat HTML files.
 * Version: 3.0.0
 *
 * Architecture (v3 — Sharded Storage):
 *   - Each table is a folder: html_db/{table}/ with chunk files and an append-only WAL.
 *   - Chunks hold ≤ CHUNK_SIZE rows (default 500) as full HTML pages with retro-terminal CSS.
 *   - All mutations (INSERT, UPDATE, DELETE) append to wal.html — O(1), crash-safe.
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
// Namespace: HtmlDatabase\Core — Configuration, Shard Router, Storage Manager
// ---------------------------------------------------------------------------
namespace HtmlDatabase\Core {

    use SplFileObject;
    use RuntimeException;

    /**
     * Immutable runtime configuration.
     */
    final readonly class Configuration
    {
        /** Maximum rows per chunk file. */
        public int $chunkSize;

        /** Number of WAL entries before inline compaction triggers. */
        public int $compactThreshold;

        public function __construct(
            public string $basePath,
            int           $chunkSize        = 500,
            int           $compactThreshold = 200
        ) {
            $this->chunkSize        = $chunkSize;
            $this->compactThreshold = $compactThreshold;
        }
    }

    /**
     * Routes queries to the correct chunk file(s) within a table folder.
     *
     * Each table is stored as:
     *   html_db/{table}/
     *     _meta.json        — metadata (pk column, chunk_size, row count, etc.)
     *     _index.html       — human-browsable table-of-contents
     *     chunk_0001.html   — rows with PK 1..chunk_size
     *     chunk_0002.html   — rows with PK chunk_size+1..2*chunk_size
     *     wal.html          — append-only mutation journal
     *     .seq              — auto-increment counter
     */
    final class ShardRouter
    {
        public function __construct(private Configuration $config) {}

        /**
         * Compute which chunk file a given PK value belongs to.
         *
         * @return string Chunk filename, e.g. "chunk_0003.html"
         */
        public function chunkForPk(int $pkValue): string
        {
            $num = (int) ceil($pkValue / $this->config->chunkSize);
            if ($num < 1) $num = 1;
            return sprintf('chunk_%04d.html', $num);
        }

        /**
         * The PK range a given chunk covers.
         *
         * @return array{from: int, to: int}
         */
        public function chunkRange(string $chunkFile): array
        {
            if (preg_match('/chunk_(\d+)\.html$/', $chunkFile, $m)) {
                $num = (int) $m[1];
                $from = ($num - 1) * $this->config->chunkSize + 1;
                $to   = $num * $this->config->chunkSize;
                return ['from' => $from, 'to' => $to];
            }
            return ['from' => 1, 'to' => $this->config->chunkSize];
        }

        /**
         * Determine which chunk files to scan for given WHERE conditions.
         *
         * If conditions contain a PK equality (e.g. ID = 42), returns only
         * the one relevant chunk. Otherwise returns ALL chunks.
         *
         * @return string[] List of chunk filenames to scan.
         */
        public function resolveChunks(string $tableDir, ?string $pkCol, array $conditions): array
        {
            // If we have a PK equality condition, narrow to one chunk
            if ($pkCol !== null && isset($conditions[$pkCol])) {
                $pkVal = (int) $conditions[$pkCol];
                if ($pkVal > 0) {
                    $chunk = $this->chunkForPk($pkVal);
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
         * Ensure the table directory exists with security files.
         */
        public function ensureTableDir(string $table): string
        {
            $dir = $this->tableDir($table);
            if (!is_dir($dir)) {
                mkdir($dir, 0755, true);
                // Create security index.html in the table directory
                $this->writeSecurityIndex($dir, $table);
            }
            return $dir;
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
                $chunkLinks .= "      <li><a href=\"{$chunkFile}\">Chunk {$num}</a> — rows {$range}</li>\n";
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
    <nav><a href="../_index.html">⌂ Database</a></nav>
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
                    "      <li><a href=\"%s/_index.html\">%s</a> — %d rows, %d chunks</li>\n",
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
  <title>HtmlDB — Database Index</title>
  <link rel="stylesheet" href="_style.css">
</head>
<body>
  <header>
    <h1>🗄️ HtmlDB — Database Browser</h1>
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
    <h1>📝 {$table} — WAL</h1>
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

        // -- INSERT -----------------------------------------------------------

        /**
         * Append a single row to the WAL.
         */
        public function insert(string $table, array $payload, int $txId): int
        {
            $this->router->ensureTableDir($table);
            $html = $this->buildWalEntry($payload, 'insert', $txId, $this->extractPkFromPayload($table, $payload));
            $this->appendToWal($table, $html);
            $this->incrementMeta($table, 'total_rows', 1);
            $this->incrementMeta($table, 'wal_entries', 1);
            return 1;
        }

        /**
         * Append multiple rows in one atomic write.
         */
        public function insertBatch(string $table, array $columns, array $rows, int $txId): int
        {
            $this->router->ensureTableDir($table);
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
            $this->router->ensureTableDir($table);

            // We need to find matching rows to know which PKs to update.
            // Read from chunks + WAL, apply conditions, get PKs.
            $pkCol = $this->resolvePkColumn($table);
            $matchingRows = $this->findMatchingRows($table, $conditions);

            if (empty($matchingRows)) {
                // WP expects update to return 1 even when 0 rows matched in some contexts
                return 1;
            }

            $html = '';
            foreach ($matchingRows as $row) {
                $pk = $pkCol !== null ? ((string) ($row[$pkCol] ?? '0')) : '0';
                // Merge: existing row data + set values (set values override)
                $merged = array_merge($row, $setValues);
                $html .= $this->buildWalEntry($merged, 'update', $txId, $pk);
            }

            if ($html !== '') {
                $this->appendToWal($table, $html);
                $this->incrementMeta($table, 'wal_entries', count($matchingRows));
            }

            return count($matchingRows);
        }

        // -- DELETE (append-only) ---------------------------------------------

        /**
         * Append tombstone entries to the WAL.
         */
        public function deleteRows(string $table, array $conditions, int $txId): int
        {
            $this->router->ensureTableDir($table);
            $pkCol = $this->resolvePkColumn($table);
            $matchingRows = $this->findMatchingRows($table, $conditions);

            if (empty($matchingRows)) {
                return 0;
            }

            $html = '';
            foreach ($matchingRows as $row) {
                $pk = $pkCol !== null ? ((string) ($row[$pkCol] ?? '0')) : '0';
                $html .= $this->buildWalEntry($row, 'delete', $txId, $pk);
            }

            if ($html !== '') {
                $this->appendToWal($table, $html);
                $this->incrementMeta($table, 'wal_entries', count($matchingRows));
                $this->incrementMeta($table, 'total_rows', -count($matchingRows));
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
            $chunkFiles = $this->router->resolveChunks($tableDir, $pkCol, $conditions);

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
                    $pk  = $entry['pk'];
                    $op  = $entry['op'];
                    $key = ($pkCol !== null && $pk !== '0') ? $pk : count($rows);

                    if ($op === 'delete') {
                        unset($rows[$key]);
                    } elseif ($op === 'update') {
                        if (isset($rows[$key])) {
                            // Merge: existing data + WAL update
                            $rows[$key]['data'] = array_merge($rows[$key]['data'], $entry['data']);
                            $rows[$key]['tx']   = $entry['tx'];
                        } else {
                            // Update for a row not in chunks — treat as full row
                            $rows[$key] = ['data' => $entry['data'], 'tx' => $entry['tx']];
                        }
                    } elseif ($op === 'insert') {
                        $rows[$key] = ['data' => $entry['data'], 'tx' => $entry['tx']];
                    }
                }
            }

            return array_values($rows);
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

        // -- Auto-increment ---------------------------------------------------

        /**
         * Persistent per-table auto-increment counter.
         * Protected by exclusive flock for the full read-increment-write cycle.
         */
        public function nextAutoIncrement(string $table): int
        {
            $this->router->ensureTableDir($table);
            $seqPath = $this->router->seqPath($table);

            $file = new SplFileObject($seqPath, 'c+');
            if (!$file->flock(LOCK_EX)) {
                throw new RuntimeException("Unable to lock sequence file: {$seqPath}");
            }
            $file->rewind();
            $current = (int) $file->fgets();
            $next = $current + 1;

            // Crash-safe: write to temp, then rename
            $tmpPath = $seqPath . '.tmp';
            file_put_contents($tmpPath, (string) $next, LOCK_EX);
            rename($tmpPath, $seqPath);

            $file->flock(LOCK_UN);
            return $next;
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

            $tmpPath = $seqPath . '.tmp';
            file_put_contents($tmpPath, (string) $next, LOCK_EX);
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
         * Strategy:
         * 1. Acquire exclusive compact lock
         * 2. Rename wal.html → wal.processing.html (new writes go to fresh wal.html)
         * 3. Read all chunks + processing WAL, merge in-memory
         * 4. Rewrite affected chunks as temp + rename (crash-safe)
         * 5. Delete wal.processing.html
         * 6. Update _meta.json + _index.html
         */
        public function compact(string $table): void
        {
            $tableDir = $this->router->tableDir($table);
            $lockPath = $tableDir . '/.compact.lock';

            $lockFile = new SplFileObject($lockPath, 'c+');
            if (!$lockFile->flock(LOCK_EX | LOCK_NB)) {
                // Another process is compacting — skip
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

                // Step 3: Group WAL entries by target chunk
                $chunkUpdates = []; // chunkFile => [pk => entry]
                $allInserts   = []; // for rows without clear chunk assignment

                foreach ($walEntries as $entry) {
                    $pk = $entry['pk'];
                    $pkInt = (int) $pk;

                    if ($pkInt > 0 && $pkCol !== null) {
                        $chunkFile = $this->router->chunkForPk($pkInt);
                        $chunkUpdates[$chunkFile][$pk] = $entry;
                    } else {
                        $allInserts[] = $entry;
                    }
                }

                // Step 4: Update each affected chunk
                foreach ($chunkUpdates as $chunkFile => $entries) {
                    $chunkPath = $tableDir . '/' . $chunkFile;
                    $existing = file_exists($chunkPath) ? $this->parseHtmlRows($chunkPath) : [];

                    // Index by PK
                    $rowsByPk = [];
                    foreach ($existing as $row) {
                        $key = ($pkCol !== null && isset($row['data'][$pkCol])) ? $row['data'][$pkCol] : count($rowsByPk);
                        $rowsByPk[$key] = $row;
                    }

                    // Apply WAL entries
                    foreach ($entries as $pk => $entry) {
                        if ($entry['op'] === 'delete') {
                            unset($rowsByPk[$pk]);
                        } elseif ($entry['op'] === 'update') {
                            if (isset($rowsByPk[$pk])) {
                                $rowsByPk[$pk]['data'] = array_merge($rowsByPk[$pk]['data'], $entry['data']);
                                $rowsByPk[$pk]['tx'] = $entry['tx'];
                            } else {
                                $rowsByPk[$pk] = ['data' => $entry['data'], 'tx' => $entry['tx']];
                            }
                        } elseif ($entry['op'] === 'insert') {
                            $rowsByPk[$pk] = ['data' => $entry['data'], 'tx' => $entry['tx']];
                        }
                    }

                    // Write chunk (crash-safe: temp + rename)
                    $this->writeChunkFile($table, $chunkFile, array_values($rowsByPk));
                }

                // Step 5: Handle inserts without PK into appropriate chunks
                foreach ($allInserts as $entry) {
                    if ($entry['op'] === 'insert') {
                        // Put into chunk_0001 as fallback
                        $chunkFile = 'chunk_0001.html';
                        $chunkPath = $tableDir . '/' . $chunkFile;
                        $existing = file_exists($chunkPath) ? $this->parseHtmlRows($chunkPath) : [];
                        $existing[] = ['data' => $entry['data'], 'tx' => $entry['tx']];
                        $this->writeChunkFile($table, $chunkFile, $existing);
                    }
                }

                // Step 6: Cleanup
                @unlink($processingPath);

                // Step 7: Update metadata
                $this->rebuildMetaAndIndex($table);

            } finally {
                $lockFile->flock(LOCK_UN);
            }
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
                $safeCol = htmlspecialchars((string) $column, ENT_QUOTES | ENT_HTML5, 'UTF-8');
                $safeVal = htmlspecialchars((string) $value,  ENT_QUOTES | ENT_HTML5, 'UTF-8');
                $html .= sprintf('<td data-column="%s">%s</td>', $safeCol, $safeVal);
            }
            $html .= "</tr>\n";
            return $html;
        }

        /**
         * Build a chunk <tr> row (no MVCC attributes — compacted data).
         */
        private function buildChunkRow(array $payload, int $tx): string
        {
            $html = sprintf('<tr data-tx="%d">', $tx);
            foreach ($payload as $column => $value) {
                $safeCol = htmlspecialchars((string) $column, ENT_QUOTES | ENT_HTML5, 'UTF-8');
                $safeVal = htmlspecialchars((string) $value,  ENT_QUOTES | ENT_HTML5, 'UTF-8');
                $html .= sprintf('<td data-column="%s">%s</td>', $safeCol, $safeVal);
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
                $data[htmlspecialchars_decode($col, ENT_QUOTES | ENT_HTML5)]
                    = htmlspecialchars_decode($val, ENT_QUOTES | ENT_HTML5);

                $searchPos = $valEnd + 5;
            }
            return $data;
        }

        /**
         * Extract PK value from a payload row.
         */
        private function extractPkFromPayload(string $table, array $payload): string
        {
            $pkCol = $this->resolvePkColumn($table);
            if ($pkCol !== null && isset($payload[$pkCol])) {
                return (string) $payload[$pkCol];
            }
            return '0';
        }

        /**
         * Resolve the PK column for a table.
         */
        public function resolvePkColumn(string $table): ?string
        {
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
        private function writeChunkFile(string $table, string $chunkFile, array $rows): void
        {
            $tableDir = $this->router->tableDir($table);
            $targetPath = $tableDir . '/' . $chunkFile;
            $tmpPath    = $targetPath . '.tmp';

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

            // Count total chunks
            $allChunks = $this->router->listChunks($tableDir);
            if (!in_array($chunkFile, $allChunks, true)) {
                $allChunks[] = $chunkFile;
                sort($allChunks);
            }
            $totalChunks = count($allChunks);

            // Count approximate total rows
            $totalRows = count($rows) * $totalChunks; // rough estimate

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

            // Count total rows
            $totalRows = 0;
            foreach ($chunks as $chunkFile) {
                $rows = $this->parseHtmlRows($tableDir . '/' . $chunkFile);
                $totalRows += count($rows);
            }

            // Count WAL entries
            $walPath = $this->router->walPath($table);
            $walEntries = 0;
            if (file_exists($walPath) && filesize($walPath) > 0) {
                $entries = $this->parseWalEntries($walPath);
                $walEntries = count($entries);
            }

            // Write _meta.json
            $meta = [
                'table'       => $table,
                'pk'          => $this->resolvePkColumn($table),
                'chunk_size'  => $this->config->chunkSize,
                'chunks'      => count($chunks),
                'total_rows'  => $totalRows,
                'wal_entries' => $walEntries,
                'updated_at'  => gmdate('Y-m-d\TH:i:s\Z'),
            ];
            $metaPath = $this->router->metaPath($table);
            $tmpMeta  = $metaPath . '.tmp';
            file_put_contents($tmpMeta, json_encode($meta, JSON_PRETTY_PRINT));
            rename($tmpMeta, $metaPath);

            $this->metaCache[$table] = $meta;

            // Write _index.html
            $indexHtml = $this->pageBuilder->buildIndexPage($table, $chunks, $totalRows, $walEntries);
            $indexPath = $tableDir . '/_index.html';
            $tmpIndex  = $indexPath . '.tmp';
            file_put_contents($tmpIndex, $indexHtml);
            rename($tmpIndex, $indexPath);
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
         * Increment a numeric field in _meta.json.
         */
        private function incrementMeta(string $table, string $field, int $delta): void
        {
            $meta = $this->readMeta($table);
            $meta[$field] = ($meta[$field] ?? 0) + $delta;
            $meta['updated_at'] = gmdate('Y-m-d\TH:i:s\Z');
            $this->metaCache[$table] = $meta;

            $metaPath = $this->router->metaPath($table);
            $tmpMeta  = $metaPath . '.tmp';
            @file_put_contents($tmpMeta, json_encode($meta, JSON_PRETTY_PRINT));
            @rename($tmpMeta, $metaPath);
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
                // New WAL — write full HTML page
                ftruncate($fh, 0);
                rewind($fh);
                $header = $this->pageBuilder->buildWalHeader($table);
                $footer = $this->pageBuilder->buildWalFooter();
                fwrite($fh, $header . $trContent . $footer);
            } else {
                // Existing WAL — find </tbody> and insert before it
                $tailLen = min($size, 256);
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

            // Security: .htaccess
            $htaccessPath = $dir . '/.htaccess';
            if (!file_exists($htaccessPath)) {
                file_put_contents($htaccessPath, implode("\n", [
                    '# HtmlDB — Deny all direct access to database files',
                    '<IfModule mod_authz_core.c>',
                    '    Require all denied',
                    '</IfModule>',
                    '<IfModule !mod_authz_core.c>',
                    '    Order deny,allow',
                    '    Deny from all',
                    '</IfModule>',
                    '',
                ]));
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
/* HtmlDB v3.0 — Retro Terminal Theme */
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap');

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
// Namespace: HtmlDatabase\Parser — SQL Tokenizer & XPath Translator
// ---------------------------------------------------------------------------
namespace HtmlDatabase\Parser {

    use HtmlDatabase\Core\ShardedStorageManager;

    /**
     * Character-level state-machine tokenizer for SQL INSERT statements.
     *
     * Handles:
     *  - Multi-row: INSERT INTO t (a,b) VALUES (1,2),(3,4);
     *  - Nested parentheses inside values (serialised PHP arrays, JSON, etc.).
     *  - Single-quote escaping via backslash and doubled single-quotes.
     *  - Strips ON DUPLICATE KEY UPDATE clause on the fly.
     */
    final class InsertTokenizer
    {
        /**
         * Parse a full INSERT statement.
         *
         * @return array{table: string, columns: string[], rows: array[]}|null
         */
        public function tokenize(string $sql): ?array
        {
            $sql = trim($sql);

            // 1. Strip trailing semicolons
            $sql = rtrim($sql, "; \t\n\r");

            // 2. Strip ON DUPLICATE KEY UPDATE (state-machine aware)
            $sql = $this->stripOnDuplicate($sql);

            // 3. Extract table name
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

            return ['table' => $table, 'columns' => $columns, 'rows' => $rows];
        }

        private function extractParenGroup(string $sql, int $startPos): ?array
        {
            $len   = strlen($sql);
            $depth = 0;
            $inStr = false;
            $esc   = false;
            $start = null;

            for ($i = $startPos; $i < $len; $i++) {
                $ch = $sql[$i];
                if ($esc) { $esc = false; continue; }
                if ($ch === '\\') { $esc = true; continue; }
                if ($ch === "'" && !$esc) {
                    if ($inStr && isset($sql[$i + 1]) && $sql[$i + 1] === "'") { $i++; continue; }
                    $inStr = !$inStr;
                    continue;
                }
                if ($inStr) continue;
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
            $esc    = false;
            $depth  = 0;
            $len    = strlen($raw);

            for ($i = 0; $i < $len; $i++) {
                $ch = $raw[$i];
                if ($esc) { $buf .= $ch; $esc = false; continue; }
                if ($ch === '\\') { $esc = true; if ($inStr) $buf .= $ch; continue; }
                if ($ch === "'" && !$esc) {
                    if ($inStr && isset($raw[$i + 1]) && $raw[$i + 1] === "'") { $buf .= "'"; $i++; continue; }
                    $inStr = !$inStr;
                    continue;
                }
                if (!$inStr) {
                    if ($ch === '(') { $depth++; $buf .= $ch; continue; }
                    if ($ch === ')') { $depth--; $buf .= $ch; continue; }
                    if ($ch === ',' && $depth === 0) { $values[] = $this->cleanValue($buf); $buf = ''; continue; }
                }
                $buf .= $ch;
            }
            $values[] = $this->cleanValue($buf);
            return $values;
        }

        private function cleanValue(string $v): string
        {
            $v = trim($v);
            if (strcasecmp($v, 'NULL') === 0) return '';
            return stripslashes($v);
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
     * v3: No more DOMDocument — uses string-based parsing from
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

            // Table name
            if (!preg_match('/FROM\s+([a-zA-Z0-9_]+)/i', $sql, $tblMatch)) {
                return null;
            }
            $table = $tblMatch[1];

            // Strip table/alias prefixes: wp_posts.* → *, wp_posts.ID → ID
            $sql = preg_replace('/\b[a-zA-Z0-9_]+\.\*/', '*', $sql);
            $sql = preg_replace('/\b[a-zA-Z0-9_]+\.([a-zA-Z0-9_]+)/', '$1', $sql);

            // Columns
            preg_match('/SELECT\s+(.*?)\s+FROM/is', $sql, $colMatch);
            $rawCols = $colMatch[1] ?? '*';
            $columns = array_map('trim', explode(',', $rawCols));

            // LIMIT
            $limit  = null;
            $offset = 0;
            if (preg_match('/LIMIT\s+(\d+)\s*,\s*(\d+)/i', $sql, $limMatch)) {
                $offset = (int) $limMatch[1];
                $limit  = (int) $limMatch[2];
            } elseif (preg_match('/LIMIT\s+(\d+)/i', $sql, $limMatch)) {
                $limit = (int) $limMatch[1];
            }

            // ORDER BY
            $orderBy  = null;
            $orderDir = 'ASC';
            if (preg_match('/ORDER\s+BY\s+([a-zA-Z0-9_]+)(?:\s+(ASC|DESC))?/i', $sql, $ordMatch)) {
                $orderBy  = $ordMatch[1];
                $orderDir = strtoupper($ordMatch[2] ?? 'ASC');
            }

            // Raw WHERE
            $rawWhere = null;
            if (preg_match('/WHERE\s+(.*?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|\s+LIMIT|$)/is', $sql, $wm)) {
                $rawWhere = trim($wm[1]);
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
                                'limit' => 0, 'offset' => 0,
                                'orderBy' => null, 'orderDir' => 'ASC',
                                'rawWhere' => $rawWhere,
                                'calcFoundRows' => $calcFoundRows,
                            ];
                        }
                        continue;
                    }

                    // OR group → IN (handles both "(a OR b)" and bare "a OR b")
                    if (preg_match('/\bOR\b/i', $part)) {
                        $inner = $part;
                        // Strip optional outer parens
                        if (preg_match('/^\((.+)\)$/s', $inner, $paren)) {
                            $inner = $paren[1];
                        }
                        $orParts = preg_split('/\s+OR\s+/i', $inner);
                        $orCol = null;
                        $orVals = [];
                        $valid = true;
                        foreach ($orParts as $op) {
                            $op = trim($op);
                            // Strip any individual parens: (col = 'val') → col = 'val'
                            while (preg_match('/^\((.+)\)$/s', $op, $pm)) { $op = trim($pm[1]); }
                            if (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*[\'"](.*?)[\'"]$/s', $op, $om)) {
                                if ($orCol === null) $orCol = $om[1];
                                if ($om[1] === $orCol) $orVals[] = $om[2];
                                else { $valid = false; break; }
                            } else { $valid = false; break; }
                        }
                        if ($valid && $orCol !== null && !empty($orVals)) {
                            $inConditions[$orCol] = $orVals;
                        }
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
                'limit'             => $limit,
                'offset'            => $offset,
                'orderBy'           => $orderBy,
                'orderDir'          => $orderDir,
                'rawWhere'          => $rawWhere,
                'calcFoundRows'     => $calcFoundRows,
            ];
        }

        /**
         * Parse values from an IN(...) clause, handling both quoted strings and unquoted numbers.
         * e.g. "'a','b','c'" → ['a','b','c'], "1,2,3" → ['1','2','3'], "'a',1,'b'" → ['a','1','b']
         */
        private function parseInValues(string $raw): array
        {
            $vals = [];
            // Match quoted strings
            if (preg_match_all('/[\'"]([^\'"]*)[\'"]/', $raw, $quoted)) {
                $vals = $quoted[1];
            }
            // Also match unquoted numbers (not inside quotes)
            $stripped = preg_replace('/[\'"][^\'"]*[\'"]/', '', $raw);
            if (preg_match_all('/(-?\d+(?:\.\d+)?)/', $stripped, $nums)) {
                $vals = array_merge($vals, $nums[1]);
            }
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

                // At depth 0, check for \bAND\b keyword
                if ($depth === 0
                    && ($ch === 'A' || $ch === 'a')
                    && strtoupper(substr($where, $i, 3)) === 'AND'
                    && ($i === 0 || ctype_space($where[$i - 1]))
                    && ($i + 3 >= $len || ctype_space($where[$i + 3]))
                ) {
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

            // Apply conditions filter
            $filtered = [];
            foreach ($allRows as $row) {
                $data = $row['data'];

                // Equality conditions
                $match = true;
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

                // LIKE conditions (SQL % → regex .*, _ → regex .)
                if ($match) {
                    foreach ($parsed['likeConditions'] as $col => $pattern) {
                        $val = (string)($data[$col] ?? '');
                        // Build regex: escape everything, then convert SQL wildcards
                        $escaped = '';
                        $len = strlen($pattern);
                        for ($i = 0; $i < $len; $i++) {
                            $ch = $pattern[$i];
                            if ($ch === '%') { $escaped .= '.*'; }
                            elseif ($ch === '_') { $escaped .= '.'; }
                            else { $escaped .= preg_quote($ch, '/'); }
                        }
                        if (!preg_match('/^' . $escaped . '$/is', $val)) {
                            $match = false;
                            break;
                        }
                    }
                }

                // NOT LIKE conditions
                if ($match) {
                    foreach ($parsed['notLikeConditions'] as $col => $pattern) {
                        $val = (string)($data[$col] ?? '');
                        $escaped = '';
                        $len = strlen($pattern);
                        for ($i = 0; $i < $len; $i++) {
                            $ch = $pattern[$i];
                            if ($ch === '%') { $escaped .= '.*'; }
                            elseif ($ch === '_') { $escaped .= '.'; }
                            else { $escaped .= preg_quote($ch, '/'); }
                        }
                        if (preg_match('/^' . $escaped . '$/is', $val)) {
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

                if ($match) {
                    $filtered[] = $data;
                }
            }

            // ORDER BY
            if ($parsed['orderBy'] !== null && !empty($filtered)) {
                $col = $parsed['orderBy'];
                $dir = $parsed['orderDir'];
                usort($filtered, function ($a, $b) use ($col, $dir) {
                    $av = $a[$col] ?? '';
                    $bv = $b[$col] ?? '';
                    // Try numeric comparison first
                    if (is_numeric($av) && is_numeric($bv)) {
                        $cmp = (float)$av <=> (float)$bv;
                    } else {
                        $cmp = strcmp($av, $bv);
                    }
                    return $dir === 'DESC' ? -$cmp : $cmp;
                });
            }

            // Save total count before LIMIT for SQL_CALC_FOUND_ROWS
            if ($parsed['calcFoundRows'] ?? false) {
                $this->calcFoundRows = count($filtered);
            }

            // OFFSET + LIMIT
            if ($parsed['offset'] > 0 || $parsed['limit'] !== null) {
                $offset = $parsed['offset'];
                $length = $parsed['limit'] ?? count($filtered);
                $filtered = array_slice($filtered, $offset, $length);
            }

            // Project columns
            $results = [];
            foreach ($filtered as $data) {
                $projected = [];
                foreach ($data as $col => $val) {
                    if (in_array('*', $parsed['columns'], true)
                        || in_array($col, $parsed['columns'], true)) {
                        // Serialization safety: decode HTML entities + strip slashes
                        $projected[$col] = $val;
                    }
                }
                $results[] = (object) $projected;
            }

            return $results;
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
            if (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*[\'"](.*)[\'"]$/s', $expr, $m)) {
                $pairs[$m[1]] = stripslashes($m[2]);
            } elseif (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*(.+)$/s', $expr, $m)) {
                $pairs[$m[1]] = trim($m[2]);
            }
        }

        private function parseWhereEquality(string $clause): array
        {
            $conditions = [];
            if (trim($clause) === '') return $conditions;

            $clause = preg_replace('/\b[a-zA-Z0-9_]+\.([a-zA-Z0-9_]+)/', '$1', $clause);

            $parts = preg_split('/\s+AND\s+/i', $clause);
            foreach ($parts as $part) {
                $part = trim($part);
                if (preg_match('/^\d+\s*=\s*\d+$/', $part)) continue;
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
    }
}

// ---------------------------------------------------------------------------
// Global namespace — HtmlDatabase_WPDB adapter (extends wpdb)
// ---------------------------------------------------------------------------
namespace {

    use HtmlDatabase\Core\Configuration;
    use HtmlDatabase\Core\ShardedStorageManager;
    use HtmlDatabase\Parser\InsertTokenizer;
    use HtmlDatabase\Parser\MutationParser;
    use HtmlDatabase\Parser\SqlToXpathTranslator;

    class HtmlDatabase_WPDB extends wpdb
    {
        private ShardedStorageManager $storage;
        private SqlToXpathTranslator  $translator;
        private InsertTokenizer       $insertTokenizer;
        private MutationParser        $mutationParser;

        /** Per-request TX counter (used as fallback, prefer global TX). */
        private int $txCounter = 0;

        public function __construct(
            mixed $dbuser,
            mixed $dbpassword,
            mixed $dbname,
            mixed $dbhost
        ) {
            $this->show_errors();

            $storagePath = WP_CONTENT_DIR . '/html_db';
            $config      = new Configuration($storagePath);

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
            $supported = ['collation', 'group_concat', 'subqueries', 'set_charset', 'utf8mb4'];
            return is_string($db_cap) && in_array(strtolower($db_cap), $supported, true);
        }

        public function get_col_charset($table, $column) { return 'utf8mb4'; }
        public function get_col_length($table, $column) { return ['type' => 'byte', 'length' => 16777216]; }

        // -- Query router -----------------------------------------------------

        public function query($query)
        {
            if (!$this->ready) return false;

            $this->flush();

            // WordPress prepare() replaces literal % with a unique hash.
            $query = $this->remove_placeholder_escape($query);

            $this->last_query = $query;
            $this->num_queries++;
            $this->txCounter++;

            $clean = trim($query);
            $verb  = strtoupper(strtok($clean, " \t\n\r"));

            try {
                return match ($verb) {
                    'SELECT' => $this->handleSelect($clean),
                    'INSERT' => $this->handleInsert($clean),
                    'UPDATE' => $this->handleUpdate($clean),
                    'DELETE' => $this->handleDelete($clean),
                    'REPLACE' => $this->handleReplace($clean),
                    'CREATE', 'ALTER', 'DROP', 'TRUNCATE' => $this->handleDdl($clean),
                    'SET', 'START', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE' => true,
                    'SHOW'   => $this->handleShow($clean),
                    'DESCRIBE', 'DESC' => $this->handleDescribe($clean),
                    default  => true,
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

            // YEAR() / MONTH() aggregate: SELECT DISTINCT YEAR(col) AS y, MONTH(col) AS m FROM ...
            if (preg_match('/\bYEAR\s*\(/i', $sql) || preg_match('/\bMONTH\s*\(/i', $sql)) {
                return $this->handleDateFunctions($sql);
            }

            // GROUP BY + COUNT(*)
            if (preg_match('/GROUP\s+BY/i', $sql) && preg_match('/COUNT\s*\(\s*\*\s*\)/i', $sql)) {
                return $this->handleGroupByCount($sql);
            }

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

            // Strip date functions and DISTINCT, replace with SELECT * to get all rows
            $stripped = preg_replace('/SELECT\s+(DISTINCT\s+)?.*?\s+FROM/is', 'SELECT * FROM', $sql);
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

            // ORDER BY — inherit from parsed SQL (typically ORDER BY post_date DESC)
            // Results are already grouped, just preserve the order they appeared.

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

            // Simple UNION
            $parts = preg_split('/\bUNION\s+(ALL\s+)?/i', $sql);
            $allRows = [];
            foreach ($parts as $part) {
                $part = trim($part);
                if (stripos($part, 'SELECT') === 0) {
                    foreach ($this->translator->executeSelect($part) as $r) $allRows[] = $r;
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

        private function resolveColumnDefaults(string $table): array
        {
            foreach (self::COLUMN_DEFAULTS as $suffix => $defaults) {
                if ($table === $suffix || str_ends_with($table, '_' . $suffix)) return $defaults;
            }
            return [];
        }

        private function resolveAutoPkColumn(string $table): ?string
        {
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
                if ($pkIdx !== false && isset($rows[0][$pkIdx])) {
                    $firstGeneratedId = (int) $rows[0][$pkIdx];
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

            $this->insert_id = $firstGeneratedId ?: $this->txCounter;
            return $this->rows_affected;
        }

        // -- REPLACE ----------------------------------------------------------

        private function handleReplace(string $sql): int|false
        {
            $converted = preg_replace('/^REPLACE\s+/i', 'INSERT ', $sql);
            return $this->handleInsert($converted);
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

        private function handleDdl(string $sql): true { return true; }

        // -- SHOW / DESCRIBE --------------------------------------------------

        private function handleShow(string $sql): int
        {
            $this->last_result = [];
            $this->num_rows    = 0;

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
            return 0;
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
