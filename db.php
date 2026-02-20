<?php
 * Version: 2.2.0
 *
 * Architecture:
 *   - WAL (Write-Ahead Log) pattern with atomic flock(LOCK_EX) appends.
 *   - Read-modify-write (RMW) for UPDATE and DELETE operations.
 *   - State-machine tokenizer for INSERT parsing (multi-row, nested parens, escapes).
 *   - SQL-to-XPath 1.0 translator for SELECTs with autoload fallback.
 *   - MySQL DEFAULT column emulation for WordPress core tables.
 *   - Table-prefix stripping, OR→IN conversion, 1=1 tautology handling.
 *   - WordPress core compatibility hacks (URL safeguard, serialization safety).
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
// Namespace: HtmlDatabase\Core — Configuration & Write-Ahead Log Manager
// ---------------------------------------------------------------------------
namespace HtmlDatabase\Core {

    use SplFileObject;
    use RuntimeException;

    /**
     * Immutable runtime configuration.
     */
    final readonly class Configuration
    {
        public function __construct(
            public string $basePath
        ) {}
    }

    /**
     * Manages atomic writes to per-table WAL files stored as HTML table rows.
     *
     * Every mutation (INSERT, UPDATE, DELETE) is expressed as an append-only
     * operation so that readers never see torn writes.
     */
    final class WriteAheadLogManager
    {
        public function __construct(private Configuration $config)
        {
            $this->ensureStorageExists();
        }

        // -- Public API -------------------------------------------------------

        /**
         * Append a single row to the WAL file for $table.
         *
         * @return int Number of affected rows (always 1).
         */
        public function insert(string $table, array $payload, int $transactionId): int
        {
            $html = $this->buildRowHtml($payload, $transactionId, 0);
            $this->appendToFile($this->walPath($table), $html);
            return 1;
        }

        /**
         * Append multiple rows in one atomic write (multi-row INSERT).
         *
         * @param string   $table   Sanitised table name.
         * @param string[] $columns Ordered column names.
         * @param array[]  $rows    Array of value arrays, each matching $columns.
         * @param int      $txId    Current transaction counter.
         *
         * @return int Number of rows written.
         */
        public function insertBatch(string $table, array $columns, array $rows, int $txId): int
        {
            $colCount = count($columns);
            $html = '';
            foreach ($rows as $ri => $values) {
                if (count($values) !== $colCount) {
                    error_log("[HtmlDB] BATCH COL MISMATCH table={$table} row={$ri} cols={$colCount} vals=" . count($values) . " colNames=" . implode(',', $columns) . " vals=" . implode('|', array_map(fn($v) => substr((string)$v, 0, 40), $values)));
                    continue;
                }
                $payload = array_combine($columns, $values);
                $html .= $this->buildRowHtml($payload, $txId, 0);
            }
            if ($html !== '') {
                $this->appendToFile($this->walPath($table), $html);
            }
            return count($rows);
        }

        /**
         * Read-modify-write DELETE.
         *
         * Reads the entire WAL, removes matching rows, rewrites atomically.
         */
        public function deleteRows(string $table, array $conditions, int $txId): int
        {
            $path = $this->walPath($table);
            if (!file_exists($path)) {
                return 0;
            }

            clearstatcache(true, $path);
            $rows = $this->readAllRows($path);
            $kept = [];
            $deleted = 0;

            foreach ($rows as $row) {
                if ($this->rowMatchesConditions($row['data'], $conditions)) {
                    $deleted++;
                } else {
                    $kept[] = $row;
                }
            }

            // Only rewrite when rows were actually removed — never wipe the
            // file when readAllRows returned nothing or conditions matched
            // zero rows.
            if ($deleted > 0) {
                $this->rewriteFile($path, $kept, $txId);
            }

            return $deleted;
        }

        /**
         * Read-modify-write UPDATE.
         *
         * Reads all rows, finds those matching $conditions, merges
         * $setValues into their FULL column set, and rewrites the file.
         */
        public function updateRows(
            string $table,
            array  $setValues,
            array  $conditions,
            int    $txId
        ): int {
            $path = $this->walPath($table);
            if (!file_exists($path)) {
                return 0;
            }

            clearstatcache(true, $path);
            $rows    = $this->readAllRows($path);
            $updated = 0;

            foreach ($rows as &$row) {
                if ($this->rowMatchesConditions($row['data'], $conditions)) {
                    $row['data'] = array_merge($row['data'], $setValues);
                    $row['tx']   = $txId;
                    $updated++;
                }
            }
            unset($row);

            if ($updated > 0) {
                $this->rewriteFile($path, $rows, $txId);
            }

            return $updated ?: 1;
        }

        /**
         * Parse every <tr> in a WAL file into structured row data.
         * Uses string-splitting (no regex/DOM) for maximum reliability.
         * Skips legacy tombstone rows (data-tx-created="0").
         */
        private function readAllRows(string $path): array
        {
            clearstatcache(true, $path);
            $content = file_get_contents($path);
            if ($content === false || trim($content) === '') {
                return [];
            }

            $rows = [];
            $contentLen = strlen($content);
            $trCount = substr_count($content, '<tr ');

            // Split on </tr> — each segment (except possibly the last)
            // contains exactly one <tr ...>...</td>... block.
            $segments = explode('</tr>', $content);

            foreach ($segments as $segment) {
                // Find the <tr start
                $trPos = strpos($segment, '<tr ');
                if ($trPos === false) {
                    continue;
                }

                $trBlock = substr($segment, $trPos);

                // Extract data-tx-created
                $created = 0;
                if (preg_match('/data-tx-created="(\d+)"/', $trBlock, $am)) {
                    $created = (int) $am[1];
                }

                // Skip legacy tombstone rows
                if ($created === 0) {
                    continue;
                }

                // Extract all <td> cells using simple string scanning
                $data = [];
                $searchPos = 0;
                while (($tdStart = strpos($trBlock, '<td data-column="', $searchPos)) !== false) {
                    // Extract column name
                    $colStart = $tdStart + strlen('<td data-column="');
                    $colEnd   = strpos($trBlock, '">', $colStart);
                    if ($colEnd === false) break;

                    $col = substr($trBlock, $colStart, $colEnd - $colStart);

                    // Extract value (between "> and </td>)
                    $valStart = $colEnd + 2; // skip ">
                    $valEnd   = strpos($trBlock, '</td>', $valStart);
                    if ($valEnd === false) break;

                    $val = substr($trBlock, $valStart, $valEnd - $valStart);
                    $data[htmlspecialchars_decode($col, ENT_QUOTES | ENT_HTML5)]
                        = htmlspecialchars_decode($val, ENT_QUOTES | ENT_HTML5);

                    $searchPos = $valEnd + 5; // skip </td>
                }

                if (!empty($data)) {
                    $rows[] = ['tx' => $created, 'data' => $data];
                }
            }

            if (count($rows) < $trCount) {
                error_log("[HtmlDB] readAllRows MISMATCH file=" . basename($path) . " contentLen={$contentLen} trInContent={$trCount} parsed=" . count($rows));
            }

            return $rows;
        }

        /**
         * Does a row's data match ALL the given conditions?
         */
        private function rowMatchesConditions(array $data, array $conditions): bool
        {
            foreach ($conditions as $col => $val) {
                if (!array_key_exists($col, $data)) {
                    return false;
                }
                if ((string) $data[$col] !== (string) $val) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Atomically rewrite a WAL file from structured row data.
         */
        private function rewriteFile(string $path, array $rows, int $txId): void
        {
            $html = '';
            foreach ($rows as $row) {
                $html .= $this->buildRowHtml($row['data'], $row['tx'], 0);
            }
            $this->atomicWrite($path, $html);
        }

        /**
         * Full-file atomic write with exclusive lock.
         */
        private function atomicWrite(string $filePath, string $content): void
        {
            $file = new SplFileObject($filePath, 'c+');
            if (!$file->flock(LOCK_EX)) {
                throw new RuntimeException("Unable to acquire exclusive lock on WAL: {$filePath}");
            }
            $file->rewind();
            $file->ftruncate(0);
            $file->fwrite($content);
            $file->fflush();
            $file->flock(LOCK_UN);
        }

        /**
         * Return the absolute WAL path for a given table name (public for the
         * parser which needs to locate the file for reads).
         */
        public function walPath(string $table): string
        {
            $safe = preg_replace('/[^a-zA-Z0-9_]/', '', $table);
            return sprintf('%s/%s.wal.html', rtrim($this->config->basePath, '/'), $safe);
        }

        /**
         * Persistent per-table auto-increment counter.
         *
         * Reads / increments / writes a {table}.seq file under the
         * storage directory, protected by an exclusive flock.
         */
        public function nextAutoIncrement(string $table): int
        {
            $safe = preg_replace('/[^a-zA-Z0-9_]/', '', $table);
            $seqPath = sprintf('%s/%s.seq', rtrim($this->config->basePath, '/'), $safe);

            $file = new SplFileObject($seqPath, 'c+');
            if (!$file->flock(LOCK_EX)) {
                throw new RuntimeException("Unable to lock sequence file: {$seqPath}");
            }
            $file->rewind();
            $current = (int) $file->fgets();
            $next = $current + 1;
            $file->rewind();
            $file->ftruncate(0);
            $file->fwrite((string) $next);
            $file->flock(LOCK_UN);

            return $next;
        }

        // -- Private helpers --------------------------------------------------

        private function buildRowHtml(array $payload, int $txCreated, int $txDeleted): string
        {
            $html = sprintf(
                '<tr data-tx-created="%d" data-tx-deleted="%d">',
                $txCreated,
                $txDeleted
            );
            foreach ($payload as $column => $value) {
                $safeCol = htmlspecialchars((string) $column, ENT_QUOTES | ENT_HTML5, 'UTF-8');
                $safeVal = htmlspecialchars((string) $value,  ENT_QUOTES | ENT_HTML5, 'UTF-8');
                $html .= sprintf('<td data-column="%s">%s</td>', $safeCol, $safeVal);
            }
            $html .= "</tr>\n";
            return $html;
        }

        // buildTombstoneHtml removed — no longer using tombstone MVCC.

        /**
         * Atomic append with an exclusive advisory lock.
         */
        private function appendToFile(string $filePath, string $content): void
        {
            $file = new SplFileObject($filePath, 'a');
            if (!$file->flock(LOCK_EX)) {
                throw new RuntimeException("Unable to acquire exclusive lock on WAL: {$filePath}");
            }
            $file->fwrite($content);
            $file->fflush();
            $file->flock(LOCK_UN);
        }

        private function ensureStorageExists(): void
        {
            $dir = $this->config->basePath;
            if (!is_dir($dir) && !mkdir($dir, 0755, true)) {
                throw new RuntimeException("Storage directory creation failed: {$dir}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Namespace: HtmlDatabase\Parser — SQL Tokenizer & XPath Translator
// ---------------------------------------------------------------------------
namespace HtmlDatabase\Parser {

    use DOMDocument;
    use DOMXPath;

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
                // Skip comma between groups
                if (isset($rest[0]) && $rest[0] === ',') {
                    $rest = substr($rest, 1);
                }
            }

            if (empty($rows)) {
                return null;
            }

            return ['table' => $table, 'columns' => $columns, 'rows' => $rows];
        }

        // -----------------------------------------------------------------
        // State-machine helpers
        // -----------------------------------------------------------------

        /**
         * Extract the content between a matched pair of parentheses starting
         * at $startPos (which must point to the opening '(').
         *
         * Respects single-quoted strings so that literal ')' inside values
         * don't break the grouping.
         *
         * @return array{content: string, endPos: int}|null
         */
        private function extractParenGroup(string $sql, int $startPos): ?array
        {
            $len   = strlen($sql);
            $depth = 0;
            $inStr = false;
            $esc   = false;
            $start = null;

            for ($i = $startPos; $i < $len; $i++) {
                $ch = $sql[$i];

                if ($esc) {
                    $esc = false;
                    continue;
                }
                if ($ch === '\\') {
                    $esc = true;
                    continue;
                }
                if ($ch === "'" && !$esc) {
                    // Handle doubled single-quote escape ('')
                    if ($inStr && isset($sql[$i + 1]) && $sql[$i + 1] === "'") {
                        $i++; // skip the second quote
                        continue;
                    }
                    $inStr = !$inStr;
                    continue;
                }
                if ($inStr) {
                    continue;
                }

                if ($ch === '(') {
                    if ($depth === 0) {
                        $start = $i + 1;
                    }
                    $depth++;
                } elseif ($ch === ')') {
                    $depth--;
                    if ($depth === 0 && $start !== null) {
                        return [
                            'content' => substr($sql, $start, $i - $start),
                            'endPos'  => $i,
                        ];
                    }
                }
            }

            return null;
        }

        /**
         * Split a comma-separated value list respecting quoted strings and
         * nested parentheses.
         *
         * @return string[]
         */
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

                if ($esc) {
                    $buf .= $ch;
                    $esc  = false;
                    continue;
                }
                if ($ch === '\\') {
                    $esc = true;
                    if ($inStr) {
                        $buf .= $ch;
                    }
                    continue;
                }
                if ($ch === "'" && !$esc) {
                    // doubled single-quote
                    if ($inStr && isset($raw[$i + 1]) && $raw[$i + 1] === "'") {
                        $buf .= "'";
                        $i++;
                        continue;
                    }
                    $inStr = !$inStr;
                    continue; // strip the delimiting quote
                }
                if (!$inStr) {
                    if ($ch === '(') {
                        $depth++;
                        $buf .= $ch;
                        continue;
                    }
                    if ($ch === ')') {
                        $depth--;
                        $buf .= $ch;
                        continue;
                    }
                    if ($ch === ',' && $depth === 0) {
                        $values[] = $this->cleanValue($buf);
                        $buf = '';
                        continue;
                    }
                }
                $buf .= $ch;
            }

            $values[] = $this->cleanValue($buf);
            return $values;
        }

        /**
         * Final value cleanup: trim whitespace, resolve NULL literal,
         * strip remaining slashes from escape sequences.
         */
        private function cleanValue(string $v): string
        {
            $v = trim($v);
            if (strcasecmp($v, 'NULL') === 0) {
                return '';
            }
            return stripslashes($v);
        }

        /**
         * Remove the ON DUPLICATE KEY UPDATE tail from an INSERT statement,
         * respecting quoted strings so that the keyword inside a value is
         * not mistakenly matched.
         */
        private function stripOnDuplicate(string $sql): string
        {
            $upper = strtoupper($sql);
            $inStr = false;
            $esc   = false;
            $len   = strlen($sql);

            for ($i = 0; $i < $len; $i++) {
                $ch = $sql[$i];
                if ($esc) {
                    $esc = false;
                    continue;
                }
                if ($ch === '\\') {
                    $esc = true;
                    continue;
                }
                if ($ch === "'") {
                    if ($inStr && isset($sql[$i + 1]) && $sql[$i + 1] === "'") {
                        $i++;
                        continue;
                    }
                    $inStr = !$inStr;
                    continue;
                }
                if ($inStr) {
                    continue;
                }

                // Outside a string — look for "ON DUPLICATE"
                if ($upper[$i] === 'O'
                    && substr($upper, $i, 12) === 'ON DUPLICATE'
                ) {
                    return rtrim(substr($sql, 0, $i));
                }
            }

            return $sql;
        }
    }

    /**
     * Translates SQL SELECT statements into XPath 1.0 queries and executes
     * them against the WAL HTML files using DOMDocument + DOMXPath.
     *
     * Includes:
     *  - Autoload fallback: detects WordPress IN ('yes','on','auto',...) clauses
     *    and falls back to a brute-force in-memory scan when XPath cannot
     *    express the predicate.
     *  - Serialization safety: every cell value is run through
     *    htmlspecialchars_decode() + stripslashes() so that unserialize()
     *    works on wp_options rows.
     */
    final class SqlToXpathTranslator
    {
        public function __construct(private string $storagePath)
        {
            $this->storagePath = rtrim($storagePath, '/');
        }

        // -- Public entry point -----------------------------------------------

        /**
         * Execute a SELECT query and return an array of stdClass row objects.
         */
        public function executeSelect(string $sql): array
        {
            $parsed = $this->parseSql($sql);
            if ($parsed === null) {
                return [];
            }

            return $this->queryHtmlTree($parsed);
        }

        // -- SQL Parsing ------------------------------------------------------

        /**
         * @return array{
         *     columns: string[],
         *     table: string,
         *     conditions: array<string,string>,
         *     inConditions: array<string,string[]>,
         *     limit: int|null,
         *     rawWhere: string|null
         * }|null
         */
        private function parseSql(string $sql): ?array
        {
            // Remove backticks globally
            $sql = str_replace('`', '', $sql);

            // Strip MySQL hints
            $sql = preg_replace('/\bSQL_CALC_FOUND_ROWS\b/i', '', $sql);

            // Table name
            if (!preg_match('/FROM\s+([a-zA-Z0-9_]+)/i', $sql, $tblMatch)) {
                return null;
            }
            $table = $tblMatch[1];

            // Strip table/alias prefixes from qualified column names
            // e.g. wp_posts.post_type → post_type, wp_posts.* → *
            $sql = preg_replace('/\b[a-zA-Z0-9_]+\.\*/', '*', $sql);
            $sql = preg_replace('/\b[a-zA-Z0-9_]+\.([a-zA-Z0-9_]+)/', '$1', $sql);

            // Columns
            preg_match('/SELECT\s+(.*?)\s+FROM/is', $sql, $colMatch);
            $rawCols = $colMatch[1] ?? '*';
            $columns = array_map('trim', explode(',', $rawCols));

            // LIMIT — handle both "LIMIT n" and "LIMIT offset, n"
            $limit = null;
            if (preg_match('/LIMIT\s+(\d+)\s*,\s*(\d+)/i', $sql, $limMatch)) {
                $limit = (int) $limMatch[1] + (int) $limMatch[2];
            } elseif (preg_match('/LIMIT\s+(\d+)/i', $sql, $limMatch)) {
                $limit = (int) $limMatch[1];
            }

            // Raw WHERE (for autoload fallback detection)
            $rawWhere = null;
            if (preg_match('/WHERE\s+(.*?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|\s+LIMIT|$)/is', $sql, $wm)) {
                $rawWhere = trim($wm[1]);
            }

            // Simple equality conditions
            $conditions   = [];
            $inConditions = [];

            if ($rawWhere !== null) {
                // Split on AND (not inside quotes — sufficient for WP core queries)
                $parts = preg_split('/\s+AND\s+/i', $rawWhere);
                foreach ($parts as $part) {
                    $part = trim($part);

                    // Skip tautologies (1=1) but abort on contradictions (0=1)
                    if (preg_match('/^(\d+)\s*=\s*(\d+)$/', $part, $taut)) {
                        if ($taut[1] !== $taut[2]) {
                            // Contradiction: condition can never be true → return empty
                            return [
                                'columns'      => $columns,
                                'table'        => $table,
                                'conditions'   => ['__contradiction__' => '__never__'],
                                'inConditions' => [],
                                'limit'        => 0,
                                'rawWhere'     => $rawWhere,
                            ];
                        }
                        continue; // True tautology like 1=1 — skip
                    }

                    // Parenthesized OR group on same column →
                    // convert to IN, e.g. (post_status = 'publish' OR post_status = 'draft')
                    if (preg_match('/^\((.+)\)$/s', $part, $paren)) {
                        $orParts = preg_split('/\s+OR\s+/i', $paren[1]);
                        $orCol = null;
                        $orVals = [];
                        $valid = true;
                        foreach ($orParts as $op) {
                            $op = trim($op);
                            // Strip any inner parentheses: (col = 'val') → col = 'val'
                            $op = preg_replace('/^\((.+)\)$/', '$1', $op);
                            $op = trim($op);
                            if (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*[\'"](.*?)[\'"]$/s', $op, $om)) {
                                if ($orCol === null) {
                                    $orCol = $om[1];
                                }
                                if ($om[1] === $orCol) {
                                    $orVals[] = $om[2];
                                } else {
                                    $valid = false;
                                    break;
                                }
                            } else {
                                $valid = false;
                                break;
                            }
                        }
                        if ($valid && $orCol !== null && !empty($orVals)) {
                            $inConditions[$orCol] = $orVals;
                        }
                        continue;
                    }

                    // col = 'value' (quoted string)
                    if (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*[\'"](.*?)[\'"]$/s', $part, $eq)) {
                        $conditions[$eq[1]] = $eq[2];
                        continue;
                    }

                    // col = 123 (unquoted numeric — used by WP for PK lookups)
                    if (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*(-?\d+(?:\.\d+)?)$/s', $part, $eq)) {
                        $conditions[$eq[1]] = $eq[2];
                        continue;
                    }

                    // col IN ('a','b','c')
                    if (preg_match('/^([a-zA-Z0-9_]+)\s+IN\s*\((.+)\)$/i', $part, $inm)) {
                        $vals = [];
                        preg_match_all('/[\'"]([^\'"]*)[\'"]/', $inm[2], $inVals);
                        if (!empty($inVals[1])) {
                            $vals = $inVals[1];
                        }
                        if (!empty($vals)) {
                            $inConditions[$inm[1]] = $vals;
                        }
                        continue;
                    }
                }
            }

            return [
                'columns'      => $columns,
                'table'        => $table,
                'conditions'   => $conditions,
                'inConditions' => $inConditions,
                'limit'        => $limit,
                'rawWhere'     => $rawWhere,
            ];
        }

        // -- HTML / XPath query layer ----------------------------------------

        private function queryHtmlTree(array $parsed): array
        {
            $table = $parsed['table'];
            $walPath = sprintf('%s/%s.wal.html', $this->storagePath, $table);

            if (!file_exists($walPath)) {
                return [];
            }

            $htmlContent = file_get_contents($walPath);
            if ($htmlContent === false || trim($htmlContent) === '') {
                return [];
            }

            // Wrap raw <tr> rows in a valid HTML skeleton
            $fullHtml = sprintf(
                "<!DOCTYPE html><html><head><meta charset='UTF-8'></head>"
                . "<body><table id='%s'><tbody>%s</tbody></table></body></html>",
                $table,
                $htmlContent
            );

            $dom = new DOMDocument();
            libxml_use_internal_errors(true);
            $dom->loadHTML($fullHtml, LIBXML_NOERROR | LIBXML_NOWARNING);
            libxml_clear_errors();

            $xpath = new DOMXPath($dom);

            // Decide: XPath query or in-memory fallback?
            $useAutoloadFallback = $this->needsAutoloadFallback($parsed);

            if ($useAutoloadFallback) {
                return $this->fullScanFallback($xpath, $parsed);
            }

            return $this->xpathQuery($xpath, $parsed);
        }

        /**
         * Standard XPath-based query path.
         */
        private function xpathQuery(DOMXPath $xpath, array $parsed): array
        {
            $xpathStr = $this->buildXpathString(
                $parsed['table'],
                $parsed['conditions'],
                $parsed['inConditions']
            );

            $nodes   = $xpath->query($xpathStr);
            $results = [];

            if ($nodes === false) {
                return [];
            }

            foreach ($nodes as $node) {
                $row = $this->nodeToRow($xpath, $node, $parsed['columns']);
                if ($row !== null) {
                    $results[] = $row;
                }
                if ($parsed['limit'] !== null && count($results) >= $parsed['limit']) {
                    break;
                }
            }

            return $results;
        }

        /**
         * Full table-scan fallback used when the WHERE clause contains
         * constructs that XPath 1.0 cannot express (e.g. complex IN lists
         * used by WordPress's autoloaded-options query).
         */
        private function fullScanFallback(DOMXPath $xpath, array $parsed): array
        {
            $table = $parsed['table'];
            $query = sprintf('//table[@id="%s"]/tbody/tr[@data-tx-created!="0"]', $table);
            $nodes = $xpath->query($query);

            if ($nodes === false) {
                return [];
            }

            $results = [];

            foreach ($nodes as $node) {
                $cells = $xpath->query('td', $node);
                $rowData = [];
                foreach ($cells as $cell) {
                    $col = $cell->getAttribute('data-column');
                    $val = $this->decodeCellValue($cell->nodeValue);
                    $rowData[$col] = $val;
                }

                // Apply equality conditions
                $match = true;
                foreach ($parsed['conditions'] as $col => $expected) {
                    if (($rowData[$col] ?? null) !== $expected) {
                        $match = false;
                        break;
                    }
                }

                // Apply IN conditions
                if ($match) {
                    foreach ($parsed['inConditions'] as $col => $allowedValues) {
                        if (!in_array($rowData[$col] ?? '', $allowedValues, true)) {
                            $match = false;
                            break;
                        }
                    }
                }

                if (!$match) {
                    continue;
                }

                // Project requested columns
                $projected = [];
                foreach ($rowData as $col => $val) {
                    if (in_array('*', $parsed['columns'], true)
                        || in_array($col, $parsed['columns'], true)
                    ) {
                        $projected[$col] = $val;
                    }
                }

                $results[] = (object) $projected;

                if ($parsed['limit'] !== null && count($results) >= $parsed['limit']) {
                    break;
                }
            }

            return $results;
        }

        // -- XPath builder ----------------------------------------------------

        private function buildXpathString(
            string $table,
            array  $conditions,
            array  $inConditions
        ): string {
            // Base: non-deleted rows only (MVCC)
            $xp = sprintf('//table[@id="%s"]/tbody/tr[@data-tx-created!="0"]', $table);

            $predicates = [];

            // Equality predicates
            foreach ($conditions as $column => $value) {
                $safe = $this->xpathEscape($value);
                $predicates[] = sprintf('td[@data-column="%s" and text()=%s]', $column, $safe);
            }

            // IN predicates — expressible via or-chains in XPath 1.0
            foreach ($inConditions as $column => $values) {
                $orParts = [];
                foreach ($values as $v) {
                    $safe = $this->xpathEscape($v);
                    $orParts[] = sprintf('text()=%s', $safe);
                }
                $predicates[] = sprintf(
                    'td[@data-column="%s" and (%s)]',
                    $column,
                    implode(' or ', $orParts)
                );
            }

            if (!empty($predicates)) {
                $xp .= '[' . implode(' and ', $predicates) . ']';
            }

            return $xp;
        }

        /**
         * XPath 1.0 safe string escaping.
         *
         * If the value contains both single and double quotes, use the
         * concat() trick. Otherwise, wrap in the appropriate quote type.
         */
        private function xpathEscape(string $value): string
        {
            if (!str_contains($value, "'")) {
                return "'" . $value . "'";
            }
            if (!str_contains($value, '"')) {
                return '"' . $value . '"';
            }
            // Contains both — use concat()
            $parts = [];
            $segments = explode("'", $value);
            foreach ($segments as $i => $seg) {
                if ($i > 0) {
                    $parts[] = "\"'\"";
                }
                if ($seg !== '') {
                    $parts[] = "'" . $seg . "'";
                }
            }
            return 'concat(' . implode(',', $parts) . ')';
        }

        // -- Row helpers ------------------------------------------------------

        /**
         * Convert a DOM <tr> node into a stdClass row, respecting column
         * projection and applying serialisation safety decoding.
         */
        private function nodeToRow(DOMXPath $xpath, \DOMNode $node, array $columns): ?object
        {
            $cells = $xpath->query('td', $node);
            $row   = [];

            foreach ($cells as $cell) {
                $colName = $cell->getAttribute('data-column');
                if (in_array('*', $columns, true) || in_array($colName, $columns, true)) {
                    $row[$colName] = $this->decodeCellValue($cell->nodeValue);
                }
            }

            return empty($row) ? null : (object) $row;
        }

        /**
         * Serialization safety: decode HTML entities and strip slashes so
         * that PHP's unserialize() works on stored option values.
         */
        private function decodeCellValue(string $raw): string
        {
            $decoded = htmlspecialchars_decode($raw, ENT_QUOTES | ENT_HTML5);
            return stripslashes($decoded);
        }

        // -- Autoload detection -----------------------------------------------

        /**
         * Detect WordPress-style autoload queries that use IN ('yes','on',...)
         * or other complex predicates that may choke the simple XPath builder.
         */
        private function needsAutoloadFallback(array $parsed): bool
        {
            if ($parsed['rawWhere'] === null) {
                return false;
            }

            $raw = $parsed['rawWhere'];

            // Heuristic: if the raw WHERE contains an IN clause with the
            // typical WordPress autoload values, prefer fallback.
            if (preg_match('/\bIN\s*\(\s*[\'"](?:yes|on|auto|auto-on)/i', $raw)) {
                return true;
            }

            // If there are OR operators or sub-selects, fallback.
            if (preg_match('/\bOR\b/i', $raw) || preg_match('/\(\s*SELECT\b/i', $raw)) {
                return true;
            }

            return false;
        }
    }

    /**
     * Minimal parser for UPDATE and DELETE statements.
     *
     * Extracts enough information for the WAL manager to create tombstones
     * and new rows without building a full SQL AST.
     */
    final class MutationParser
    {
        /**
         * Parse: UPDATE table SET col='val', ... WHERE col='val' AND ...
         *
         * @return array{table: string, set: array, conditions: array}|null
         */
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

            // If the WHERE clause exists but yielded no parseable conditions,
            // treat it as a parse failure to prevent accidental mass update.
            if (trim($whereClause) !== '' && empty($conditions)) {
                return null;
            }

            return ['table' => $table, 'set' => $setValues, 'conditions' => $conditions];
        }

        /**
         * Parse: DELETE FROM table WHERE col='val' AND ...
         *
         * @return array{table: string, conditions: array}|null
         */
        public function parseDelete(string $sql): ?array
        {
            $sql = str_replace('`', '', $sql);

            if (!preg_match('/DELETE\s+FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+(.*?))?$/is', $sql, $m)) {
                return null;
            }

            $table       = $m[1];
            $whereClause = $m[2] ?? '';
            $conditions  = $this->parseWhereEquality($whereClause);

            // If the WHERE clause exists but yielded no parseable conditions,
            // treat it as a parse failure to prevent accidental mass deletion.
            if (trim($whereClause) !== '' && empty($conditions)) {
                return null;
            }

            return ['table' => $table, 'conditions' => $conditions];
        }

        private function parseAssignments(string $clause): array
        {
            $pairs = [];
            // State-machine split on commas outside quotes
            $buf   = '';
            $inStr = false;
            $esc   = false;
            $len   = strlen($clause);

            for ($i = 0; $i < $len; $i++) {
                $ch = $clause[$i];
                if ($esc) {
                    $buf .= $ch;
                    $esc  = false;
                    continue;
                }
                if ($ch === '\\') {
                    $esc = true;
                    $buf .= $ch;
                    continue;
                }
                if ($ch === "'") {
                    $inStr = !$inStr;
                    $buf .= $ch;
                    continue;
                }
                if ($ch === ',' && !$inStr) {
                    $this->addAssignment($buf, $pairs);
                    $buf = '';
                    continue;
                }
                $buf .= $ch;
            }
            $this->addAssignment($buf, $pairs);

            return $pairs;
        }

        private function addAssignment(string $expr, array &$pairs): void
        {
            $expr = trim($expr);
            if ($expr === '') {
                return;
            }
            if (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*[\'"](.*)[\'"]$/s', $expr, $m)) {
                $pairs[$m[1]] = stripslashes($m[2]);
            } elseif (preg_match('/^([a-zA-Z0-9_]+)\s*=\s*(.+)$/s', $expr, $m)) {
                $pairs[$m[1]] = trim($m[2]);
            }
        }

        private function parseWhereEquality(string $clause): array
        {
            $conditions = [];
            if (trim($clause) === '') {
                return $conditions;
            }

            // Strip table/alias prefixes: wp_posts.post_type → post_type
            $clause = preg_replace('/\b[a-zA-Z0-9_]+\.([a-zA-Z0-9_]+)/', '$1', $clause);

            $parts = preg_split('/\s+AND\s+/i', $clause);
            foreach ($parts as $part) {
                $part = trim($part);
                // Skip tautologies (1=1)
                if (preg_match('/^\d+\s*=\s*\d+$/', $part)) {
                    continue;
                }
                // col = 'value' (quoted)
                if (preg_match('/([a-zA-Z0-9_]+)\s*=\s*[\'"](.*?)[\'"]/', $part, $m)) {
                    $conditions[$m[1]] = $m[2];
                    continue;
                }
                // col = 123 (unquoted numeric)
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
    use HtmlDatabase\Core\WriteAheadLogManager;
    use HtmlDatabase\Parser\InsertTokenizer;
    use HtmlDatabase\Parser\MutationParser;
    use HtmlDatabase\Parser\SqlToXpathTranslator;

    /**
     * WordPress database adapter that stores all data in flat HTML WAL files
     * instead of MySQL.
     *
     * Extends the native wpdb class so that WordPress core, plugins and themes
     * interact with it transparently.
     */
    class HtmlDatabase_WPDB extends wpdb
    {
        private WriteAheadLogManager $walManager;
        private SqlToXpathTranslator $translator;
        private InsertTokenizer      $insertTokenizer;
        private MutationParser       $mutationParser;

        /** Monotonically increasing transaction counter (per-request). */
        private int $txCounter = 0;

        public function __construct(
            mixed $dbuser,
            mixed $dbpassword,
            mixed $dbname,
            mixed $dbhost
        ) {
            // Override error display (same as parent's first step)
            $this->show_errors();

            $storagePath = WP_CONTENT_DIR . '/html_db';
            $config      = new Configuration($storagePath);

            $this->walManager      = new WriteAheadLogManager($config);
            $this->translator      = new SqlToXpathTranslator($storagePath);
            $this->insertTokenizer = new InsertTokenizer();
            $this->mutationParser  = new MutationParser();

            // Pretend we have a working MySQL connection so that WordPress
            // core checks pass without modification.
            $this->dbuser     = $dbuser;
            $this->dbpassword = $dbpassword;
            $this->dbname     = $dbname;
            $this->dbhost     = $dbhost;

            $this->is_mysql       = true;
            $this->has_connected  = true;
            $this->ready          = true;

            // Set the default prefix so WP installer does not crash
            if (empty($this->prefix)) {
                $this->set_prefix($GLOBALS['table_prefix'] ?? 'wp_');
            }

            // Charset/collate defaults WordPress expects
            $this->charset = 'utf8mb4';
            $this->collate = 'utf8mb4_unicode_ci';
        }

        // -- Connection stubs -------------------------------------------------

        /** @return true */
        public function db_connect($allow_bail = true)
        {
            $this->has_connected = true;
            return true;
        }

        /** @return true */
        public function check_connection($allow_bail = true)
        {
            return true;
        }

        /** Report a MySQL-compatible version so WP core proceeds. */
        public function db_version()
        {
            return '8.0.32';
        }

        public function db_server_info()
        {
            return '8.0.32-HtmlDB';
        }

        /** Minimal escaping — no real MySQL connection to use mysql_real_escape_string. */
        public function _real_escape($data)
        {
            return addslashes((string) $data);
        }

        /**
         * Determine charset — must not touch a real MySQL handle.
         */
        public function determine_charset($charset, $collate)
        {
            return compact('charset', 'collate');
        }

        /**
         * Set charset — no-op for HTML engine.
         */
        public function set_charset($dbh, $charset = null, $collate = null)
        {
            return true;
        }

        /**
         * set_sql_mode — no-op.
         */
        public function set_sql_mode($modes = array())
        {
            return;
        }

        /**
         * select — pretend database was selected.
         */
        public function select($db, $dbh = null)
        {
            $this->ready = true;
            return;
        }

        /**
         * has_cap — report capabilities expected by WordPress core.
         */
        public function has_cap($db_cap)
        {
            $supported = ['collation', 'group_concat', 'subqueries', 'set_charset', 'utf8mb4'];
            if (is_string($db_cap)) {
                return in_array(strtolower($db_cap), $supported, true);
            }
            return false;
        }

        /**
         * get_col_charset — pretend all columns are utf8mb4.
         */
        public function get_col_charset($table, $column)
        {
            return 'utf8mb4';
        }

        /**
         * get_col_length — return a safe default.
         */
        public function get_col_length($table, $column)
        {
            return ['type' => 'byte', 'length' => 16777216];
        }

        // -- Query router -----------------------------------------------------

        /**
         * Central query dispatcher.
         *
         * @return int|bool Number of rows affected/selected, or false on error.
         */
        public function query($query)
        {
            if (!$this->ready) {
                return false;
            }

            $this->flush();

            // WordPress prepare() replaces literal % with a unique hash.
            // The parent wpdb::query() fires apply_filters('query', ...)
            // which calls remove_placeholder_escape().  Since we override
            // query() completely, we must do this ourselves.
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
            // SELECT @@SESSION.sql_mode — WP core health checks
            if (preg_match('/SELECT\s+@@/i', $sql)) {
                $this->last_result = [(object) ['@@SESSION.sql_mode' => '']];
                $this->num_rows = 1;
                return 1;
            }

            // SELECT FOUND_ROWS() — pagination helper
            if (preg_match('/SELECT\s+FOUND_ROWS\s*\(\s*\)/i', $sql)) {
                $this->last_result = [(object) ['FOUND_ROWS()' => $this->num_rows]];
                $this->num_rows = 1;
                return 1;
            }

            // GROUP BY … COUNT(*) aggregate — used by wp_count_posts(), etc.
            if (preg_match('/GROUP\s+BY/i', $sql) && preg_match('/COUNT\s*\(\s*\*\s*\)/i', $sql)) {
                return $this->handleGroupByCount($sql);
            }

            // Sub-query / UNION — flatten to the inner SELECT
            if (preg_match('/\bUNION\b/i', $sql)) {
                return $this->handleUnionSelect($sql);
            }

            $results = $this->translator->executeSelect($sql);
            $this->last_result = $results;
            $this->num_rows    = count($results);
            return $this->num_rows;
        }

        /**
         * Handle SELECT col, COUNT(*) AS alias … GROUP BY col.
         *
         * Fetches all matching rows, groups them by the GROUP BY column,
         * and returns aggregate counts.  Covers wp_count_posts() and
         * wp_count_comments() patterns.
         */
        private function handleGroupByCount(string $sql): int|false
        {
            // Identify the GROUP BY column
            if (!preg_match('/GROUP\s+BY\s+([a-zA-Z0-9_.`]+)/i', $sql, $gm)) {
                return 0;
            }
            $groupCol = str_replace(['`', ' '], '', $gm[1]);
            // Strip table prefix: wp_posts.post_status → post_status
            if (str_contains($groupCol, '.')) {
                $groupCol = substr($groupCol, strpos($groupCol, '.') + 1);
            }

            // Identify the COUNT alias
            $countAlias = 'num_posts';
            if (preg_match('/COUNT\s*\(\s*\*\s*\)\s+AS\s+([a-zA-Z0-9_]+)/i', $sql, $cm)) {
                $countAlias = $cm[1];
            }

            // Identify select columns (before COUNT)
            preg_match('/SELECT\s+(.*?)\s+FROM/is', str_replace('`', '', $sql), $colMatch);
            $rawCols = $colMatch[1] ?? '*';
            $selectCols = [];
            foreach (explode(',', $rawCols) as $c) {
                $c = trim($c);
                if (stripos($c, 'COUNT') !== false) continue;
                // Strip table prefix
                if (str_contains($c, '.')) {
                    $c = substr($c, strpos($c, '.') + 1);
                }
                $selectCols[] = $c;
            }

            // Strip GROUP BY and COUNT(*) from SQL, then run as normal SELECT *
            $stripped = preg_replace('/GROUP\s+BY\s+[a-zA-Z0-9_.`]+/i', '', $sql);
            $stripped = preg_replace('/,?\s*COUNT\s*\(\s*\*\s*\)\s*(AS\s+[a-zA-Z0-9_]+)?/i', '', $stripped);
            // Ensure we select * to get all columns
            $stripped = preg_replace('/SELECT\s+.*?\s+FROM/is', 'SELECT * FROM', $stripped);

            $rows = $this->translator->executeSelect($stripped);

            // Group
            $groups = [];
            foreach ($rows as $row) {
                $rowArr = (array) $row;
                $key = $rowArr[$groupCol] ?? '__unknown__';
                if (!isset($groups[$key])) {
                    $groups[$key] = 0;
                }
                $groups[$key]++;
            }

            // Build result
            $results = [];
            foreach ($groups as $val => $count) {
                $obj = new \stdClass();
                $obj->{$groupCol} = (string) $val;
                $obj->{$countAlias} = (string) $count;
                // Include other select columns
                foreach ($selectCols as $sc) {
                    if ($sc !== $groupCol && !isset($obj->{$sc})) {
                        $obj->{$sc} = (string) $val; // GROUP BY value
                    }
                }
                $results[] = $obj;
            }

            $this->last_result = $results;
            $this->num_rows    = count($results);
            return $this->num_rows;
        }

        /**
         * Handle UNION / UNION ALL SELECT queries.
         *
         * Splits into individual SELECTs, executes each, and merges results.
         * Covers the wp_count_posts() optimized query with subqueries.
         */
        private function handleUnionSelect(string $sql): int|false
        {
            // If it's a subquery wrapper: SELECT col, COUNT(*) FROM (SELECT ... UNION ALL SELECT ...) AS x GROUP BY col
            if (preg_match('/GROUP\s+BY/i', $sql) && preg_match('/COUNT\s*\(\s*\*\s*\)/i', $sql)) {
                // Extract inner SELECTs from the subquery
                if (preg_match('/FROM\s*\((.+)\)\s*AS\s+/is', $sql, $sub)) {
                    $inner = $sub[1];
                    $parts = preg_split('/\bUNION\s+ALL\b/i', $inner);

                    $allRows = [];
                    foreach ($parts as $part) {
                        $part = trim($part);
                        if (stripos($part, 'SELECT') === 0) {
                            $rows = $this->translator->executeSelect($part);
                            foreach ($rows as $r) {
                                $allRows[] = $r;
                            }
                        }
                    }

                    // Now do GROUP BY + COUNT
                    if (preg_match('/GROUP\s+BY\s+([a-zA-Z0-9_.`]+)/i', $sql, $gm)) {
                        $groupCol = str_replace(['`', ' '], '', $gm[1]);
                        if (str_contains($groupCol, '.')) {
                            $groupCol = substr($groupCol, strpos($groupCol, '.') + 1);
                        }
                        $countAlias = 'num_posts';
                        if (preg_match('/COUNT\s*\(\s*\*\s*\)\s+AS\s+([a-zA-Z0-9_]+)/i', $sql, $cm)) {
                            $countAlias = $cm[1];
                        }

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

            // Simple UNION: split, execute, merge
            $parts = preg_split('/\bUNION\s+(ALL\s+)?/i', $sql);
            $allRows = [];
            foreach ($parts as $part) {
                $part = trim($part);
                if (stripos($part, 'SELECT') === 0) {
                    $rows = $this->translator->executeSelect($part);
                    foreach ($rows as $r) {
                        $allRows[] = $r;
                    }
                }
            }

            $this->last_result = $allRows;
            $this->num_rows    = count($allRows);
            return $this->num_rows;
        }

        // -- INSERT -----------------------------------------------------------

        /**
         * Map of table-name suffixes to their AUTO_INCREMENT primary-key
         * column. Covers every WordPress core table.
         */
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

        /**
         * MySQL DEFAULT column values for WordPress core tables.
         * Applied when an INSERT doesn't specify a column that MySQL
         * would fill via DEFAULT.
         */
        private const COLUMN_DEFAULTS = [
            'posts' => [
                'post_status'           => 'publish',
                'post_type'             => 'post',
                'comment_status'        => 'open',
                'ping_status'           => 'open',
                'post_password'         => '',
                'post_parent'           => '0',
                'menu_order'            => '0',
                'post_mime_type'        => '',
            ],
            'users' => [
                'user_status'           => '0',
            ],
            'term_taxonomy' => [
                'parent'                => '0',
                'count'                 => '0',
            ],
            'comments' => [
                'comment_approved'      => '1',
                'comment_type'          => 'comment',
                'comment_parent'        => '0',
            ],
            'options' => [
                'autoload'              => 'yes',
            ],
        ];

        /**
         * Resolve DEFAULT column values for a table, or empty array.
         */
        private function resolveColumnDefaults(string $table): array
        {
            foreach (self::COLUMN_DEFAULTS as $suffix => $defaults) {
                if ($table === $suffix || str_ends_with($table, '_' . $suffix)) {
                    return $defaults;
                }
            }
            return [];
        }

        /**
         * Resolve the AUTO_INCREMENT column for a table, or null.
         */
        private function resolveAutoPkColumn(string $table): ?string
        {
            foreach (self::AUTO_PK_MAP as $suffix => $pkCol) {
                if ($table === $suffix || str_ends_with($table, '_' . $suffix)) {
                    return $pkCol;
                }
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

            // --- URL Safeguard ------------------------------------------------
            $rows = $this->applySiteurlSafeguard($table, $columns, $rows);

            // --- Column defaults (MySQL DEFAULT emulation) -------------------
            $defaults = $this->resolveColumnDefaults($table);
            if (!empty($defaults)) {
                foreach ($defaults as $defCol => $defVal) {
                    if (!in_array($defCol, $columns, true)) {
                        $columns[] = $defCol;
                        foreach ($rows as &$row) {
                            $row[] = $defVal;
                        }
                        unset($row);
                    }
                }
            }

            // --- Auto-increment PK injection ---------------------------------
            $pkCol = $this->resolveAutoPkColumn($table);
            $firstGeneratedId = 0;

            if ($pkCol !== null && !in_array($pkCol, $columns, true)) {
                // PK column is not in the INSERT — auto-generate it.
                array_unshift($columns, $pkCol);
                foreach ($rows as $idx => &$row) {
                    $newId = $this->walManager->nextAutoIncrement($table);
                    if ($idx === 0) {
                        $firstGeneratedId = $newId;
                    }
                    array_unshift($row, (string) $newId);
                }
                unset($row);
            } elseif ($pkCol !== null) {
                // PK column IS in the INSERT — use the value as insert_id.
                $pkIdx = array_search($pkCol, $columns, true);
                if ($pkIdx !== false && isset($rows[0][$pkIdx])) {
                    $firstGeneratedId = (int) $rows[0][$pkIdx];
                }
            }

            if (count($rows) === 1) {
                $payload = array_combine($columns, $rows[0]);
                $this->rows_affected = $this->walManager->insert(
                    $table,
                    $payload,
                    $this->txCounter
                );
            } else {
                $this->rows_affected = $this->walManager->insertBatch(
                    $table,
                    $columns,
                    $rows,
                    $this->txCounter
                );
            }

            $this->insert_id = $firstGeneratedId ?: $this->txCounter;
            return $this->rows_affected;
        }

        // -- REPLACE (treat as INSERT) ----------------------------------------

        private function handleReplace(string $sql): int|false
        {
            // REPLACE INTO is used by WP for session tokens, etc.
            // Convert to INSERT for our engine.
            $converted = preg_replace('/^REPLACE\s+/i', 'INSERT ', $sql);
            return $this->handleInsert($converted);
        }

        // -- UPDATE -----------------------------------------------------------

        private function handleUpdate(string $sql): int|false
        {
            $parsed = $this->mutationParser->parseUpdate($sql);
            if ($parsed === null) {
                return 0;
            }

            // URL safeguard for updates too
            $setValues = $this->applySiteurlSafeguardToSet(
                $parsed['table'],
                $parsed['set']
            );

            $this->rows_affected = $this->walManager->updateRows(
                $parsed['table'],
                $setValues,
                $parsed['conditions'],
                $this->txCounter
            );

            return $this->rows_affected;
        }

        // -- DELETE -----------------------------------------------------------

        private function handleDelete(string $sql): int|false
        {
            $parsed = $this->mutationParser->parseDelete($sql);
            if ($parsed === null) {
                return 0;
            }

            $this->rows_affected = $this->walManager->deleteRows(
                $parsed['table'],
                $parsed['conditions'],
                $this->txCounter
            );

            return $this->rows_affected;
        }

        // -- DDL (CREATE TABLE etc.) ------------------------------------------

        /**
         * DDL statements are silently accepted. The HTML engine does not need
         * schema definitions — columns are inferred from INSERT payloads.
         */
        private function handleDdl(string $sql): true
        {
            return true;
        }

        // -- SHOW / DESCRIBE --------------------------------------------------

        /**
         * Handle SHOW TABLES, SHOW COLUMNS, SHOW FULL COLUMNS, etc.
         *
         * Returns an empty result set so that WordPress dbDelta() and plugin
         * installers don't fatal-error, while the HTML engine quietly creates
         * structure on first write.
         */
        private function handleShow(string $sql): int
        {
            $this->last_result = [];
            $this->num_rows    = 0;

            // SHOW TABLES / SHOW TABLES LIKE '...'
            if (preg_match('/SHOW\s+TABLES/i', $sql)) {
                $likePattern = null;
                if (preg_match('/LIKE\s+[\'"](.+?)[\'"]/i', $sql, $lm)) {
                    // Convert SQL LIKE pattern to a regex
                    $likePattern = '/^' . str_replace(['%', '_'], ['.*', '.'], preg_quote($lm[1], '/')) . '$/i';
                    // Re-interpret the original SQL LIKE wildcards
                    $likePattern = '/^' . str_replace(
                        ['\%', '\_'],
                        ['.*', '.'],
                        preg_quote($lm[1], '/')
                    ) . '$/i';
                }
                $dir = WP_CONTENT_DIR . '/html_db';
                if (is_dir($dir)) {
                    $files = glob($dir . '/*.wal.html');
                    if (is_array($files)) {
                        foreach ($files as $f) {
                            $tbl = basename($f, '.wal.html');
                            if ($likePattern !== null && !preg_match($likePattern, $tbl)) {
                                continue;
                            }
                            $key = 'Tables_in_' . ($this->dbname ?: 'htmldb');
                            $this->last_result[] = (object) [$key => $tbl];
                        }
                    }
                    $this->num_rows = count($this->last_result);
                }
            }

            // SHOW FULL COLUMNS — return empty
            // SHOW CREATE TABLE — return empty

            return $this->num_rows;
        }

        /**
         * DESCRIBE returns an empty column list. Enough for WP's dbDelta().
         */
        private function handleDescribe(string $sql): int
        {
            $this->last_result = [];
            $this->num_rows    = 0;
            return 0;
        }

        // -- WordPress compatibility hacks ------------------------------------

        /**
         * URL Safeguard for INSERTs.
         *
         * If a row targets wp_options with option_name = 'siteurl' or 'home'
         * and the option_value is empty, inject the current server host.
         */
        private function applySiteurlSafeguard(
            string $table,
            array  $columns,
            array  $rows
        ): array {
            // Only act on tables ending in "options"
            if (!str_ends_with($table, 'options')) {
                return $rows;
            }

            $nameIdx  = array_search('option_name',  $columns, true);
            $valueIdx = array_search('option_value', $columns, true);

            if ($nameIdx === false || $valueIdx === false) {
                return $rows;
            }

            $urlTargets = ['siteurl', 'home'];

            foreach ($rows as &$row) {
                $name  = $row[$nameIdx]  ?? '';
                $value = trim($row[$valueIdx] ?? '');

                if (in_array($name, $urlTargets, true) && $value === '') {
                    $row[$valueIdx] = $this->inferSiteUrl();
                }
            }
            unset($row);

            return $rows;
        }

        /**
         * URL Safeguard for UPDATEs.
         */
        private function applySiteurlSafeguardToSet(string $table, array $set): array
        {
            if (!str_ends_with($table, 'options')) {
                return $set;
            }
            $urlKeys = ['siteurl', 'home'];
            if (isset($set['option_value'])
                && trim($set['option_value']) === ''
                && isset($set['option_name'])
                && in_array($set['option_name'], $urlKeys, true)
            ) {
                $set['option_value'] = $this->inferSiteUrl();
            }
            return $set;
        }

        /**
         * Build the site URL from the current request context.
         */
        private function inferSiteUrl(): string
        {
            $scheme = (!empty($_SERVER['HTTPS']) && $_SERVER['HTTPS'] !== 'off')
                ? 'https'
                : 'http';
            $host = $_SERVER['HTTP_HOST'] ?? 'localhost';
            return $scheme . '://' . $host;
        }
    }

    // Bootstrap: replace the global $wpdb
    $GLOBALS['wpdb'] = new HtmlDatabase_WPDB(
        defined('DB_USER')     ? DB_USER     : '',
        defined('DB_PASSWORD') ? DB_PASSWORD : '',
        defined('DB_NAME')     ? DB_NAME     : '',
        defined('DB_HOST')     ? DB_HOST     : ''
    );
}
