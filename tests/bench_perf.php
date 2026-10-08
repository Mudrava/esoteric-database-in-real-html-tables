<?php
// Synthetic benchmark: write throughput, read latency, compaction cost.
global $wpdb;
set_time_limit(300);

$wpdb->query('DROP TABLE IF EXISTS bench_rows');
$wpdb->query('CREATE TABLE bench_rows (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name LONGTEXT, val BIGINT DEFAULT 0)');

$N = 2000;
$t0 = microtime(true);
for ($i = 0; $i < $N; $i++) {
    $wpdb->insert('bench_rows', ['name' => 'row_' . $i, 'val' => $i]);
}
$ins = microtime(true) - $t0;
printf("INSERT %d rows: %.2fs (%.0f rows/s)\n", $N, $ins, $N / $ins);

// PK point read (chunk-pruned).
$t0 = microtime(true);
for ($i = 0; $i < 200; $i++) {
    $wpdb->get_row($wpdb->prepare('SELECT * FROM bench_rows WHERE id=%d', random_int(1, $N)));
}
$pk = microtime(true) - $t0;
printf("PK point read x200: %.2fs (%.1f ms/op)\n", $pk, $pk / 200 * 1000);

// Full scan (no WHERE).
$t0 = microtime(true);
for ($i = 0; $i < 10; $i++) {
    $rows = $wpdb->get_results('SELECT * FROM bench_rows');
}
$scan = microtime(true) - $t0;
printf("Full scan x10 (rows=%d): %.2fs (%.1f ms/scan)\n", count($rows), $scan, $scan / 10 * 1000);

// WHERE on non-indexed column (full scan + filter).
$t0 = microtime(true);
$wpdb->get_results($wpdb->prepare('SELECT * FROM bench_rows WHERE val=%d', 1234));
$w = microtime(true) - $t0;
printf("WHERE val=? : %.1f ms\n", $w * 1000);

// WAL size before compaction.
$wal = WP_CONTENT_DIR . '/html_db/bench_rows/wal.html';
printf("WAL size after %d inserts: %.1f MB\n", $N, filesize($wal) / 1048576);

// Compaction cost.
$t0 = microtime(true);
$ref = new ReflectionClass($wpdb);
foreach ($ref->getProperties() as $p) {
    $p->setAccessible(true);
    $v = $p->getValue($wpdb);
    if (is_object($v) && method_exists($v, 'compact')) { $v->compact('bench_rows'); break; }
}
$cmp = microtime(true) - $t0;
printf("Compaction: %.2fs\n", $cmp);
printf("Chunks after: %d, WAL size: %.1f MB\n",
    count(glob(WP_CONTENT_DIR . '/html_db/bench_rows/chunk_*.html')),
    filesize($wal) / 1048576);

// Scan after compaction.
$t0 = microtime(true);
for ($i = 0; $i < 10; $i++) { $wpdb->get_results('SELECT * FROM bench_rows'); }
$scan2 = microtime(true) - $t0;
printf("Full scan x10 after compaction: %.2fs (%.1f ms/scan)\n", $scan2, $scan2 / 10 * 1000);

$wpdb->query('DROP TABLE bench_rows');
echo "done\n";
