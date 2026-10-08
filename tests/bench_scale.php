<?php
// Scaling benchmark: how latency grows with table size.
global $wpdb;
set_time_limit(600);

$wpdb->query('DROP TABLE IF EXISTS bench_big');
$wpdb->query('CREATE TABLE bench_big (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name LONGTEXT, val BIGINT DEFAULT 0)');

$N = 20000;
$t0 = microtime(true);
for ($i = 0; $i < $N; $i++) {
    $wpdb->insert('bench_big', ['name' => 'row_' . $i . '_padding_xxxxxxxxxxxxxxxxxxxxxxxx', 'val' => $i]);
}
$ins = microtime(true) - $t0;
printf("INSERT %d: %.1fs (%.0f rows/s)\n", $N, $ins, $N / $ins);

foreach ([100, 500, 1000] as $k) {
    $t0 = microtime(true);
    for ($i = 0; $i < $k; $i++) {
        $wpdb->get_row($wpdb->prepare('SELECT * FROM bench_big WHERE id=%d', random_int(1, $N)));
    }
    $d = microtime(true) - $t0;
    printf("PK read x%d: %.1f ms/op\n", $k, $d / $k * 1000);
}

$t0 = microtime(true);
$rows = $wpdb->get_results('SELECT * FROM bench_big');
$d = microtime(true) - $t0;
printf("Full scan (%d rows): %.0f ms\n", count($rows), $d * 1000);

$t0 = microtime(true);
$wpdb->get_results($wpdb->prepare('SELECT COUNT(*) FROM bench_big WHERE val > %d', 10000));
$d = microtime(true) - $t0;
printf("COUNT WHERE val>10000: %.0f ms\n", $d * 1000);

$dir = WP_CONTENT_DIR . '/html_db/bench_big';
printf("Disk: %d chunks, total %.1f MB\n", count(glob($dir . '/chunk_*.html')), array_sum(array_map('filesize', glob($dir . '/*'))) / 1048576);

$wpdb->query('DROP TABLE bench_big');
echo "done\n";
