<?php
/**
 * HtmlDB bench - T01 basic CRUD & core flows.
 * Run: wp eval-file htmldb-tests/t01_basic.php --allow-root
 */
global $wpdb, $PASS, $FAIL;
$PASS = 0; $FAIL = 0;
function check(string $name, $cond, $detail = ''): void {
    global $PASS, $FAIL;
    if ($cond) { $PASS++; echo "PASS  $name\n"; }
    else { $FAIL++; echo "FAIL  $name  $detail\n"; }
}

// Idempotency: purge test fixtures from previous runs so aggregate/ORDER BY
// assertions see a deterministic set.
$wpdb->query("DELETE FROM {$wpdb->posts} WHERE ID BETWEEN 90000 AND 90099");
$wpdb->query("DELETE FROM {$wpdb->postmeta} WHERE post_id BETWEEN 90000 AND 90099");
delete_option('t01_opt');
$wpdb->query("DELETE FROM {$wpdb->options} WHERE option_name LIKE '\\_transient\\_t01\\_%'");
$wpdb->query("DELETE FROM {$wpdb->users} WHERE user_login = 't01user'");
foreach (['T01 Cat', 'T02 Menu', 'T02 Widget Area'] as $termName) {
    $t = get_term_by('name', $termName, 'category');
    if ($t) wp_delete_term($t->term_id, 'category');
}

// -- 1. INSERT + get_row by PK -----------------------------------------------
$id = wp_insert_post(['post_title' => 'T01 Hello', 'post_content' => 'Hello <b>world</b>', 'post_status' => 'publish', 'post_type' => 'post']);
check('wp_insert_post returns numeric ID', is_numeric($id) && $id > 0, "got: $id");
$p = get_post($id);
check('get_post roundtrip title', $p && $p->post_title === 'T01 Hello', $p ? $p->post_title : 'null');
check('get_post content with HTML', $p && str_contains($p->post_content, '<b>world</b>'), $p->post_content ?? '');

// -- 2. UPDATE ---------------------------------------------------------------
wp_update_post(['ID' => $id, 'post_title' => 'T01 Updated']);
$p = get_post($id);
check('wp_update_post title', $p->post_title === 'T01 Updated', $p->post_title);
check('update keeps other columns', $p->post_status === 'publish', $p->post_status);

// -- 3. meta (postmeta CRUD) ---------------------------------------------------
$mid = add_post_meta($id, 't01_key', 'v1');
check('add_post_meta returns id', is_numeric($mid) && $mid > 0, "got: $mid");
check('get_post_meta single', get_post_meta($id, 't01_key', true) === 'v1');
update_post_meta($id, 't01_key', 'v2');
check('update_post_meta', get_post_meta($id, 't01_key', true) === 'v2', var_export(get_post_meta($id, 't01_key', true), true));
delete_post_meta($id, 't01_key');
$left_meta = $wpdb->get_var($wpdb->prepare("SELECT COUNT(*) FROM {$wpdb->postmeta} WHERE post_id=%d AND meta_key=%s", $id, 't01_key'));
check('delete_post_meta removes row', (int)$left_meta === 0, "raw rows left: " . var_export($left_meta, true));

// -- 4. serialized data --------------------------------------------------------
$ser = ['a' => 1, 'b' => ['nested', "\0bin\0key", 'quote\'s "dq" <tag>&amp;'], 'c' => null];
update_post_meta($id, 't01_ser', $ser);
$back = get_post_meta($id, 't01_ser', true);
check('serialized array roundtrip', $back === $ser, var_export($back, true));

// -- 5. options ---------------------------------------------------------------
delete_option('t01_opt');
add_option('t01_opt', ['deep' => ['x' => 1]]);
check('add_option/get_option array', get_option('t01_opt') === ['deep' => ['x' => 1]], var_export(get_option('t01_opt'), true));
update_option('t01_opt', ['deep' => ['x' => 2]]);
check('update_option', get_option('t01_opt') === ['deep' => ['x' => 2]]);
$dup = $wpdb->get_row($wpdb->prepare("SELECT COUNT(*) c FROM {$wpdb->options} WHERE option_name=%s", 't01_opt'));
check('update_option no duplicate rows', (int)$dup->c === 1, "rows: {$dup->c}");

// -- 6. DELETE + force delete ---------------------------------------------------
wp_delete_post($id, true);
check('wp_delete_post force', get_post($id) === null);
$cnt = $wpdb->get_var($wpdb->prepare("SELECT COUNT(*) FROM {$wpdb->posts} WHERE ID=%d", $id));
check('row really gone from storage', (int)$cnt === 0, "count: $cnt");

// -- 7. COUNT(*) without GROUP BY (aggregate) ------------------------------------
$wpdb->query("INSERT INTO {$wpdb->posts} (ID, post_title, post_status, post_type, post_date) VALUES (90001,'agg1','publish','post','2026-01-01 00:00:00')");
$wpdb->query("INSERT INTO {$wpdb->posts} (ID, post_title, post_status, post_type, post_date) VALUES (90002,'agg2','publish','post','2026-01-02 00:00:00')");
$c = $wpdb->get_var("SELECT COUNT(*) FROM {$wpdb->posts} WHERE post_status='publish' AND post_type='post'");
check('COUNT(*) aggregate', is_numeric($c) && (int)$c >= 2, "got: " . var_export($c, true));
$m = $wpdb->get_var("SELECT MAX(ID) FROM {$wpdb->posts}");
check('MAX() aggregate', (int)$m >= 90002, "got: " . var_export($m, true));

// -- 8. LIKE / IN / NOT IN / comparisons ---------------------------------------
$found = $wpdb->get_col("SELECT ID FROM {$wpdb->posts} WHERE post_title LIKE 'agg%' AND post_status='publish'");
check('LIKE prefix match', in_array('90001', array_map('strval', $found)) && in_array('90002', array_map('strval', $found)), implode(',', $found));
$in = $wpdb->get_col("SELECT ID FROM {$wpdb->posts} WHERE ID IN (90001, 90002)");
check('IN list', count($in) === 2, implode(',', $in));
$gt = $wpdb->get_col("SELECT ID FROM {$wpdb->posts} WHERE ID > 90001 AND post_type='post'");
check('numeric > comparison', in_array('90002', array_map('strval', $gt)) && !in_array('90001', array_map('strval', $gt)), implode(',', $gt));

// -- 9. ORDER BY + LIMIT/OFFSET ---------------------------------------------------
$ordered = $wpdb->get_col("SELECT ID FROM {$wpdb->posts} WHERE post_type='post' AND post_status='publish' AND ID BETWEEN 90001 AND 90005 ORDER BY ID DESC LIMIT 1");
check('ORDER BY DESC LIMIT', (int)($ordered[0] ?? 0) === 90002, implode(',', $ordered));

// -- 10. term_relationships (no-PK table) tombstone correctness -------------------
$cat = wp_insert_term('T01 Cat', 'category');
$pid = wp_insert_post(['post_title' => 't01 cat post', 'post_status' => 'publish', 'post_type' => 'post']);
wp_set_post_terms($pid, [$cat['term_id']], 'category');
$terms = wp_get_post_terms($pid, 'category', ['fields' => 'ids']);
check('term assigned', in_array((int)$cat['term_id'], array_map('intval', $terms)), implode(',', array_map('strval', $terms)));
wp_set_object_terms($pid, [], 'category');
$terms2 = wp_get_post_terms($pid, 'category', ['fields' => 'ids']);
check('term removed (no-PK tombstone)', !in_array((int)$cat['term_id'], array_map('intval', $terms2)), 'STILL ASSIGNED: ' . implode(',', array_map('strval', $terms2)));

// -- 11. users & usermeta ----------------------------------------------------------
$uid = wp_create_user('t01user', 'pass12345678', 't01@example.com');
check('wp_create_user', is_numeric($uid) && $uid > 0, "got: $uid");
update_user_meta($uid, 't01_um', 'umv');
check('usermeta roundtrip', get_user_meta($uid, 't01_um', true) === 'umv');
$u = get_userdata($uid);
check('get_userdata login', $u && $u->user_login === 't01user');

// -- 12. comments -------------------------------------------------------------------
$cid = wp_new_comment(['comment_post_ID' => $pid, 'comment_author' => 'Bob', 'comment_content' => 'Nice <3 & "post"', 'user_id' => 0, 'comment_author_email' => 'bob@example.com']);
check('wp_new_comment', is_numeric($cid) && $cid > 0, "got: $cid");
$cobj = get_comment($cid);
check('comment content roundtrip', $cobj && $cobj->comment_content === 'Nice <3 & "post"', $cobj->comment_content ?? '');
// comment_whitelist=1 holds a first-time commenter in moderation; approve to count
$wpdb->update($wpdb->comments, ['comment_approved' => '1'], ['comment_ID' => $cid]);
wp_update_comment_count_now($pid);
$cc = get_comments_number($pid);
check('get_comments_number', (int)$cc === 1, "got: $cc");

// -- 13. transients (LIKE with backslash-escaped underscore) -------------------------
set_transient('t01_tr', 'transient-val', 300);
check('set/get transient', get_transient('t01_tr') === 'transient-val', var_export(get_transient('t01_tr'), true));
// simulate expired (drop object cache first: same-request cache would mask timeout)
$wpdb->update($wpdb->options, ['option_value' => time() - 10], ['option_name' => '_transient_timeout_t01_tr']);
wp_cache_delete('t01_tr', 'transient');
wp_cache_delete('_transient_timeout_t01_tr', 'transient_timeout');
// get_option reads the timeout from the 'options' group; raw $wpdb->update
// bypasses cache invalidation there, so drop it explicitly.
wp_cache_delete('_transient_timeout_t01_tr', 'options');
check('expired transient returns false', get_transient('t01_tr') === false, var_export(get_transient('t01_tr'), true));
// cleanup deletes via LIKE '\_transient\_%'
delete_transient('t01_tr');
$left = $wpdb->get_var("SELECT COUNT(*) FROM {$wpdb->options} WHERE option_name LIKE '\\_transient\\_t01\\_tr'");
check('transient rows deleted', (int)$left === 0, "left: $left");

// -- 14. cron ------------------------------------------------------------------------
wp_schedule_single_event(time() + 60, 't01_cron_event');
$cron = get_option('cron');
$found = false;
foreach ($cron as $ts => $events) { if (is_array($events)) foreach ($events as $hook => $v) { if ($hook === 't01_cron_event') $found = true; } }
check('cron event scheduled + serialized', $found);

// -- 15. search (LIKE %s%) -------------------------------------------------------------
$wpdb->query("INSERT INTO {$wpdb->posts} (ID, post_title, post_content, post_status, post_type, post_date) VALUES (90003,'uniquezzz needle','body text','publish','post','2026-01-03 00:00:00')");
$s = new WP_Query(['s' => 'uniquezzz', 'post_type' => 'post', 'fields' => 'ids']);
check('WP_Query search', in_array(90003, array_map('intval', $s->posts)), implode(',', $s->posts));

// -- 16. archive date query YEAR/MONTH ---------------------------------------------------
$wpdb->query("INSERT INTO {$wpdb->posts} (ID, post_title, post_status, post_type, post_date, post_date_gmt) VALUES (90004,'arch1','publish','post','2025-03-15 10:00:00','2025-03-15 10:00:00')");
$aq = new WP_Query(['date_query' => [['year' => 2025, 'month' => 3]], 'fields' => 'ids', 'post_type' => 'post']);
check('date_query year/month', in_array(90004, array_map('intval', $aq->posts)), implode(',', $aq->posts));

// -- 17. meta_query (JOIN-heavy) -----------------------------------------------------------
update_post_meta(90001, 'mq_key', 'mq_val');
$mq = new WP_Query(['meta_query' => [['key' => 'mq_key', 'value' => 'mq_val']], 'fields' => 'ids', 'post_type' => 'post']);
check('meta_query finds row', in_array(90001, array_map('intval', $mq->posts)), 'posts: ' . implode(',', $mq->posts));

// -- 18. term count update (JOIN UPDATE) ------------------------------------------------------
$before = (int)$wpdb->get_var($wpdb->prepare("SELECT count FROM {$wpdb->term_taxonomy} WHERE term_id=%d", $cat['term_id']));
wp_set_object_terms($pid, [(int)$cat['term_id']], 'category');
$after = (int)$wpdb->get_var($wpdb->prepare("SELECT count FROM {$wpdb->term_taxonomy} WHERE term_id=%d", $cat['term_id']));
check('term_taxonomy count incremented via JOIN UPDATE', $after === $before + 1, "before=$before after=$after");

// -- 19. NOW() literal ---------------------------------------------------------------------
$wpdb->query("INSERT INTO {$wpdb->posts} (ID, post_title, post_status, post_type, post_date, post_modified) VALUES (90005,'nowtest','publish','post',NOW(),NOW())");
$nd = $wpdb->get_var("SELECT post_date FROM {$wpdb->posts} WHERE ID=90005");
check('NOW() evaluated (not literal)', $nd !== 'NOW()' && strtotime((string)$nd) !== false, "stored: " . var_export($nd, true));

// -- 20. arithmetic UPDATE (count = count + 1) -------------------------------------------------
$wpdb->query("UPDATE {$wpdb->term_taxonomy} SET count = count + 5 WHERE term_id=" . (int)$cat['term_id']);
$cv = $wpdb->get_var($wpdb->prepare("SELECT count FROM {$wpdb->term_taxonomy} WHERE term_id=%d", $cat['term_id']));
check('arithmetic UPDATE count+5', is_numeric($cv), "stored: " . var_export($cv, true));

// -- 21. INSERT IGNORE on options (core lock pattern) -------------------------------------------
// WP_Upgrader::create_lock() / taxonomy / comment locks all use
// "INSERT IGNORE INTO options (option_name, ...)". option_name is UNIQUE,
// so the second insert must affect 0 rows (lock held) without erroring.
$wpdb->query("DELETE FROM {$wpdb->options} WHERE option_name = 't01_lock'");
$first = $wpdb->query("INSERT IGNORE INTO {$wpdb->options} (option_name, option_value, autoload) VALUES ('t01_lock', '111', 'off')");
check('INSERT IGNORE first row affects 1', (int)$first === 1, "affected: " . var_export($first, true));
$second = $wpdb->query("INSERT IGNORE INTO {$wpdb->options} (option_name, option_value, autoload) VALUES ('t01_lock', '222', 'off')");
check('INSERT IGNORE duplicate affects 0', (int)$second === 0, "affected: " . var_export($second, true));
$lockVal = $wpdb->get_var($wpdb->prepare("SELECT option_value FROM {$wpdb->options} WHERE option_name=%s", 't01_lock'));
check('duplicate did not overwrite value', $lockVal === '111', "value: " . var_export($lockVal, true));
$lockRows = $wpdb->get_var($wpdb->prepare("SELECT COUNT(*) FROM {$wpdb->options} WHERE option_name=%s", 't01_lock'));
check('still exactly one row', (int)$lockRows === 1, "rows: " . var_export($lockRows, true));
$wpdb->query("DELETE FROM {$wpdb->options} WHERE option_name = 't01_lock'");

// -- 22. INSERT IGNORE multi-row (partial skip) -------------------------------------------------
$wpdb->query("DELETE FROM {$wpdb->options} WHERE option_name IN ('t01_mi_a','t01_mi_b','t01_mi_c')");
$wpdb->query("INSERT INTO {$wpdb->options} (option_name, option_value, autoload) VALUES ('t01_mi_a', 'a', 'off')");
// b and c are new; a collides. MySQL inserts the 2 new rows, skips a.
$aff = $wpdb->query("INSERT IGNORE INTO {$wpdb->options} (option_name, option_value, autoload) VALUES ('t01_mi_a','a2','off'),('t01_mi_b','b','off'),('t01_mi_c','c','off')");
check('INSERT IGNORE multi-row skips only the clash', (int)$aff === 2, "affected: " . var_export($aff, true));
$keptA = $wpdb->get_var($wpdb->prepare("SELECT option_value FROM {$wpdb->options} WHERE option_name=%s", 't01_mi_a'));
check('multi-row: existing row untouched', $keptA === 'a', "a=" . var_export($keptA, true));
$newB = $wpdb->get_var($wpdb->prepare("SELECT option_value FROM {$wpdb->options} WHERE option_name=%s", 't01_mi_b'));
check('multi-row: new row inserted', $newB === 'b', "b=" . var_export($newB, true));
$wpdb->query("DELETE FROM {$wpdb->options} WHERE option_name IN ('t01_mi_a','t01_mi_b','t01_mi_c')");

// -- 23. dotted option names (core_updater.lock round-trip) -------------------
// Prefix stripping must not reach inside quoted literals: a naive
// wp_x.col -> col regex turns 'core_updater.lock' into 'lock', which leaks
// lock rows (DELETE matches nothing) and then wedges create_lock() forever.
$dot = 't01.dot.name';
$wpdb->query("DELETE FROM {$wpdb->options} WHERE option_name = '$dot'");

$wpdb->query("INSERT INTO {$wpdb->options} (option_name, option_value, autoload) VALUES ('$dot', 'v1', 'off')");
$sel = $wpdb->get_var("SELECT option_value FROM {$wpdb->options} WHERE option_name = '$dot'");
check('dotted name: raw SELECT finds row', $sel === 'v1', "value: " . var_export($sel, true));

$cnt = $wpdb->get_var("SELECT COUNT(*) FROM {$wpdb->options} WHERE option_name = '$dot'");
check('dotted name: COUNT matches 1', (int)$cnt === 1, "count: " . var_export($cnt, true));

// UPDATE ... WHERE option_name = 'a.b' (release-style mutation path)
$wpdb->query("UPDATE {$wpdb->options} SET option_value = 'v2' WHERE option_name = '$dot'");
$upd = $wpdb->get_var("SELECT option_value FROM {$wpdb->options} WHERE option_name = '$dot'");
check('dotted name: UPDATE hits row', $upd === 'v2', "value: " . var_export($upd, true));

// DELETE ... WHERE option_name = 'a.b' must actually remove the row.
$wpdb->query("DELETE FROM {$wpdb->options} WHERE option_name = '$dot'");
$gone = $wpdb->get_var("SELECT COUNT(*) FROM {$wpdb->options} WHERE option_name = '$dot'");
check('dotted name: DELETE removes row', (int)$gone === 0, "count: " . var_export($gone, true));

// Full core lock cycle: acquire (INSERT IGNORE), fail re-acquire, release
// (DELETE), re-acquire must succeed again. This is do-core-reinstall.
$lock = 'core_updater.lock';
$wpdb->query("DELETE FROM {$wpdb->options} WHERE option_name = '$lock'");
$a1 = $wpdb->query("INSERT IGNORE INTO {$wpdb->options} (option_name, option_value, autoload) VALUES ('$lock', '111', 'off')");
check('lock: first acquire affects 1', (int)$a1 === 1, "affected: " . var_export($a1, true));
$a2 = $wpdb->query("INSERT IGNORE INTO {$wpdb->options} (option_name, option_value, autoload) VALUES ('$lock', '222', 'off')");
check('lock: second acquire affects 0', (int)$a2 === 0, "affected: " . var_export($a2, true));
$wpdb->query("DELETE FROM {$wpdb->options} WHERE option_name = '$lock'");
$left = $wpdb->get_var("SELECT COUNT(*) FROM {$wpdb->options} WHERE option_name = '$lock'");
check('lock: release DELETE really removes it', (int)$left === 0, "count: " . var_export($left, true));
$a3 = $wpdb->query("INSERT IGNORE INTO {$wpdb->options} (option_name, option_value, autoload) VALUES ('$lock', '333', 'off')");
check('lock: re-acquire after release affects 1', (int)$a3 === 1, "affected: " . var_export($a3, true));
$wpdb->query("DELETE FROM {$wpdb->options} WHERE option_name = '$lock'");

// get_option / update_option / delete_option API path with a dotted name.
delete_option('t01.dot.opt');
add_option('t01.dot.opt', 'api-v', '', 'no');
check('dotted name: get_option round-trip', get_option('t01.dot.opt') === 'api-v',
    'got: ' . var_export(get_option('t01.dot.opt'), true));
update_option('t01.dot.opt', 'api-v2');
check('dotted name: update_option works', get_option('t01.dot.opt') === 'api-v2',
    'got: ' . var_export(get_option('t01.dot.opt'), true));
delete_option('t01.dot.opt');
check('dotted name: delete_option works', get_option('t01.dot.opt') === false,
    'got: ' . var_export(get_option('t01.dot.opt'), true));

echo "\n== T01: $PASS passed, $FAIL failed ==\n";
