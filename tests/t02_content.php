<?php
/**
 * T02 - content subsystems: menus, widgets, media, revisions, attachments,
 * utf8/emoji, HTML-in-content, excerpts, pings.
 * Run: wp eval-file htmldb-tests/t02_content.php
 */
global $wpdb, $PASS, $FAIL;
$PASS = 0; $FAIL = 0;
function check(string $name, $cond, $detail = ''): void {
    global $PASS, $FAIL;
    if ($cond) { $PASS++; echo "PASS  $name\n"; }
    else { $FAIL++; echo "FAIL  $name  $detail\n"; }
}

// Idempotency: purge fixtures from previous runs.
foreach (['T02 Menu'] as $mn) {
    $m = wp_get_nav_menu_object($mn);
    if ($m) wp_delete_nav_menu($m->term_id);
}
foreach ([['T02 Parent', 'category'], ['T02 Child', 'category'], ['T02 Cat', 'category'], ['T02 Widget Area', 'category'], ['t02 tag', 'post_tag']] as [$tn, $tax]) {
    $t = get_term_by('name', $tn, $tax);
    if ($t) wp_delete_term($t->term_id, $tax);
}
$wpdb->query("DELETE FROM {$wpdb->posts} WHERE post_title IN ('t02 rev parent','T02 Template Page','t02 attach') OR post_title LIKE 'Emoji%'");
$wpdb->query("DELETE FROM {$wpdb->posts} WHERE post_type='nav_menu_item'");
delete_option('sidebars_widgets');
// remove previous uploads so wp_upload_bits keeps the original filename
array_map('unlink', glob(WP_CONTENT_DIR . '/uploads/' . date('Y') . '/' . date('m') . '/t02-test*.txt'));
// register a page template so core accepts it (block themes ship none)
add_filter('theme_page_templates', function ($tpls) { $tpls['template-fullwidth.php'] = 'Full Width'; return $tpls; });

// -- menus -------------------------------------------------------------------
$menu_id = wp_create_nav_menu('T02 Menu');
check('wp_create_nav_menu', is_numeric($menu_id) && $menu_id > 0, "got: $menu_id");
$item1 = wp_update_nav_menu_item($menu_id, 0, ['menu-item-title' => 'Home', 'menu-item-type' => 'custom', 'menu-item-url' => '/', 'menu-item-status' => 'publish']);
$item2 = wp_update_nav_menu_item($menu_id, 0, ['menu-item-title' => 'About', 'menu-item-type' => 'custom', 'menu-item-url' => '/about', 'menu-item-status' => 'publish']);
check('nav menu items created', is_numeric($item1) && is_numeric($item2), "$item1/$item2");
$items = wp_get_nav_menu_items((int)$menu_id);
check('wp_get_nav_menu_items count', is_array($items) && count($items) === 2, is_array($items) ? 'count=' . count($items) : 'not array');
check('nav menu item titles', is_array($items) && $items[0]->post_title === 'Home' && $items[1]->post_title === 'About', $items ? $items[0]->post_title : '');

// -- widgets (serialized option) ------------------------------------------------
$sidebars = get_option('sidebars_widgets');
$sidebars['wp_inactive_widgets'] = $sidebars['wp_inactive_widgets'] ?? [];
$sidebars['sidebar-1'] = ['search-2'];
update_option('sidebars_widgets', $sidebars);
$sb = get_option('sidebars_widgets');
check('widgets option roundtrip', ($sb['sidebar-1'] ?? null) === ['search-2'], var_export($sb['sidebar-1'] ?? null, true));

// -- media / attachments -----------------------------------------------------------
$upload = wp_upload_bits('t02-test.txt', null, 'hello attachment');
check('wp_upload_bits', !empty($upload['file']), var_export($upload['error'] ?? '', true));
$aid = wp_insert_attachment(['post_mime_type' => 'text/plain', 'post_title' => 't02 attach', 'post_status' => 'inherit'], $upload['file']);
check('wp_insert_attachment', is_numeric($aid) && $aid > 0, "got: $aid");
update_post_meta($aid, '_wp_attached_file', $upload['file']);
$meta = ['width' => 100, 'height' => 50, 'file' => 't02-test.txt', 'sizes' => ['thumb' => ['file' => 't02-100x50.txt', 'width' => 10, 'height' => 5]]];
wp_update_attachment_metadata($aid, $meta);
$got = wp_get_attachment_metadata($aid);
check('attachment metadata serialized roundtrip', $got === $meta, var_export($got, true));
$att = wp_get_attachment_url($aid);
check('wp_get_attachment_url', is_string($att) && str_contains($att, 't02-test.txt'), var_export($att, true));

// -- revisions ------------------------------------------------------------------------
$pid = wp_insert_post(['post_title' => 't02 rev parent', 'post_content' => 'v1', 'post_status' => 'publish']);
wp_update_post(['ID' => $pid, 'post_content' => 'v2 content']);
$revs = wp_get_post_revisions($pid);
check('revisions created', count($revs) >= 1, 'count=' . count($revs));

// -- utf8 / emoji / special -------------------------------------------------------------
// wp_insert_post expects slashed data (it unslashes internally), so feed
// it wp_slash()'d values to get an exact roundtrip of the raw string.
$weird = "Emoji 🚀🔥 + <script>alert(1)</script> + 'quote' \"dq\" \\ backslash %d %s \$var 日本 ĄĆĘ";
$wid = wp_insert_post(wp_slash(['post_title' => $weird, 'post_content' => $weird, 'post_status' => 'publish']));
$wpost = get_post($wid);
check('emoji+HTML+quotes title roundtrip', $wpost->post_title === $weird, var_export($wpost->post_title, true));
check('emoji+HTML+quotes content roundtrip', $wpost->post_content === $weird, var_export($wpost->post_content, true));

// -- page with template -------------------------------------------------------------------
$pageid = wp_insert_post(['post_title' => 'T02 Template Page', 'post_status' => 'publish', 'post_type' => 'page', 'page_template' => 'template-fullwidth.php']);
check('page_template meta', get_post_meta($pageid, '_wp_page_template', true) === 'template-fullwidth.php', var_export(get_post_meta($pageid, '_wp_page_template', true), true));

// -- ping/trackback columns + status transitions ---------------------------------------------
$st = get_post_status($pid);
check('get_post_status', $st === 'publish', var_export($st, true));
$stati = get_post_stati();
check('get_post_stati non-empty', is_array($stati) && count($stati) > 3);

// -- category tree (term_taxonomy parent) ------------------------------------------------------
$parent = wp_insert_term('T02 Parent', 'category');
$child  = wp_insert_term('T02 Child', 'category', ['parent' => $parent['term_id']]);
$kids = get_terms(['taxonomy' => 'category', 'parent' => $parent['term_id'], 'fields' => 'ids', 'hide_empty' => false]);
check('child term query by parent', in_array((int)$child['term_id'], array_map('intval', (array)$kids)), implode(',', array_map('strval', (array)$kids)));

// -- tag slug query ----------------------------------------------------------------------------
$tag = wp_insert_term('t02 tag', 'post_tag');
wp_set_post_terms($pid, [(int)$tag['term_id']], 'post_tag');
$bytag = new WP_Query(['tag' => 't02-tag', 'fields' => 'ids']);
check('WP_Query by tag slug (JOIN chain)', in_array((int)$pid, array_map('intval', $bytag->posts)), 'posts: ' . implode(',', $bytag->posts));

// -- WP_Query found_posts (SQL_CALC_FOUND_ROWS) ---------------------------------------------------
$q = new WP_Query(['post_type' => 'post', 'posts_per_page' => 1]);
check('found_posts > 0 with LIMIT 1', $q->found_posts > 0, "found={$q->found_posts}");

// -- get_categories hide_empty (COUNT(tp.term_id) + GROUP BY + HAVING) -----------------------------
$cnts = wp_count_posts();
check('wp_count_posts', isset($cnts->publish) && $cnts->publish > 0, var_export($cnts, true));
$cats = get_categories(['hide_empty' => true]);
check('get_categories hide_empty', is_array($cats) && count($cats) >= 1, 'count=' . (is_array($cats) ? count($cats) : getLastError()));

echo "\n== T02: $PASS passed, $FAIL failed ==\n";
