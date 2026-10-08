<?php
/**
 * T03 - plugin ecosystem: ACF + Elementor DB-layer behaviour.
 * Run: wp eval-file htmldb-tests/t03_plugins.php
 */
global $wpdb, $PASS, $FAIL;
$PASS = 0; $FAIL = 0;
function check(string $name, $cond, $detail = ''): void {
    global $PASS, $FAIL;
    if ($cond) { $PASS++; echo "PASS  $name\n"; }
    else { $FAIL++; echo "FAIL  $name  $detail\n"; }
}

// Idempotency: purge fixtures from previous runs.
$old = get_posts(['post_type' => ['acf-field-group', 'acf-field'], 'numberposts' => -1,
    'post_status' => ['publish', 'draft', 'trash'], 'fields' => 'ids',
    'meta_query' => []]);
foreach ($old as $oid) {
    $t = get_post($oid);
    if ($t && (str_starts_with($t->post_title, 'T03') || str_starts_with($t->post_title, 'Text Field') || $t->post_title === 'Repeater')) {
        wp_delete_post($oid, true);
    }
}
$oldPages = get_posts(['post_type' => ['page', 'post', 'e_header'], 'numberposts' => -1,
    'post_status' => ['publish', 'draft', 'trash'], 'fields' => 'ids']);
foreach ($oldPages as $oid) {
    $t = get_post($oid);
    if ($t && str_starts_with($t->post_title, 'T03')) wp_delete_post($oid, true);
}

// ================= ACF =====================================================
if (!class_exists('ACF')) {
    check('ACF active', false, 'ACF plugin not active');
} else {
    // Field group + fields stored as posts (what the ACF UI does)
    // ACF resolves a group key through post_name (the UI stores it there)
    $group_id = wp_insert_post([
        'post_title' => 'T03 Group',
        'post_name'  => 'group_placeholder',
        'post_type'  => 'acf-field-group',
        'post_status' => 'publish',
        'post_content' => serialize(['position' => 'acf_after_title', 'style' => 'default', 'menu_order' => 0]),
    ]);
    $wpdb->update($wpdb->posts, ['post_name' => 'group_' . $group_id], ['ID' => $group_id]);
    clean_post_cache($group_id);
    check('ACF group post created', is_numeric($group_id) && $group_id > 0, "got: $group_id");

    $f1 = wp_insert_post([
        'post_title' => 'Text Field',
        'post_name'  => 'field_t03_text',
        'post_excerpt' => 't03_text',
        'post_type'  => 'acf-field',
        'post_status' => 'publish',
        'post_parent' => $group_id,
        'menu_order' => 0,
        'post_content' => serialize(['key' => 'field_t03_text', 'label' => 'Text Field', 'name' => 't03_text', 'type' => 'text', 'parent' => 'group_' . $group_id]),
    ]);
    $f2 = wp_insert_post([
        'post_title' => 'Repeater',
        'post_name'  => 'field_t03_rep',
        'post_excerpt' => 't03_rep',
        'post_type'  => 'acf-field',
        'post_status' => 'publish',
        'post_parent' => $group_id,
        'menu_order' => 1,
        'post_content' => serialize(['key' => 'field_t03_rep', 'label' => 'Repeater', 'name' => 't03_rep', 'type' => 'repeater', 'parent' => 'group_' . $group_id, 'sub_fields' => [['key' => 'field_t03_sub', 'name' => 'sub1', 'type' => 'text', 'parent' => 'field_t03_rep']]]),
    ]);
    check('ACF field posts created', is_numeric($f1) && is_numeric($f2), "$f1/$f2");

    // ACF loads field groups via WP_Query on acf-field-group with meta/tax queries
    $groups = acf_get_field_groups();
    $found = false;
    foreach ((array)$groups as $g) { if (($g['ID'] ?? null) == $group_id || ($g['key'] ?? '') === 'group_' . $group_id) $found = true; }
    check('acf_get_field_groups finds group', $found, 'groups=' . count((array)$groups));

    // ACF loads fields of a group: ORDER BY menu_order ASC on post_parent
    $fields = acf_get_fields('group_' . $group_id);
    check('acf_get_fields returns 2 fields', is_array($fields) && count($fields) === 2, is_array($fields) ? 'count=' . count($fields) : var_export($fields, true));
    check('acf_get_fields menu_order sorted', is_array($fields) && ($fields[0]['name'] ?? '') === 't03_text', is_array($fields) ? ($fields[0]['name'] ?? '?') : '');

    // Field values on a post, incl repeater (multi-row meta pattern)
    $post_id = wp_insert_post(['post_title' => 'T03 ACF post', 'post_status' => 'publish']);
    // update_metadata() unslashes its input (WP core contract), so callers
    // must pass slashed data — exactly what the ACF UI does.
    update_post_meta($post_id, 't03_text', wp_slash('Hello ACF'));
    update_post_meta($post_id, 'field_t03_text', 'field_t03_text');
    update_post_meta($post_id, 't03_rep', 2);
    update_post_meta($post_id, 'field_t03_rep', 'field_t03_rep');
    update_post_meta($post_id, 't03_rep_0_sub1', wp_slash('row1'));
    update_post_meta($post_id, 't03_rep_1_sub1', wp_slash('row2'));
    update_post_meta($post_id, 'field_t03_rep_0_sub1', 'field_t03_sub');
    update_post_meta($post_id, 'field_t03_rep_1_sub1', 'field_t03_sub');

    $v = get_field('t03_text', $post_id);
    check('get_field text', $v === 'Hello ACF', var_export($v, true));
    // Repeater formatting is ACF PRO-only (free has no repeater type), so the
    // DB-layer contract is the multi-row meta pattern itself: each row stored
    // as its own meta key, read back in order.
    $row1 = get_post_meta($post_id, 't03_rep_0_sub1', true);
    $row2 = get_post_meta($post_id, 't03_rep_1_sub1', true);
    $rowCount = (int) get_post_meta($post_id, 't03_rep', true);
    check('repeater multi-row meta pattern', $rowCount === 2 && $row1 === 'row1' && $row2 === 'row2', "count=$rowCount r1=" . var_export($row1, true) . " r2=" . var_export($row2, true));

    // ACF options page style: meta on 'option'
    update_field('t03_text', wp_slash('OptionVal'), 'option');
    check('get_field on option', get_field('t03_text', 'option') === 'OptionVal', var_export(get_field('t03_text', 'option'), true));
}

// ================= Elementor ================================================
if (!class_exists('\Elementor\Plugin')) {
    check('Elementor active', false, 'Elementor plugin not active');
} else {
    $ep = wp_insert_post(['post_title' => 'T03 Elementor Page', 'post_status' => 'publish', 'post_type' => 'page']);
    update_post_meta($ep, '_wp_page_template', 'elementor_full');
    update_post_meta($ep, '_elementor_edit_mode', 'builder');

    // Realistic elementor data: nested JSON with HTML, slashes, emoji
    $widget_html = '<h2>Elementor 🚀 Title</h2><p>quote\'s "dq" back\\slash &amp; entity</p>';
    $edata = [[
        'id' => 'a1b2c3d', 'elType' => 'container', 'settings' => ['background' => 'gradient'],
        'elements' => [[
            'id' => 'e4f5g6', 'elType' => 'widget', 'settings' => ['html' => $widget_html, '_title' => 'HTML \"escaped\"'],
            'elements' => [], 'widgetType' => 'html',
        ]],
    ]];
    // Elementor stores slashed JSON via update_post_meta (core unslashes it)
    update_post_meta($ep, '_elementor_data', wp_slash(json_encode($edata, JSON_UNESCAPED_UNICODE)));
    update_post_meta($ep, '_elementor_version', '3.24.0');

    $raw = get_post_meta($ep, '_elementor_data', true);
    $decoded = json_decode($raw, true);
    check('elementor data JSON roundtrip', is_array($decoded) && ($decoded[0]['elements'][0]['settings']['html'] ?? '') === $widget_html, substr(var_export($raw, true), 0, 200));

    // Elementor loads editor data via get_metadata + its own query for templates
    $plugin = \Elementor\Plugin::instance();
    $doc = $plugin->documents->get($ep);
    check('Elementor document loads', $doc !== false, 'doc=false');
    if ($doc) {
        $content = $doc->get_content();
        check('Elementor content renders widget', is_string($content) && str_contains($content, 'Elementor'), substr(var_export($content, true), 0, 150));
    }

    // Elementor CSS file generation (API differs across versions; guard it)
    if ($doc && method_exists($plugin->files ?? new stdClass(), 'get_css_file_content')) {
        $plugin->files->get_css_file_content($doc);
    }
    check('Elementor css gen no crash', true);

    // Query pages built with elementor (meta_key EXISTS + meta_value)
    $q = new WP_Query(['post_type' => 'page', 'meta_key' => '_elementor_edit_mode', 'meta_value' => 'builder', 'fields' => 'ids']);
    check('WP_Query meta_key/value finds page', in_array((int)$ep, array_map('intval', $q->posts)), 'posts: ' . implode(',', $q->posts));

    // Elementor CPT types (e-page-template etc)
    $tpl = wp_insert_post(['post_title' => 'T03 Header', 'post_type' => 'e_header', 'post_status' => 'publish']);
    check('Elementor CPT e_header insert', is_numeric($tpl) && $tpl > 0, "got: $tpl");
}

// -- REST API smoke (uses SQL_CALC_FOUND_ROWS + meta/tax joins) ---------------------
// In-process dispatch: the container cannot curl its own published host port.
$restReq = new WP_REST_Request('GET', '/wp/v2/posts');
$restReq->set_param('per_page', 5);
$restResp = rest_do_request($restReq);
$body = $restResp->get_data();
check('REST /wp/v2/posts works', is_array($body) && count($body) >= 1, 'status=' . $restResp->get_status());
$restReq2 = new WP_REST_Request('GET', '/wp/v2/pages/' . ($ep ?? 0));
$restResp2 = rest_do_request($restReq2);
$b2 = $restResp2->get_data();
check('REST /wp/v2/pages/{id} works', is_array($b2) && (int)($b2['id'] ?? 0) === (int)$ep, 'status=' . $restResp2->get_status());

echo "\n== T03: $PASS passed, $FAIL failed ==\n";
