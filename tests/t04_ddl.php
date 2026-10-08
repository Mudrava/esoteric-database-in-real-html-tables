<?php
// WHERE clause matching on a schema-created table.
global $wpdb;
$wpdb->query("DROP TABLE IF EXISTS wp_where_test");
$wpdb->query("CREATE TABLE wp_where_test (
    id bigint unsigned NOT NULL AUTO_INCREMENT,
    name varchar(100) NOT NULL,
    status varchar(20) NOT NULL DEFAULT 'draft',
    score int NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
) ENGINE=InnoDB");

$wpdb->query("INSERT INTO wp_where_test (name, status) VALUES ('alpha', 'live')");
$wpdb->query("INSERT INTO wp_where_test (name) VALUES ('beta')");
$wpdb->query("INSERT INTO wp_where_test (name, status, score) VALUES ('gamma', 'live', 42)");

$GLOBALS['pass'] = 0; $GLOBALS['fail'] = 0;
function check($label, $cond) {
    if ($cond) { $GLOBALS['pass']++; echo "PASS: $label\n"; }
    else { $GLOBALS['fail']++; echo "FAIL: $label\n"; }
}

// 1. equality with single quotes
$r = $wpdb->get_results("SELECT id FROM wp_where_test WHERE status = 'live' ORDER BY id", ARRAY_A);
check("WHERE status='live' -> 2 rows", count($r) === 2);

// 2. equality via prepare-style
$r = $wpdb->get_row("SELECT name FROM wp_where_test WHERE name = 'beta'", ARRAY_A);
check("WHERE name='beta' -> beta", ($r['name'] ?? '') === 'beta');

// 3. numeric comparison
$r = $wpdb->get_results("SELECT id FROM wp_where_test WHERE score > 10", ARRAY_A);
check("WHERE score>10 -> 1 row", count($r) === 1);

// 4. AND
$r = $wpdb->get_results("SELECT id FROM wp_where_test WHERE status = 'live' AND score = 42", ARRAY_A);
check("WHERE live AND score=42 -> 1", count($r) === 1);

// 5. IN
$r = $wpdb->get_results("SELECT id FROM wp_where_test WHERE name IN ('alpha','gamma')", ARRAY_A);
check("WHERE name IN -> 2", count($r) === 2);

// 6. LIKE
$r = $wpdb->get_results("SELECT id FROM wp_where_test WHERE name LIKE 'a%'", ARRAY_A);
check("WHERE name LIKE a% -> 1", count($r) === 1);

// 7. default value present on row 2
$row = $wpdb->get_row("SELECT * FROM wp_where_test WHERE name = 'beta'", ARRAY_A);
check("beta default status=draft", ($row['status'] ?? '') === 'draft');
check("beta default score=0", ($row['score'] ?? '') === '0');

// 8. UPDATE by PK
$wpdb->query("UPDATE wp_where_test SET score = 99 WHERE id = 1");
$row = $wpdb->get_row("SELECT score FROM wp_where_test WHERE id = 1", ARRAY_A);
check("UPDATE score=99", ($row['score'] ?? '') === '99');

// 9. DELETE by PK
$wpdb->query("DELETE FROM wp_where_test WHERE id = 2");
$r = $wpdb->get_results("SELECT id FROM wp_where_test", ARRAY_A);
check("DELETE -> 2 rows left", count($r) === 2);

// 10. COUNT aggregate
$n = (int) $wpdb->get_var("SELECT COUNT(*) FROM wp_where_test");
check("COUNT(*)=2", $n === 2);

// 11. SHOW COLUMNS reflects schema
$cols = $wpdb->get_results("SHOW COLUMNS FROM wp_where_test", ARRAY_A);
$names = array_column($cols, 'Field');
check("SHOW COLUMNS has all 4", count(array_diff(['id','name','status','score'], $names)) === 0);

// 12. DESCRIBE works
$desc = $wpdb->get_results("DESCRIBE wp_where_test", ARRAY_A);
check("DESCRIBE returns rows", count($desc) === 4);

echo "\n=== t04: {$GLOBALS['pass']} passed, {$GLOBALS['fail']} failed ===\n";
