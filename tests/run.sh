#!/usr/bin/env bash
# HtmlDB test bench: spins up WordPress on the HTML engine and runs all suites.
#
#   ./tests/run.sh          # run all suites
#   ./tests/run.sh t02      # run suites matching "t02"
#
# Requires Docker with compose. First run pulls the WordPress image and
# downloads wp-cli; the WP install lives in a named volume and is reused.
set -euo pipefail

cd "$(dirname "$0")"

WP_URL=${WP_URL:-http://localhost:8080}
ADMIN_PASS=${ADMIN_PASS:-benchpass123}

if [ ! -f wp-cli.phar ]; then
    echo "==> downloading wp-cli"
    curl -sSLo wp-cli.phar https://raw.githubusercontent.com/wp-cli/builds/gh-pages/phar/wp-cli.phar
fi

echo "==> starting containers"
docker compose up -d

# Wait for WordPress to answer.
echo "==> waiting for $WP_URL"
for _ in $(seq 1 60); do
    if curl -fsS -o /dev/null "$WP_URL/wp-login.php"; then break; fi
    sleep 1
done

# wp-cli runs as www-data, the same user Apache runs as. Running it as root
# would create engine files (chunks, WAL, .seq) owned by root, and the web
# server could no longer write them - core updates and option writes then
# fail with EACCES. Same-user CLI keeps the whole storage consistent.
wp() {
    docker compose exec -T -u www-data wp php /var/www/html/wp-content/htmldb-tests/wp-cli.phar \
        "$@" --path=/var/www/html
}

# First boot: install WordPress against the drop-in (MySQL container is only
# here so core's expectations hold; all data lands in html_db/).
if ! wp core is-installed >/dev/null 2>&1; then
    echo "==> installing WordPress"
    wp core install --url="$WP_URL" --title="HtmlDB Bench" \
        --admin_user=admin --admin_password="$ADMIN_PASS" \
        --admin_email=admin@example.com --skip-email
    wp plugin install advanced-custom-fields elementor --activate 2>/dev/null || true
fi

PATTERN=${1:-t0}
STATUS=0
for suite in t01_basic t02_content t03_plugins t04_ddl; do
    if [[ "$suite" != *"$PATTERN"* ]]; then continue; fi
    echo "==> $suite"
    OUT=$(wp eval-file "/var/www/html/wp-content/htmldb-tests/$suite.php" 2>&1) || true
    echo "$OUT" | grep -E '^(FAIL|===? )' || true
    if echo "$OUT" | grep -q '^FAIL' || ! echo "$OUT" | grep -q ' 0 failed'; then
        STATUS=1
    fi
done

# Safety net: if a previous run left root-owned files (older bench versions
# ran wp-cli as root), hand the storage back to the web server user.
docker compose exec -T wp chown -R www-data:www-data /var/www/html/wp-content/html_db 2>/dev/null || true

exit $STATUS
