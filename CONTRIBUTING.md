# Contributing

Thanks for interest in the project. It is a single-file WordPress database
drop-in (`db.php`, roughly 5k lines) plus a Docker test bench, so the bar for
a useful contribution is low and the loop is fast.

## Before You Start

- Read the [README](README.md) first: architecture, configuration, and the
  measured limitations are all there.
- This engine is experimental and explicitly **not for production**. Proposals
  that assume production traffic (replication, transactions across tables,
  concurrent writers across machines) are out of scope by design.

## Development Setup

Everything runs through the test bench:

```bash
./tests/run.sh          # all suites
./tests/run.sh t03      # only suites matching "t03"
```

The script starts WordPress 6 on PHP 8.1 with the drop-in mounted from the
working tree, installs WordPress and the plugin fixtures on first boot, and
runs the suites through wp-cli. Data lands in `html_db/` inside the container
volume; the MySQL container exists only to satisfy core's bootstrap and stays
empty.

Requirements: Docker with Compose. Nothing else on the host.

## Making Changes

1. Fork the repository and create a branch off `main`.
2. Keep `db.php` self-contained: no Composer dependencies, no files outside
   the single drop-in. The engine must stay a true drop-in.
3. Follow the existing style: `declare(strict_types=1)`, PHPDoc on classes
   and public methods, section banners between namespaces.
4. If you touch SQL handling, add or extend a suite under `tests/` so the
   change is covered by `run.sh`.
5. Run the full bench (`./tests/run.sh`) and make sure every suite reports
   `0 failed` before opening a pull request.

## Pull Requests

- One logical change per pull request.
- Describe what changed and why; for performance claims, include the bench
  numbers (the `tests/bench_*.php` scripts are there for this).
- If behavior or configuration changes, update the README in the same pull
  request.
- The CI of this project is the bench itself: a green `run.sh` output in the
  pull request description is what reviewers look for.

## Bug Reports

Use the issue template. The most useful reports include the PHP version, the
WordPress version, the failing query (enable `WP_DEBUG` and check the
`[HtmlDB]` log lines), and the state of the affected table directory
(`_meta.json`, WAL size).
