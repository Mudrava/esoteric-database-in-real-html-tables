## What changed

One paragraph: what this pull request does and why.

## Scope check

- [ ] `db.php` stays a single self-contained file (no new runtime files, no Composer)
- [ ] Behavior and configuration changes are reflected in the README
- [ ] SQL handling changes come with a new or extended suite under `tests/`

## Bench output

Paste the tail of `./tests/run.sh` (every suite must report `0 failed`):

```
== T01: NN passed, 0 failed ==
```

## Performance notes

If this touches the read/write path, include before/after numbers from
`tests/bench_perf.php` or `tests/bench_scale.php`. Otherwise delete this
section.
