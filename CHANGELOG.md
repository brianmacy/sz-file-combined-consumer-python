# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.1] - 2026-09-24

No behaviour change; CI and code-quality only.

### Changed
- Four small value-mapping helpers (`ParseError` message, `classify_error`,
  `redo_preferring_count`, `mode`) use explicit returns instead of `match`
  blocks: CodeQL does not model `match` exhaustiveness and flagged them as
  mixed/uninitialized returns. Pattern-matching dispatch elsewhere is unchanged.
- CI: `actions/upload-artifact` 4.6.2 -> 7.0.1 (Dependabot #1).

## [0.1.0] - 2026-09-23

Initial Python port of the **file-input mode** of
[`sz_queue_combined_consumer`](https://github.com/brianmacy/sz_queue_combined_consumer)
(Rust), with the combined redo scheduler carried over so a file load and its
redo drain run in one process.

### Added
- JSONL file loader: one JSON record per line -> `add_record`, N worker threads,
  blank lines skipped, `--skip-lines` resume with a contiguous-completion
  watermark printed at shutdown (`safe resume with --skip-lines N`).
- Reject file (`--reject-file`, default `<input>.rejected.jsonl`): every
  rejected line — unparseable JSON, missing `DATA_SOURCE`/`RECORD_ID`, engine
  bad input, `SENZ0010` retry timeout, `SENZ0082` — appended **verbatim**, one
  unbuffered write per line, created lazily. Reprocess with `--file <reject>`.
  Unwritable reject file -> load completes, exit 255 (bodies are in the log).
- Concurrent redo (`--redo-percent`, default 20): `|B| = clamp(round(N·redo%/100), 1, N−1)`
  redo-preferring workers + a single redo fetcher run alongside the load; after
  EOF every worker drains redo and the process exits 0 once
  `get_redo_record()` is empty with nothing outstanding. `0` = pure loader
  (zero redo calls), `100` = pure redoer (no file; runs until SIGTERM).
- Error policy identical to the Rust driver: bad input / timeout / SENZ0082 ->
  reject (load) or drop (redo) and keep going; everything else (incl. DB
  connection lost / transient) -> fatal, orderly shutdown, exit 255.
- `Engine stats:` + machine-parseable `Combined stats: {...}` lines every
  `LONG_RECORD/2` seconds; long-record monitor; redo-floor (`__REPAIR__` loop)
  guard with raw redo-record sampling.
- Live engine-config reload (periodic `SENZING_CONFIG_RELOAD_SECS` + error-driven
  retry-once), keyed on the registered default changing.
- SIGINT/SIGTERM/SIGHUP graceful shutdown with a 10 s worker grace and a 5 s
  bounded native teardown (leak-on-exit when a worker is stuck in an engine call).
- Exit codes: 0 clean / EOF + drained, 1 configuration error, 255 fatal.
- CI: ruff + mypy (strict), unit matrix (3.10/3.12/3.13), real-engine
  integration suite on `senzing/senzingsdk-runtime` + PostgreSQL with coverage,
  Docker build matrix (postgres / mssql / both), pip-audit + bandit, CodeQL,
  Dependabot (pip, actions, docker).
- Dockerfile on `senzing/senzingsdk-runtime:4.4.1` with `WITH_POSTGRES` /
  `WITH_MSSQL` build args and the GDEV-4294 glibc malloc mitigation.

### Fixed
- CI: quoted the workflow step name that made `ci.yml` unparseable (the parse
  error also broke the Dependabot `github_actions` updater runs).
- CI: removed `assert` statements from package code so bandit's B101 check
  passes on `sz_file_combined_consumer`.
- CI: pip-audit now audits the dependency closure without the local package —
  `--strict` and editable-install skipping conflicted, so the local editable
  project is no longer installed into the audit environment.
- CI: made the Docker build step shellcheck-clean.
