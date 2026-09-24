# sz-file-combined-consumer-python

Combined Senzing **file load + redo** driver in Python. One process loads a
JSONL file (`add_record`) **and** processes the engine's redo queue
(`get_redo_record` → `process_redo_record`) in a single worker pool, governed
by one `SENZING_REDO_PERCENT` knob. Rejected records go verbatim to a JSONL
**reject file**; an interrupted load resumes with `--skip-lines`.

Python port of the file-input mode of
[`sz_queue_combined_consumer`](https://github.com/brianmacy/sz_queue_combined_consumer)
(Rust). No message broker: the file is the queue, the reject file is the
dead-letter queue, and the resume watermark replaces redelivery.

| redo% | Behavior | Exits |
|---|---|---|
| 0 | Pure file loader. No redo fetcher, zero redo-related calls. | 0 at EOF |
| (0,100) | `\|B\| = clamp(round(N × redo% / 100), 1, N−1)` workers prefer redo, the rest prefer load, with cross-over fallback. After EOF **all** workers drain redo. | 0 once the file is loaded **and** the redo queue is drained |
| 100 | Pure redoer. No file is read. | on SIGINT/SIGTERM |

## Why combined

One worker pool = one DB-connection pool. Splitting loader and redoer into two
processes sizes two pools at launch that cannot borrow from each other: the
redoer lags during load (SYS_EVAL_QUEUE grows) and the loader idles during the
redo tail. Here the same capacity flows to whichever work exists, and a batch
"load this file, then drain redo" is one command with one exit code.

## Install

The driver needs the Senzing runtime (`libSz` + the native Python binding
`senzing_core`, shipped under `/opt/senzing/er/sdk/python` by
`senzingsdk-tools`). The abstract `senzing` SDK comes from PyPI.

```console
pip install .                                    # or: pip install -e '.[dev]'
export PYTHONPATH=/opt/senzing/er/sdk/python     # senzing_core (Linux runtime)
export LD_LIBRARY_PATH=/opt/senzing/er/lib
export SENZING_ENGINE_CONFIGURATION_JSON='{"PIPELINE":{...},"SQL":{"CONNECTION":"..."}}'
```

## Run

```console
# Load a file and drain redo (default 20% of 12 workers prefer redo), exit 0 when done
sz_file_combined_consumer --file /data/records.jsonl

# Pure loader (no redo calls); drain later with a redo-only run
sz_file_combined_consumer --file /data/records.jsonl --redo-percent 0
sz_file_combined_consumer --redo-percent 100        # runs until SIGTERM

# Resume an interrupted load from the printed watermark
sz_file_combined_consumer --file /data/records.jsonl --skip-lines 1234567

# Reprocess the rejects (give the second pass its own reject file)
sz_file_combined_consumer --file /data/records.jsonl.rejected.jsonl \
    --reject-file /data/records.still-rejected.jsonl
```

`examples/records.jsonl` is a small working demo: 20 records sharing a phone,
address and SSN (which enqueues redo), one blank line, one record with an
unregistered data source and one non-JSON line (both land in the reject file).
It needs a repository with the `TEST` data source registered.

Docker (base image `senzing/senzingsdk-runtime`; `WITH_POSTGRES` / `WITH_MSSQL`
select the DB-driver closure, default both):

```console
docker build -t brian/sz_file_combined_consumer .
docker build --build-arg WITH_MSSQL=0 -t brian/sz_file_combined_consumer:pg .
docker run --rm -v /data:/data \
  -e SENZING_ENGINE_CONFIGURATION_JSON \
  -e SENZING_INPUT_FILE=/data/records.jsonl \
  -e SENZING_REDO_PERCENT=20 -e SENZING_THREADS_PER_PROCESS=12 \
  brian/sz_file_combined_consumer
```

## Configuration

Precedence: CLI argument > environment variable > default. Env names match the
sibling drivers.

| Env (CLI) | Default | Meaning |
|---|---|---|
| `SENZING_ENGINE_CONFIGURATION_JSON` | required | engine init JSON (validated as JSON at startup) |
| `SENZING_INPUT_FILE` (`-f`/`--file`) | required iff redo% < 100 | JSONL input, one JSON record per line; blank lines skipped |
| `SENZING_SKIP_LINES` (`--skip-lines`) | 0 | skip the first N physical lines (resume). The driver prints a safe offset at shutdown. |
| `SENZING_REJECT_FILE` (`--reject-file`) | `<input>.rejected.jsonl` | receives every rejected line **verbatim**; created lazily, append mode |
| `SENZING_REDO_PERCENT` (`--redo-percent`) | **20** | ∈ [0,100]; see table above |
| `SENZING_THREADS_PER_PROCESS` (`--threads-per-process`) | **12** | worker threads (0 → CPU count). Scale by processes, not threads. |
| `SENZING_REDO_SLEEP_TIME_IN_SECONDS` (`--redo-sleep-secs`) | 60 | fetcher pause on an empty redo queue (auto-shortened to 2 s while redo is still outstanding) |
| `SENZING_CONFIG_RELOAD_SECS` | 60 | live config-reload poll cadence (0 disables the periodic check) |
| `LONG_RECORD` (`--long-record`) | 300 | long-record threshold, seconds (≥ 1); stats cadence = LONG_RECORD/2 |
| `SENZING_LOG_LEVEL` | info | `debug` / `info` / `warning` / `error` |
| `-i`/`--info` | off | print the WithInfo payload per record |
| `-t`/`--debugTrace` | off | engine debug trace |

Validation is loud (exit 1): redo% ∉ [0,100]; redo% < 100 without a file;
`--file` with redo% = 100; 0 < redo% < 100 with fewer than 2 threads;
`LONG_RECORD` < 1. Exit codes: **1** configuration failure, **255** fatal
runtime error (engine/DB, unreadable input, unwritable reject file) after an
orderly teardown, **0** clean shutdown / EOF + redo drained.

## Concurrency model

* **One Senzing environment per process**, one shared engine handle. Engine
  calls release the GIL and DB connections are owned per-OS-thread inside
  libSz, so N Python threads drive N concurrent engine calls.
* **Reader thread** feeds a bounded work queue (capacity N) — backpressure is
  the flow control. Unparseable lines are rejected at read time; parsed lines
  are dispatched keyed by physical line number.
* **Redo fetcher** (redo% > 0): ONE thread serially calling `get_redo_record()`
  into a tiny bounded queue (|B| + 2). Redo records are already durable in the
  DB, so hoarding them in memory only loses work on crash.
* **Scheduler**: each worker `try_get`s its preferred queue, then the other,
  then backs off briefly — both dequeues are **non-blocking** (a blocking get
  on the preferred queue would defeat the cross-over). No mode state machine:
  full-drain after EOF is emergent.
* **Exit condition** (file mode): the reader hit EOF, every dispatched line
  reported back, and `get_redo_record()` came back empty with zero redo
  outstanding. Emptiness is detected by the fetch itself, never by
  `count_redo_records()` (a table scan).
* **Result consumer** thread counts outcomes, prints WithInfo, writes engine
  rejects to the reject file and maintains the resume watermark.
* **Monitor** (main thread): every `LONG_RECORD/2` s prints `Engine stats: …`
  and a machine-parseable `Combined stats: {…}` JSON line (adds/redos rates,
  `load_active`, measured `redo_share_effective`, derived `mode`), logs
  long-running records, and runs the redo-floor guard (5 flat intervals →
  `redo-floor suspected (possible __REPAIR__ loop)` + raw redo sampling).

### Safe resume offset

Lines complete out of order across N workers, so "lines read" is not a safe
resume point. The driver tracks the highest line L such that **every** line up
to L has completed (added, rejected, or blank) and prints
`safe resume with --skip-lines L` at shutdown. A resume never skips an
unprocessed line; at most a few in-flight lines past L are re-added, which is
safe because `add_record` is idempotent.

## Failure handling

* **Poison line** (not JSON / not an object / missing or non-string
  `DATA_SOURCE`/`RECORD_ID` / non-UTF-8 / engine bad input incl. unknown data
  source / `SENZ0010` retry timeout / `SENZ0082` DQM error) → appended
  verbatim to the reject file, counted, loud warning **with the engine error
  text**, keep loading. The log says *why*, the reject file holds *what*.
* **Poison redo record** (same classes) → warn with the engine error and drop
  (`redos_dropped`); there is no queue to reject to and a DQM-rejected value can
  never succeed.
* **Database connection lost / transient DB error** → **fatal** (orderly
  shutdown, exit 255), never rejected: the database is unhealthy, not the
  record. The watermark keeps the load resumable.
* **Unwritable reject file** → the load still completes (rejects are in the
  log), then exit 255 so the operator notices before the log rotates away.
* **Shutdown** (SIGINT/SIGTERM/SIGHUP or fatal): the reader stops, queued lines
  are left unprocessed (below the watermark), workers finish their current
  engine call within a 10 s grace, the native environment is destroyed within a
  5 s bound. A worker still inside an uninterruptible engine call after the
  grace skips the destroy (leak-on-exit over use-after-free) and the process
  hard-exits.

## Live config reload

Senzing does not pick up a new registered default configuration until
`reinitialize` is called. Every engine thread polls (throttled process-wide to
one `get_default_config_id` round-trip per `SENZING_CONFIG_RELOAD_SECS`), and
any `add_record` / `process_redo_record` error triggers an immediate staleness
check with a single retry. The decision keys on the **registered default**
changing, not on `get_active_config_id()` (unreliable on the settings-JSON init
path, GDEV-4313).

## Memory under sustained load

Under long, high-volume loads the process RSS can balloon far beyond the
engine's live footprint (glibc arena high-water retention of large transient
compare/scoring buffers, tracked as GDEV-4294). The Dockerfile sets
`MALLOC_MMAP_THRESHOLD_=131072` / `MALLOC_TRIM_THRESHOLD_=131072` so large
allocations go through `mmap` and return to the OS on free. Stopgap only; unset
if the per-allocation mmap cost outweighs the RSS benefit. Do **not**
`LD_PRELOAD` jemalloc/tcmalloc — an interposed allocator SIGSEGVs libSz.

## Development

```console
uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python -e '.[dev]' senzing-core
.venv/bin/ruff format --check . && .venv/bin/ruff check . && .venv/bin/mypy
.venv/bin/pytest tests/unit                      # pure logic, no engine

# Real-engine suite (SQLite is enough locally; CI uses PostgreSQL):
SZ=/opt/senzing            # macOS homebrew cask: /opt/homebrew/Caskroom/senzingsdk/<ver>/senzing
cp $SZ/er/resources/templates/G2C.db.template /tmp/G2C.db
export LD_LIBRARY_PATH=$SZ/er/lib DYLD_LIBRARY_PATH=$SZ/er/lib
export SENZING_ENGINE_CONFIGURATION_JSON="{\"PIPELINE\":{\"CONFIGPATH\":\"$SZ/er/etc\",\"RESOURCEPATH\":\"$SZ/er/resources\",\"SUPPORTPATH\":\"$SZ/data\"},\"SQL\":{\"CONNECTION\":\"sqlite3://na:na@/tmp/G2C.db\"}}"
.venv/bin/pytest --cov --cov-report=term-missing
```

The integration suite uses **real** infrastructure only (no mocks): it
registers the `TEST` data source, loads files through the driver in-process,
verifies records via `get_record`, checks `count_redo_records()` reaches 0
after a combined run, reprocesses a reject file, and drives the CLI as a
subprocess for SIGTERM / teardown / exit-code paths. It skips (with the reason)
when `SENZING_ENGINE_CONFIGURATION_JSON` is unset.

CI (`.github/workflows/ci.yml`): ruff + mypy → unit matrix (3.10/3.12/3.13) →
integration on `senzing/senzingsdk-runtime` + PostgreSQL with coverage → Docker
build matrix (postgres / mssql / both). `security.yml` runs pip-audit and
bandit daily; `codeql.yml` runs CodeQL; Dependabot covers pip, GitHub Actions
and Docker.

## Layout

| Module | Role |
|---|---|
| `config.py` | CLI/env resolution, topology validation, `redo_preferring_count` |
| `record.py` | record parsing, error classification, redo logging ids |
| `channels.py` | bounded closable queues (the Rust mpsc stand-in) |
| `worker.py` | worker pool: pure-load / pure-redo / mixed dispatch, long-record monitors |
| `redo.py` | the single redo fetcher (drain-tail re-probe, file-mode drain exit) |
| `file_loader.py` | reader, result consumer, resume watermark, reject sink, run loop |
| `pure_redoer.py` | redo% = 100 run loop |
| `monitor.py` | `Engine stats:` / `Combined stats:` tick, floor guard |
| `config_reload.py` | live registered-default reload |
| `stats.py` | counters, flags, ticker, EWMA, floor guard, status line |
| `runtime.py` | logging, environment bring-up, signals, teardown, exit codes |

## License

Apache-2.0
