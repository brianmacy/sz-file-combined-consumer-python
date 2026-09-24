"""Combined Senzing file load + redo driver.

One process runs both roles in a single worker pool: the load role (JSONL file
-> ``add_record``) and the redo role (``get_redo_record`` ->
``process_redo_record``), governed by a single ``SENZING_REDO_PERCENT`` knob in
``[0, 100]``:

* ``0``   - pure file loader: no redo fetcher is started, zero redo calls.
* ``100`` - pure redoer: no file is read; runs until SIGINT/SIGTERM.
* ``(0, 100)`` - combined: ``|B| = clamp(round(N * redo% / 100), 1, N-1)``
  workers prefer redo, the rest prefer load, with cross-over fallback when the
  preferred channel is empty. Once the file is fully loaded ALL workers fall
  into redo and the process exits when the redo queue is drained.

Python port of the file-input mode of ``sz_queue_combined_consumer`` (Rust),
with the redo scheduler carried over so a file load and its redo drain run in
one process.
"""

INSTANCE_NAME = "sz_file_combined_consumer"
"""Instance/module name passed to the Senzing environment."""

__version__ = "0.1.0"
