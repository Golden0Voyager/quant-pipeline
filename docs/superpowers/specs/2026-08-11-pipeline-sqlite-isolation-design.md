# Pipeline SQLite Isolation Repair

## Goal

Make the daily pipeline reliable when data coverage is prioritized: no SQLite
connection crosses a worker-thread boundary, no current single-task result is
reported as failed because of historical failures, and audit writes tolerate a
brief write lock.

## Scope

1. Run database-writing daily-pipeline stages serially. The existing task
   implementations combine fetch and write work and receive one shared database
   interface, so executing them concurrently can reuse a SQLite connection in a
   different thread.
2. Keep per-task and batch result reporting, but make the TUI show only failures
   created by the command it has just launched.
3. Retry short-lived `database is locked` failures when inserting the
   `ingestion_runs` audit row. The retry changes audit reliability only; it does
   not turn a failed data write into success.

## Non-goals

- Do not set `check_same_thread=False` on the shared `DatabaseManager` write
  connection. That hides the ownership violation and does not make concurrent
  writes safe.
- Do not redesign all data tasks into fetch and writer queues in this change.
- Do not classify remote Eastmoney or financial-history transport failures as
  successful. They remain explicit degraded/failed results and preserve prior
  data.

## Behaviour

Daily runs with `workers > 1` may retain the option for future read-only work,
but both existing parallel stages are invoked with one worker until tasks have a
separate fetch/write boundary. This eliminates cross-thread use of the shared
database object and avoids write contention between those stages.

The TUI records a command start time before launching a subprocess. If the
subprocess fails, it queries failed audit rows only from that start time onward;
older failures are not attributed to the selected command.

Audit writes set a busy timeout and retry only lock errors with bounded backoff.
After retries are exhausted, the existing warning remains and the task result is
otherwise unchanged.

## Verification

- A daily-pipeline regression test proves parallel configuration sends
  database-writing stage tasks through the serial runner.
- A TUI regression test seeds an older failed audit record and proves it is not
  displayed for a later command.
- Provider tests prove a transient audit lock is retried and a non-lock error is
  not retried.
- Run focused pytest suites, Ruff, then the project's unit test marker.
