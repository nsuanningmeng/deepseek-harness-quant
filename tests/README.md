# Isolated unit tests

Run the suite with the database guard, including while the application is updating:

```sh
.venv/bin/python validation/run_isolated_tests.py
```

Run specific modules with the same guard:

```sh
.venv/bin/python validation/run_isolated_tests.py tests.test_research_integrity tests.test_research_delivery
```

The runner installs protection before discovery and test imports. It rejects SQLite
connections or attachments to any database within this checkout before file I/O,
including read-only URIs, aliases and background workers. Temporary-directory and
in-memory fixtures remain available. Connection attempts caught by application
code still fail the isolation check. The guard remains active while known
background probes/verifiers finish and through process exit.

Results and blocked call stacks are written to `runtime/research/test-isolation.json`.
Success requires passing tests, zero project database attempts, and no unfinished
background checks. Do not bypass a failing guard or weaken product validation;
patch the test's dependency or point it at an explicit temporary fixture instead.

The same runner denies connections to production ports 8787/3080 and non-loopback
destinations before network I/O. Temporary loopback servers remain usable. This
also prevents a test from indirectly asking the running application to read its
real databases. Network attempts have a separate counter and fail the run.

Environment variables alone do not isolate defaults already bound during module
import, such as `data.cache.DEFAULT_DB` and default `DailyCache()` construction.
Also isolate helper calls which may start asynchronous production verifiers.
