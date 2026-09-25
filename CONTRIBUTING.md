# Contributing

## Setup
```bash
uv sync                 # Python 3.12+, installs dev tools
make up                 # Redis (56384) + Postgres (55437) in Docker
make check              # ruff + mypy --strict + all tests
```
Integration tests use their own `relay_test` database and a random Redis key namespace per test.
If Redis/Postgres aren't reachable they **skip** (with a reason) instead of failing. Run `make up`.

## Rules
- Every status change goes through the guarded SQL in `store.py`; never `UPDATE jobs SET status`
  anywhere else.
- **Postgres before Redis**: change the row first, then make work visible in a stream
  (see `docs/delivery-guarantees.md`).
- New failure modes get a test in `tests/integration/test_m2_failures.py` style: inject the
  failure, then assert that every job reaches exactly one terminal state.
- Benchmark numbers only go into docs with the script, commit and machine that produced them.
- Conventional commits (`feat:`, `fix:`, `perf:`, `test:`, `docs:`), small and focused.
