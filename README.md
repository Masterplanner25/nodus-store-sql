# nodus-store-sql

> **Status:** v0.1.0 — published on [PyPI](https://pypi.org/project/nodus-store-sql/).

SQLAlchemy-backed persistence adapters for Nodus runs, events, and jobs.

Provides sync and async stores for the three core durable state surfaces:
`RunStore` (execution lifecycle), `EventStore` (append-only audit trail),
`JobStore` (background job queue with atomic claiming).

## Install

```bash
pip install nodus-store-sql           # sync only
pip install "nodus-store-sql[async]"  # + async support
```

## Usage

```python
from nodus_store_sql import SqlStore, SqlStoreConfig, RunRecord
from datetime import datetime, timezone

store = SqlStore(SqlStoreConfig(database_url="postgresql://user:pw@host/db"))
store.create_all()  # creates nodus_runs, nodus_events, nodus_jobs tables

now = datetime.now(timezone.utc)
store.runs.create(RunRecord("run-001", "flow", "pending", None, "trace-abc",
                             None, "user-1", None, 1, now, now))
run = store.runs.get("run-001")
```

## Async

```python
from nodus_store_sql import AsyncSqlStore, SqlStoreConfig

store = AsyncSqlStore(SqlStoreConfig(database_url="postgresql+asyncpg://..."))
await store.create_all()
await store.runs.create(run_record)
```

## Tables

| Table | Store | Purpose |
|---|---|---|
| `nodus_runs` | `RunStore` | Execution lifecycle with optimistic locking |
| `nodus_events` | `EventStore` | Append-only audit trail with causal chains |
| `nodus_jobs` | `JobStore` | Background job queue with atomic claiming |
