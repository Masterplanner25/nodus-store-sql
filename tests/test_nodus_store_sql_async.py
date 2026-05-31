from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from nodus_store_sql import (
    AsyncSqlStore,
    EventRecord,
    JobRecord,
    OptimisticLockError,
    RecordNotFoundError,
    RunRecord,
    SqlStoreConfig,
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def build_store() -> AsyncSqlStore:
    store = AsyncSqlStore(SqlStoreConfig(database_url="sqlite+aiosqlite:///:memory:"))
    await store.create_all()
    return store


def make_run(run_id: str, *, status: str = "pending", trace_id: str | None = None) -> RunRecord:
    now = utcnow()
    return RunRecord(run_id, "flow", status, None, trace_id, None, None, None, 1, now, now)


def make_event(event_id: str, *, run_id: str = "r1", trace_id: str = "t1",
               sequence_index: int = 1) -> EventRecord:
    return EventRecord(event_id, "step.done", {}, "flow", run_id, trace_id, None,
                       None, sequence_index, utcnow())


def make_job(job_id: str, *, status: str = "pending",
             scheduled_for: datetime | None = None) -> JobRecord:
    now = utcnow()
    return JobRecord(job_id, "task.run", status, {"x": 1}, None, "t1", None,
                     0, 3, scheduled_for, None, None, None, now, now)


# ---------------------------------------------------------------------------
# AsyncRunStore
# ---------------------------------------------------------------------------

async def test_async_run_round_trip_and_update() -> None:
    store = await build_store()
    r = make_run("ar1")
    await store.runs.create(r)
    loaded = await store.runs.get("ar1")
    assert loaded is not None
    assert loaded.status == "pending"
    updated = await store.runs.update(replace(loaded, status="running"))
    assert updated.status == "running"
    assert updated.version == 2


async def test_async_run_get_missing_returns_none() -> None:
    store = await build_store()
    assert await store.runs.get("nonexistent") is None


async def test_async_stale_run_update_raises() -> None:
    store = await build_store()
    r = make_run("ar2")
    await store.runs.create(r)
    current = await store.runs.get("ar2")
    assert current is not None
    await store.runs.update(replace(current, status="running"))
    with pytest.raises(OptimisticLockError):
        await store.runs.update(replace(current, status="completed"))


async def test_async_run_set_status() -> None:
    store = await build_store()
    await store.runs.create(make_run("ar3"))
    updated = await store.runs.set_status("ar3", "completed", completed_at=utcnow())
    assert updated.status == "completed"


async def test_async_run_set_status_missing_raises() -> None:
    store = await build_store()
    with pytest.raises(RecordNotFoundError):
        await store.runs.set_status("missing", "completed")


async def test_async_run_list_by_trace() -> None:
    store = await build_store()
    await store.runs.create(make_run("at1", trace_id="trace-x"))
    await store.runs.create(make_run("at2", trace_id="trace-x"))
    await store.runs.create(make_run("at3", trace_id="other"))
    results = await store.runs.list_by_trace("trace-x")
    assert len(results) == 2


async def test_async_run_list_by_status() -> None:
    store = await build_store()
    await store.runs.create(make_run("as1", status="pending"))
    await store.runs.create(make_run("as2", status="running"))
    pending = await store.runs.list_by_status("pending")
    assert len(pending) == 1
    assert pending[0].status == "pending"


# ---------------------------------------------------------------------------
# AsyncEventStore
# ---------------------------------------------------------------------------

async def test_async_event_append_and_list_ordering() -> None:
    store = await build_store()
    e1 = make_event("ae1", sequence_index=1)
    e2 = make_event("ae2", sequence_index=2)
    await store.events.append(e1)
    await store.events.append(e2)
    results = await store.events.list_for_run("r1")
    assert [e.event_id for e in results] == ["ae1", "ae2"]


async def test_async_event_get_missing_returns_none() -> None:
    store = await build_store()
    assert await store.events.get("nonexistent") is None


async def test_async_event_append_batch() -> None:
    store = await build_store()
    batch = [make_event(f"ab{i}", sequence_index=i + 1) for i in range(3)]
    await store.events.append_batch(batch)
    stored = await store.events.list_for_run("r1")
    assert len(stored) == 3


async def test_async_event_list_for_trace() -> None:
    store = await build_store()
    await store.events.append(make_event("atr1", run_id="x", trace_id="shared"))
    await store.events.append(make_event("atr2", run_id="y", trace_id="shared"))
    results = await store.events.list_for_trace("shared")
    assert len(results) == 2


# ---------------------------------------------------------------------------
# AsyncJobStore
# ---------------------------------------------------------------------------

async def test_async_job_claim_pending_once() -> None:
    store = await build_store()
    await store.jobs.create(make_job("aj1"))
    first = await store.jobs.claim_pending("aj1", "worker-a")
    second = await store.jobs.claim_pending("aj1", "worker-b")
    assert first is not None
    assert first.claimed_by == "worker-a"
    assert second is None


async def test_async_job_list_pending_due_filter() -> None:
    store = await build_store()
    now = utcnow()
    await store.jobs.create(make_job("adj1"))
    await store.jobs.create(make_job("adj2", scheduled_for=now + timedelta(hours=1)))
    due = await store.jobs.list_pending(due_before=now)
    assert len(due) == 1
    assert due[0].job_id == "adj1"


async def test_async_job_set_status() -> None:
    store = await build_store()
    await store.jobs.create(make_job("ajs1"))
    updated = await store.jobs.set_status("ajs1", "completed", completed_at=utcnow())
    assert updated.status == "completed"


# ---------------------------------------------------------------------------
# AsyncSqlStore misc
# ---------------------------------------------------------------------------

async def test_async_create_all_idempotent() -> None:
    store = await build_store()
    await store.create_all()  # second call must not raise


async def test_async_session_rollback_on_error() -> None:
    store = await build_store()
    from nodus_store_sql.orm import RunModel
    try:
        async with store.session() as session:
            session.add(RunModel(run_id="aroll", run_type="flow", status="pending",
                                 state_payload=None, trace_id=None, correlation_id=None,
                                 owner_id=None, scope=None, version=1,
                                 created_at=utcnow(), updated_at=utcnow(), completed_at=None))
            raise ValueError("forced")
    except ValueError:
        pass
    assert await store.runs.get("aroll") is None
