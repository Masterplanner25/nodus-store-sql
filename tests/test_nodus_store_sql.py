from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from nodus_store_sql import (
    EventRecord,
    JobRecord,
    OptimisticLockError,
    RecordNotFoundError,
    RunRecord,
    SqlStore,
    SqlStoreConfig,
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def build_store() -> SqlStore:
    store = SqlStore(SqlStoreConfig(database_url="sqlite:///:memory:"))
    store.create_all()
    return store


def make_run(run_id: str, *, status: str = "pending", trace_id: str | None = None,
             owner_id: str | None = None) -> RunRecord:
    now = utcnow()
    return RunRecord(run_id, "flow", status, None, trace_id, None, owner_id, None, 1, now, now)


def make_event(event_id: str, *, run_id: str = "r1", trace_id: str = "t1",
               sequence_index: int = 1, parent_event_id: str | None = None) -> EventRecord:
    return EventRecord(event_id, "step.done", {}, "flow", run_id, trace_id, None,
                       parent_event_id, sequence_index, utcnow())


def make_job(job_id: str, *, status: str = "pending", scheduled_for: datetime | None = None) -> JobRecord:
    now = utcnow()
    return JobRecord(job_id, "task.run", status, {"x": 1}, None, "t1", None,
                     0, 3, scheduled_for, None, None, None, now, now)


# ---------------------------------------------------------------------------
# Original 6 scaffold tests (unchanged)
# ---------------------------------------------------------------------------

def test_run_round_trip_and_update() -> None:
    store = build_store()
    created = RunRecord("r1", "flow", "pending", {"x": 1}, "t1", "c1", "u1", "tenant", 1, utcnow(), utcnow())
    store.runs.create(created)
    loaded = store.runs.get("r1")
    assert loaded is not None
    assert loaded.state_payload == {"x": 1}
    updated = store.runs.update(replace(loaded, status="running"))
    assert updated.status == "running"
    assert updated.version == 2


def test_stale_run_update_raises() -> None:
    store = build_store()
    created = RunRecord("r2", "flow", "pending", None, None, None, None, None, 1, utcnow(), utcnow())
    store.runs.create(created)
    current = store.runs.get("r2")
    assert current is not None
    store.runs.update(replace(current, status="running"))
    with pytest.raises(OptimisticLockError):
        store.runs.update(replace(current, status="completed"))


def test_event_append_and_list_order() -> None:
    store = build_store()
    e1 = EventRecord("e1", "step.started", {"a": 1}, "flow", "r1", "t1", "c1", None, 1, utcnow())
    e2 = EventRecord("e2", "step.done", {"a": 2}, "flow", "r1", "t1", "c1", "e1", 2, utcnow())
    store.events.append(e1)
    store.events.append(e2)
    by_run = store.events.list_for_run("r1")
    by_trace = store.events.list_for_trace("t1")
    assert [event.event_id for event in by_run] == ["e1", "e2"]
    assert [event.event_id for event in by_trace] == ["e1", "e2"]


def test_job_claim_pending_succeeds_once() -> None:
    store = build_store()
    pending = JobRecord("j1", "task.run", "pending", {"x": 1}, None, "t1", "c1", 0, 3,
                        None, None, None, None, utcnow(), utcnow())
    store.jobs.create(pending)
    first = store.jobs.claim_pending("j1", "worker-a")
    second = store.jobs.claim_pending("j1", "worker-b")
    assert first is not None
    assert first.status == "claimed"
    assert first.claimed_by == "worker-a"
    assert second is None


def test_job_payload_round_trip() -> None:
    store = build_store()
    job = JobRecord("j2", "task.run", "pending", {"nested": {"ok": True}}, "u1", "t2", "c2",
                    1, 5, utcnow(), None, None, None, utcnow(), utcnow())
    store.jobs.create(job)
    loaded = store.jobs.get("j2")
    assert loaded is not None
    assert loaded.payload == {"nested": {"ok": True}}


def test_store_session_context_commits() -> None:
    store = build_store()
    with store.session() as session:
        session.add(
            __import__("nodus_store_sql.orm").orm.RunModel(
                run_id="r3", run_type="flow", status="pending", state_payload=None,
                trace_id=None, correlation_id=None, owner_id=None, scope=None,
                version=1, created_at=utcnow(), updated_at=utcnow(), completed_at=None,
            )
        )
    assert store.runs.get("r3") is not None


# ---------------------------------------------------------------------------
# RunStore — new tests
# ---------------------------------------------------------------------------

def test_run_get_missing_returns_none() -> None:
    store = build_store()
    assert store.runs.get("nonexistent") is None


def test_run_set_status_to_completed() -> None:
    store = build_store()
    store.runs.create(make_run("r10"))
    now = utcnow()
    updated = store.runs.set_status("r10", "completed", completed_at=now)
    assert updated.status == "completed"
    assert updated.completed_at is not None


def test_run_set_status_on_missing_raises() -> None:
    store = build_store()
    with pytest.raises(RecordNotFoundError):
        store.runs.set_status("missing", "completed")


def test_run_list_by_trace_ordering() -> None:
    store = build_store()
    store.runs.create(make_run("ra", trace_id="trace-abc"))
    store.runs.create(make_run("rb", trace_id="trace-abc"))
    store.runs.create(make_run("rc", trace_id="other"))
    results = store.runs.list_by_trace("trace-abc")
    assert len(results) == 2
    assert all(r.trace_id == "trace-abc" for r in results)


def test_run_list_by_trace_empty() -> None:
    store = build_store()
    assert store.runs.list_by_trace("no-such-trace") == []


def test_run_list_by_status() -> None:
    store = build_store()
    store.runs.create(make_run("s1", status="pending"))
    store.runs.create(make_run("s2", status="running"))
    store.runs.create(make_run("s3", status="pending"))
    pending = store.runs.list_by_status("pending")
    assert len(pending) == 2
    assert all(r.status == "pending" for r in pending)


def test_run_list_by_status_limit() -> None:
    store = build_store()
    for i in range(5):
        store.runs.create(make_run(f"lim{i}", status="pending"))
    results = store.runs.list_by_status("pending", limit=3)
    assert len(results) == 3


def test_run_list_by_owner() -> None:
    store = build_store()
    store.runs.create(make_run("o1", owner_id="user-1"))
    store.runs.create(make_run("o2", owner_id="user-1"))
    store.runs.create(make_run("o3", owner_id="user-2"))
    results = store.runs.list_by_owner("user-1")
    assert len(results) == 2
    assert all(r.owner_id == "user-1" for r in results)


def test_run_version_increments_on_each_update() -> None:
    store = build_store()
    store.runs.create(make_run("v1"))
    r = store.runs.get("v1")
    assert r is not None
    r2 = store.runs.update(replace(r, status="running"))
    assert r2.version == 2
    r3 = store.runs.update(replace(r2, status="completed"))
    assert r3.version == 3


def test_multiple_runs_same_trace() -> None:
    store = build_store()
    for i in range(4):
        store.runs.create(make_run(f"mt{i}", trace_id="shared"))
    assert len(store.runs.list_by_trace("shared")) == 4


# ---------------------------------------------------------------------------
# EventStore — new tests
# ---------------------------------------------------------------------------

def test_event_get_missing_returns_none() -> None:
    store = build_store()
    assert store.events.get("nonexistent") is None


def test_event_list_for_run_empty() -> None:
    store = build_store()
    assert store.events.list_for_run("no-such-run") == []


def test_event_list_for_trace_cross_run() -> None:
    store = build_store()
    e1 = make_event("ec1", run_id="run-a", trace_id="shared-trace")
    e2 = make_event("ec2", run_id="run-b", trace_id="shared-trace", sequence_index=2)
    store.events.append(e1)
    store.events.append(e2)
    results = store.events.list_for_trace("shared-trace")
    assert {e.event_id for e in results} == {"ec1", "ec2"}


def test_event_list_for_run_limit() -> None:
    store = build_store()
    for i in range(5):
        store.events.append(make_event(f"el{i}", sequence_index=i + 1))
    results = store.events.list_for_run("r1", limit=3)
    assert len(results) == 3


def test_event_append_batch() -> None:
    store = build_store()
    batch = [make_event(f"eb{i}", sequence_index=i + 1) for i in range(4)]
    returned = store.events.append_batch(batch)
    assert returned == batch
    stored = store.events.list_for_run("r1")
    assert len(stored) == 4


def test_event_parent_chain() -> None:
    store = build_store()
    e1 = make_event("ep1", sequence_index=1)
    e2 = make_event("ep2", sequence_index=2, parent_event_id="ep1")
    store.events.append(e1)
    store.events.append(e2)
    e2_loaded = store.events.get("ep2")
    assert e2_loaded is not None
    assert e2_loaded.parent_event_id == "ep1"


# ---------------------------------------------------------------------------
# JobStore — new tests
# ---------------------------------------------------------------------------

def test_job_get_missing_returns_none() -> None:
    store = build_store()
    assert store.jobs.get("nonexistent") is None


def test_job_set_status() -> None:
    store = build_store()
    store.jobs.create(make_job("js1"))
    updated = store.jobs.set_status("js1", "completed", completed_at=utcnow())
    assert updated.status == "completed"
    assert updated.completed_at is not None


def test_job_set_status_on_missing_raises() -> None:
    store = build_store()
    with pytest.raises(RecordNotFoundError):
        store.jobs.set_status("missing", "completed")


def test_job_list_pending_due_filter() -> None:
    store = build_store()
    now = utcnow()
    future = now + timedelta(hours=1)
    store.jobs.create(make_job("jd1"))                      # no scheduled_for → always due
    store.jobs.create(make_job("jd2", scheduled_for=future))  # future → not yet due
    due = store.jobs.list_pending(due_before=now)
    assert len(due) == 1
    assert due[0].job_id == "jd1"


def test_job_list_pending_limit() -> None:
    store = build_store()
    for i in range(5):
        store.jobs.create(make_job(f"jl{i}"))
    results = store.jobs.list_pending(limit=2)
    assert len(results) == 2


def test_job_list_pending_excludes_claimed() -> None:
    store = build_store()
    store.jobs.create(make_job("jp1"))
    store.jobs.create(make_job("jp2"))
    store.jobs.claim_pending("jp1", "worker")
    pending = store.jobs.list_pending()
    assert all(j.job_id != "jp1" for j in pending)
    assert len(pending) == 1


def test_job_update_increments_attempt_count() -> None:
    store = build_store()
    store.jobs.create(make_job("ja1"))
    job = store.jobs.get("ja1")
    assert job is not None
    updated = store.jobs.update(replace(job, attempt_count=job.attempt_count + 1))
    assert updated.attempt_count == 1


# ---------------------------------------------------------------------------
# SqlStore misc
# ---------------------------------------------------------------------------

def test_create_all_idempotent() -> None:
    store = build_store()
    store.create_all()  # calling twice should not raise


def test_session_rollback_on_error() -> None:
    store = build_store()
    try:
        with store.session() as session:
            from nodus_store_sql.orm import RunModel
            session.add(RunModel(run_id="rollback-test", run_type="flow", status="pending",
                                 state_payload=None, trace_id=None, correlation_id=None,
                                 owner_id=None, scope=None, version=1,
                                 created_at=utcnow(), updated_at=utcnow(), completed_at=None))
            raise ValueError("forced error")
    except ValueError:
        pass
    assert store.runs.get("rollback-test") is None
