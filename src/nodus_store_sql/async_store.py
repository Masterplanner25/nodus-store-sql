from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from datetime import datetime, timezone
from typing import AsyncGenerator

from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .errors import OptimisticLockError, RecordNotFoundError
from .models import EventRecord, JobRecord, RunRecord, SqlStoreConfig
from .orm import Base, EventModel, JobModel, RunModel


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AsyncSqlStore:
    def __init__(self, config: SqlStoreConfig) -> None:
        self.config = config
        self.engine = create_async_engine(config.database_url, echo=config.echo)
        self._session_factory = async_sessionmaker(
            self.engine, autoflush=False, expire_on_commit=False
        )
        self.runs = AsyncRunStore(self._session_factory)
        self.events = AsyncEventStore(self._session_factory)
        self.jobs = AsyncJobStore(self._session_factory)

    async def create_all(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        async with self._session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise


class AsyncRunStore:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def create(self, record: RunRecord) -> RunRecord:
        async with self._session_factory() as session:
            session.add(_run_model_from_record(record))
            await session.commit()
            return record

    async def get(self, run_id: str) -> RunRecord | None:
        async with self._session_factory() as session:
            model = await session.get(RunModel, run_id)
            return _run_record_from_model(model) if model is not None else None

    async def update(self, record: RunRecord) -> RunRecord:
        new_version = record.version + 1
        now = _utcnow()
        async with self._session_factory() as session:
            stmt = (
                update(RunModel)
                .where(and_(RunModel.run_id == record.run_id, RunModel.version == record.version))
                .values(
                    run_type=record.run_type,
                    status=record.status,
                    state_payload=record.state_payload,
                    trace_id=record.trace_id,
                    correlation_id=record.correlation_id,
                    owner_id=record.owner_id,
                    scope=record.scope,
                    version=new_version,
                    updated_at=now,
                    completed_at=record.completed_at,
                )
            )
            result = await session.execute(stmt)
            if result.rowcount != 1:
                await session.rollback()
                raise OptimisticLockError(f"Run {record.run_id!r} failed optimistic version check.")
            await session.commit()
        return replace(record, version=new_version, updated_at=now)

    async def set_status(self, run_id: str, status: str, *, completed_at: datetime | None = None) -> RunRecord:
        record = await self.get(run_id)
        if record is None:
            raise RecordNotFoundError(f"Unknown run {run_id!r}")
        return await self.update(replace(record, status=status, completed_at=completed_at))

    async def list_by_trace(self, trace_id: str) -> list[RunRecord]:
        async with self._session_factory() as session:
            rows = (await session.scalars(
                select(RunModel).where(RunModel.trace_id == trace_id).order_by(RunModel.created_at.asc())
            )).all()
            return [_run_record_from_model(row) for row in rows]

    async def list_by_status(self, status: str, *, limit: int | None = None) -> list[RunRecord]:
        async with self._session_factory() as session:
            stmt = select(RunModel).where(RunModel.status == status).order_by(RunModel.created_at.asc())
            if limit is not None:
                stmt = stmt.limit(limit)
            rows = (await session.scalars(stmt)).all()
            return [_run_record_from_model(row) for row in rows]

    async def list_by_owner(self, owner_id: str, *, limit: int | None = None) -> list[RunRecord]:
        async with self._session_factory() as session:
            stmt = select(RunModel).where(RunModel.owner_id == owner_id).order_by(RunModel.created_at.desc())
            if limit is not None:
                stmt = stmt.limit(limit)
            rows = (await session.scalars(stmt)).all()
            return [_run_record_from_model(row) for row in rows]


class AsyncEventStore:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def append(self, record: EventRecord) -> EventRecord:
        async with self._session_factory() as session:
            session.add(_event_model_from_record(record))
            await session.commit()
            return record

    async def append_batch(self, records: list[EventRecord]) -> list[EventRecord]:
        async with self._session_factory() as session:
            session.add_all([_event_model_from_record(r) for r in records])
            await session.commit()
            return records

    async def get(self, event_id: str) -> EventRecord | None:
        async with self._session_factory() as session:
            model = await session.get(EventModel, event_id)
            return _event_record_from_model(model) if model is not None else None

    async def list_for_run(self, run_id: str, *, limit: int | None = None, offset: int = 0) -> list[EventRecord]:
        stmt = select(EventModel).where(EventModel.run_id == run_id)
        return await self._list(stmt, limit=limit, offset=offset)

    async def list_for_trace(self, trace_id: str, *, limit: int | None = None, offset: int = 0) -> list[EventRecord]:
        stmt = select(EventModel).where(EventModel.trace_id == trace_id)
        return await self._list(stmt, limit=limit, offset=offset)

    async def _list(self, stmt, *, limit: int | None = None, offset: int = 0) -> list[EventRecord]:
        async with self._session_factory() as session:
            stmt = stmt.order_by(EventModel.created_at.asc(), EventModel.sequence_index.asc())
            if offset:
                stmt = stmt.offset(offset)
            if limit is not None:
                stmt = stmt.limit(limit)
            rows = (await session.scalars(stmt)).all()
            return [_event_record_from_model(row) for row in rows]


class AsyncJobStore:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def create(self, record: JobRecord) -> JobRecord:
        async with self._session_factory() as session:
            session.add(_job_model_from_record(record))
            await session.commit()
            return record

    async def get(self, job_id: str) -> JobRecord | None:
        async with self._session_factory() as session:
            model = await session.get(JobModel, job_id)
            return _job_record_from_model(model) if model is not None else None

    async def update(self, record: JobRecord) -> JobRecord:
        now = _utcnow()
        async with self._session_factory() as session:
            model = await session.get(JobModel, record.job_id)
            if model is None:
                raise RecordNotFoundError(f"Unknown job {record.job_id!r}")
            model.task_name = record.task_name
            model.status = record.status
            model.payload = record.payload
            model.owner_id = record.owner_id
            model.trace_id = record.trace_id
            model.correlation_id = record.correlation_id
            model.attempt_count = record.attempt_count
            model.max_attempts = record.max_attempts
            model.scheduled_for = record.scheduled_for
            model.claimed_by = record.claimed_by
            model.claimed_at = record.claimed_at
            model.completed_at = record.completed_at
            model.updated_at = now
            await session.commit()
        return replace(record, updated_at=now)

    async def claim_pending(self, job_id: str, worker_id: str) -> JobRecord | None:
        now = _utcnow()
        async with self._session_factory() as session:
            stmt = (
                update(JobModel)
                .where(and_(JobModel.job_id == job_id, JobModel.status == "pending"))
                .values(status="claimed", claimed_by=worker_id, claimed_at=now, updated_at=now)
            )
            result = await session.execute(stmt)
            if result.rowcount != 1:
                await session.rollback()
                return None
            await session.commit()
            model = await session.get(JobModel, job_id)
            assert model is not None
            return _job_record_from_model(model)

    async def list_pending(self, *, due_before: datetime | None = None, limit: int | None = None) -> list[JobRecord]:
        async with self._session_factory() as session:
            stmt = select(JobModel).where(JobModel.status == "pending")
            if due_before is not None:
                stmt = stmt.where(
                    (JobModel.scheduled_for.is_(None)) | (JobModel.scheduled_for <= due_before)
                )
            stmt = stmt.order_by(JobModel.created_at.asc())
            if limit is not None:
                stmt = stmt.limit(limit)
            rows = (await session.scalars(stmt)).all()
            return [_job_record_from_model(row) for row in rows]

    async def set_status(self, job_id: str, status: str, *, completed_at: datetime | None = None) -> JobRecord:
        record = await self.get(job_id)
        if record is None:
            raise RecordNotFoundError(f"Unknown job {job_id!r}")
        return await self.update(replace(record, status=status, completed_at=completed_at))


def _run_model_from_record(record: RunRecord) -> RunModel:
    return RunModel(**asdict(record))


def _run_record_from_model(model: RunModel) -> RunRecord:
    return RunRecord(
        run_id=model.run_id,
        run_type=model.run_type,
        status=model.status,
        state_payload=model.state_payload,
        trace_id=model.trace_id,
        correlation_id=model.correlation_id,
        owner_id=model.owner_id,
        scope=model.scope,
        version=model.version,
        created_at=model.created_at,
        updated_at=model.updated_at,
        completed_at=model.completed_at,
    )


def _event_model_from_record(record: EventRecord) -> EventModel:
    return EventModel(**asdict(record))


def _event_record_from_model(model: EventModel) -> EventRecord:
    return EventRecord(
        event_id=model.event_id,
        event_type=model.event_type,
        payload=model.payload,
        source=model.source,
        run_id=model.run_id,
        trace_id=model.trace_id,
        correlation_id=model.correlation_id,
        parent_event_id=model.parent_event_id,
        sequence_index=model.sequence_index,
        created_at=model.created_at,
    )


def _job_model_from_record(record: JobRecord) -> JobModel:
    return JobModel(**asdict(record))


def _job_record_from_model(model: JobModel) -> JobRecord:
    return JobRecord(
        job_id=model.job_id,
        task_name=model.task_name,
        status=model.status,
        payload=model.payload,
        owner_id=model.owner_id,
        trace_id=model.trace_id,
        correlation_id=model.correlation_id,
        attempt_count=model.attempt_count,
        max_attempts=model.max_attempts,
        scheduled_for=model.scheduled_for,
        claimed_by=model.claimed_by,
        claimed_at=model.claimed_at,
        completed_at=model.completed_at,
        created_at=model.created_at,
        updated_at=model.updated_at,
    )
