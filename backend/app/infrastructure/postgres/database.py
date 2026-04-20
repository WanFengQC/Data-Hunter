from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from queue import Empty, Full, LifoQueue
from threading import Lock
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.core.config import settings

_PG_POOL_LOCK = Lock()
_PG_POOL_QUEUE: LifoQueue[psycopg.Connection] = LifoQueue()
_PG_POOL_SIZE = 0


def _open_pg_connection(*, autocommit: bool = False, row_factory: Any = dict_row) -> psycopg.Connection:
    return psycopg.connect(
        host=settings.pg_host,
        user=settings.pg_user,
        password=settings.pg_pass,
        port=settings.pg_port,
        dbname=settings.pg_db,
        autocommit=autocommit,
        row_factory=row_factory,
    )


def _discard_pooled_connection(conn: psycopg.Connection | None) -> None:
    global _PG_POOL_SIZE
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    with _PG_POOL_LOCK:
        _PG_POOL_SIZE = max(0, _PG_POOL_SIZE - 1)


@contextmanager
def pg_connect() -> Iterator[psycopg.Connection]:
    global _PG_POOL_SIZE
    conn: psycopg.Connection | None = None
    managed_by_pool = False

    while conn is None:
        try:
            candidate = _PG_POOL_QUEUE.get_nowait()
        except Empty:
            candidate = None

        if candidate is not None:
            if candidate.closed or getattr(candidate, "broken", False):
                _discard_pooled_connection(candidate)
                continue
            conn = candidate
            managed_by_pool = True
            break

        should_create = False
        with _PG_POOL_LOCK:
            if _PG_POOL_SIZE < max(settings.pg_pool_min_size, settings.pg_pool_max_size):
                _PG_POOL_SIZE += 1
                should_create = True

        if should_create:
            try:
                conn = _open_pg_connection()
                managed_by_pool = True
            except Exception:
                with _PG_POOL_LOCK:
                    _PG_POOL_SIZE = max(0, _PG_POOL_SIZE - 1)
                raise
            break

        try:
            candidate = _PG_POOL_QUEUE.get(timeout=settings.pg_pool_wait_timeout_seconds)
        except Empty:
            conn = _open_pg_connection()
            managed_by_pool = False
            break

        if candidate.closed or getattr(candidate, "broken", False):
            _discard_pooled_connection(candidate)
            continue
        conn = candidate
        managed_by_pool = True

    try:
        yield conn
    finally:
        if conn is None:
            return
        if not managed_by_pool:
            try:
                conn.close()
            except Exception:
                pass
            return
        if conn.closed or getattr(conn, "broken", False):
            _discard_pooled_connection(conn)
            return
        try:
            conn.rollback()
        except Exception:
            _discard_pooled_connection(conn)
            return
        try:
            _PG_POOL_QUEUE.put_nowait(conn)
        except Full:
            _discard_pooled_connection(conn)


def pg_connect_autocommit() -> psycopg.Connection:
    return _open_pg_connection(autocommit=True, row_factory=None)


def close_pg_pool() -> None:
    global _PG_POOL_SIZE
    while True:
        try:
            conn = _PG_POOL_QUEUE.get_nowait()
        except Empty:
            break
        try:
            conn.close()
        except Exception:
            pass
    with _PG_POOL_LOCK:
        _PG_POOL_SIZE = 0

