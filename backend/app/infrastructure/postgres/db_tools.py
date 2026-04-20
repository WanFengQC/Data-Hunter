from __future__ import annotations

from collections.abc import Sequence
from contextlib import contextmanager
from typing import Any

from psycopg import sql

from app.infrastructure.postgres.database import pg_connect


@contextmanager
def get_db_connection():
    with pg_connect() as conn:
        yield conn


class DBOperator:
    def execute(self, query: str | sql.Composed | sql.SQL, params: Sequence[Any] | None = None) -> None:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
            conn.commit()

    def fetch_one(
        self,
        query: str | sql.Composed | sql.SQL,
        params: Sequence[Any] | None = None,
    ) -> dict[str, Any] | None:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()

    def fetch_all(
        self,
        query: str | sql.Composed | sql.SQL,
        params: Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

    def transaction(self, statements: list[tuple[str | sql.Composed | sql.SQL, Sequence[Any] | None]]) -> None:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for stmt, params in statements:
                    cur.execute(stmt, params)
            conn.commit()


db_controller = DBOperator()

