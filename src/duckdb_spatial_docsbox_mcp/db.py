"""Read-only DuckDB connection helper with the spatial extension auto-loaded.

Design contrasts with the postgres sibling:

* DuckDB is embedded — there is no socket pool. Instead we keep a single
  long-lived "primary" connection per database path. For *file-backed*
  databases we then issue lightweight ``cursor()`` clones for each call
  (DuckDB connections are not thread-safe; cursors share the same DB but
  have their own transaction state).

* For ``:memory:`` databases each ``duckdb.connect(":memory:")`` returns a
  fresh, independent in-memory database with no shared data. To make the
  ``:memory:`` mode useful (e.g. for tests that pre-load fixtures) we keep
  the single shared writable connection and serialise access with a lock.

* Read-only is enforced two ways:
  - **File mode**: opened with ``read_only=True`` so DuckDB itself rejects
    DDL/DML at the storage layer.
  - **Memory mode**: the connection is writable (DuckDB has no in-memory
    read-only mode) but every tool query is gated by ``sql_validator`` which
    rejects everything except ``SELECT`` / ``WITH`` / ``EXPLAIN``.

* Statement timeouts: DuckDB has no SQL-level ``statement_timeout``. We use
  ``threading.Timer`` + ``con.interrupt()`` to abort runaway queries.

* The spatial extension (``INSTALL spatial; LOAD spatial;``) requires a
  *writable* connection for ``INSTALL`` (it writes the binary into
  ``~/.duckdb/extensions``). We do that once at startup using a separate
  short-lived writable connection, then open the read-only handle. After
  that, ``LOAD spatial`` works on read-only connections too because DuckDB
  reuses the cached extension binary.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import duckdb

logger = logging.getLogger(__name__)

DEFAULT_STATEMENT_TIMEOUT_S = 10.0
MEMORY_PATH = ":memory:"


@dataclass
class DuckConfig:
    path: str
    statement_timeout_s: float = DEFAULT_STATEMENT_TIMEOUT_S
    load_spatial: bool = True
    metadata_excludes: tuple[str, ...] = field(default_factory=tuple)


def config_from_env() -> DuckConfig | None:
    """Build a config from env vars. Returns None when no path is set."""
    path = os.environ.get("DUCKDB_DOCSBOX_PATH", "").strip()
    if not path:
        return None
    excludes = tuple(
        s.strip()
        for s in os.environ.get("DUCKDB_DOCSBOX_METADATA_EXCLUDES", "").split(",")
        if s.strip()
    )
    return DuckConfig(
        path=path,
        statement_timeout_s=float(
            os.environ.get("DUCKDB_DOCSBOX_STATEMENT_TIMEOUT_S", DEFAULT_STATEMENT_TIMEOUT_S)
        ),
        load_spatial=os.environ.get("DUCKDB_DOCSBOX_LOAD_SPATIAL", "1").strip() not in {
            "0",
            "false",
            "no",
            "",
        },
        metadata_excludes=excludes,
    )


class TimeoutError(Exception):
    """Raised when a query is aborted by the watchdog timer."""


class Database:
    """Lazy DuckDB wrapper, thread-safe.

    Connection model:
      - ``:memory:``  → one shared writable connection, lock-serialised.
      - file path     → one long-lived read-only connection; cursors per call.
    """

    def __init__(self, cfg: DuckConfig) -> None:
        self._cfg = cfg
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._lock = threading.Lock()
        self._is_memory = cfg.path.strip() == MEMORY_PATH

    @property
    def cfg(self) -> DuckConfig:
        return self._cfg

    @property
    def is_memory(self) -> bool:
        return self._is_memory

    def _bootstrap_spatial(self) -> None:
        """Install + load spatial via a transient writable connection.

        Idempotent. Skipped when ``load_spatial=False``. For ``:memory:`` we
        just LOAD on the shared connection; INSTALL is a global operation
        against the user's home so doing it once per process is fine.
        """
        if not self._cfg.load_spatial:
            return
        try:
            tmp = duckdb.connect()  # transient in-memory writable
            try:
                tmp.execute("INSTALL spatial")
                tmp.execute("LOAD spatial")
            finally:
                tmp.close()
        except Exception as exc:  # pragma: no cover — surfaced at first use
            logger.warning("spatial extension bootstrap failed: %s", exc)

    def _ensure_conn(self) -> duckdb.DuckDBPyConnection:
        with self._lock:
            if self._conn is None:
                self._bootstrap_spatial()
                if self._is_memory:
                    self._conn = duckdb.connect(MEMORY_PATH, read_only=False)
                else:
                    self._conn = duckdb.connect(self._cfg.path, read_only=True)
                if self._cfg.load_spatial:
                    try:
                        self._conn.execute("LOAD spatial")
                    except Exception as exc:
                        logger.warning("LOAD spatial on primary connection failed: %s", exc)
            return self._conn

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                finally:
                    self._conn = None

    @contextmanager
    def readonly(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield a DuckDB connection (or cursor) for read-only use.

        For file-backed databases this hands out a fresh ``cursor()`` so
        concurrent calls don't trample each other's row state. For
        ``:memory:`` we hand out the shared connection under a lock so
        callers see a consistent view.

        The watchdog is **not** auto-installed here; callers must use
        :meth:`run_with_timeout` if they want timeout enforcement. This
        keeps ``readonly()`` cheap for fast metadata queries.
        """
        primary = self._ensure_conn()
        if self._is_memory:
            with self._lock:
                yield primary
        else:
            cur = primary.cursor()
            try:
                yield cur
            finally:
                try:
                    cur.close()
                except Exception:
                    pass

    def run_with_timeout(
        self,
        conn: duckdb.DuckDBPyConnection,
        sql: str,
        params: list[Any] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> duckdb.DuckDBPyConnection:
        """Execute SQL with a watchdog. On expiry: ``con.interrupt()``.

        Returns the connection itself (DuckDB ``execute`` returns ``self``).
        Raises :class:`TimeoutError` if the watchdog fired.
        """
        timeout = float(timeout_s if timeout_s is not None else self._cfg.statement_timeout_s)
        fired = threading.Event()

        def _kill() -> None:
            fired.set()
            try:
                conn.interrupt()
            except Exception:
                pass

        timer = threading.Timer(timeout, _kill)
        timer.daemon = True
        timer.start()
        try:
            try:
                if params is None:
                    return conn.execute(sql)
                return conn.execute(sql, params)
            except duckdb.InterruptException as exc:
                raise TimeoutError(f"query exceeded {timeout:.1f}s") from exc
            except Exception:
                if fired.is_set():
                    raise TimeoutError(f"query exceeded {timeout:.1f}s") from None
                raise
        finally:
            timer.cancel()


def is_metadata_excluded(table: str, cfg: DuckConfig) -> bool:
    """Whether a table is in the operator-configured exclude list."""
    return table in cfg.metadata_excludes
