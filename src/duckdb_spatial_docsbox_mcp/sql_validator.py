"""Two-stage SQL safety pipeline for DuckDB.

Stage 1 (static): sqlglot parse + keyword/statement-shape rules.
Stage 2 (live):   ``EXPLAIN`` against the connection (no execution beyond
                  planning).

Why a denylist over an allowlist: spatial constructors look like function
calls (``ST_MakePoint``, ``ST_GeomFromText``) and we want them. Mutators or
sandbox-escape vectors (``COPY ... TO``, ``INSTALL``, ``LOAD``, ``ATTACH``,
``read_text``, ``read_blob``, ``PRAGMA``) live in the denylist.

Notes specific to DuckDB:

* DuckDB ``COPY`` can both *import* (``COPY t FROM ...``) and *export*
  (``COPY (SELECT ...) TO 'path'``). Both are denied here — read paths
  through ``read_csv`` / ``read_parquet`` / ``ST_Read`` remain allowed.
* ``INSTALL`` / ``LOAD`` write to ``~/.duckdb`` and can pull arbitrary
  binaries; denied.
* ``ATTACH`` / ``DETACH`` can mount external databases (sqlite, postgres,
  another duckdb file) — denied.
* ``PRAGMA`` and ``SET`` change session state, including disabling the
  read-only transaction or extending the search path; denied.
* ``CREATE SECRET`` / ``DROP SECRET`` manage cloud credentials; denied.
* File-reader functions ``read_text`` / ``read_blob`` (and ``_auto`` forms)
  return raw filesystem contents; denied.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import expressions as exp

# Disallowed SQL constructs. Matched on the lower-cased SQL with a leading
# space buffer so we don't false-positive on substrings like ``selected``.
_DENY_KEYWORDS = (
    "drop",
    "delete",
    "insert",
    "update",
    "alter",
    "truncate",
    "create",
    "grant",
    "revoke",
    "exec",
    "execute",
    "copy",
    "merge",
    "vacuum",
    "analyze",
    "cluster",
    "reindex",
    "lock",
    "listen",
    "notify",
    "comment",
    "set",
    "reset",
    "begin",
    "commit",
    "rollback",
    "savepoint",
    "do ",
    "call ",
    "install",
    "load",
    "force",
    "attach",
    "detach",
    "export",
    "import",
    "pragma",
    "checkpoint",
    "use ",
)

# Disallowed function names. Filesystem readers and any function that can
# spawn a process or fetch arbitrary URLs is rejected.
_DENY_FUNCTIONS = {
    "read_text",
    "read_blob",
    "read_text_auto",
    "read_blob_auto",
    "read_file",
    "read_files",
    "shell",
    "system",
}


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    sql: str
    error: str | None = None
    hint: str | None = None
    auto_limit_applied: bool = False


def static_validate(sql: str, *, default_limit: int = 500) -> ValidationResult:
    """Run static checks. Returns the (possibly LIMIT-augmented) SQL."""
    cleaned = sql.strip().rstrip(";").strip()
    if not cleaned:
        return ValidationResult(ok=False, sql=sql, error="empty SQL")

    # Multi-statement guard via raw split. A literal ';' inside a string
    # literal would trip this — that's acceptable for read-only introspection.
    if ";" in cleaned:
        return ValidationResult(
            ok=False,
            sql=sql,
            error="multiple statements are not allowed",
            hint="submit one SELECT or WITH statement at a time",
        )

    head = cleaned.lstrip().lower()
    if not (head.startswith("select") or head.startswith("with") or head.startswith("explain")):
        return ValidationResult(
            ok=False,
            sql=sql,
            error="only SELECT, WITH, and EXPLAIN statements are allowed",
            hint="this MCP server is read-only; use a CTE or a subquery",
        )

    lower = " " + head + " "
    # ``EXPLAIN ANALYZE <SELECT>`` is read-only profiling; allow ``analyze`` only
    # as the second token after ``explain``.
    explain_analyze = head.startswith("explain analyze")
    for kw in _DENY_KEYWORDS:
        if explain_analyze and kw.strip() == "analyze":
            continue
        needle = " " + kw.strip() + (" " if not kw.endswith(" ") else "")
        if needle in lower:
            return ValidationResult(
                ok=False,
                sql=sql,
                error=f"disallowed keyword: {kw.strip()!r}",
                hint="this MCP server only runs read-only queries",
            )

    try:
        tree = sqlglot.parse_one(cleaned, dialect="duckdb")
    except sqlglot.errors.ParseError as exc:
        return ValidationResult(
            ok=False,
            sql=sql,
            error=f"parse error: {exc}",
            hint="check the SQL syntax; column/table names that contain reserved words must be quoted",
        )

    if tree is None:
        return ValidationResult(ok=False, sql=sql, error="parse produced no statement")

    # Walk for forbidden function calls.
    for func in tree.find_all(exp.Func):
        name = (func.name or "").lower()
        if name in _DENY_FUNCTIONS:
            return ValidationResult(
                ok=False,
                sql=sql,
                error=f"disallowed function: {name}",
                hint="this function can read raw filesystem contents",
            )

    # Top-level statement must be a query.
    top = tree
    if not isinstance(top, exp.Select | exp.Union | exp.Subquery | exp.With):
        if not (
            isinstance(top, exp.Command)
            and top.this
            and top.this.upper().startswith("EXPLAIN")
        ):
            return ValidationResult(
                ok=False,
                sql=sql,
                error=f"top-level statement is {type(top).__name__}, not SELECT/WITH/EXPLAIN",
            )

    auto_limit = False
    out_sql = cleaned
    if (
        not head.startswith("explain")
        and isinstance(top, exp.Select | exp.With | exp.Union)
        and "limit" not in head
    ):
        out_sql = f"{cleaned}\nLIMIT {int(default_limit)}"
        auto_limit = True

    return ValidationResult(ok=True, sql=out_sql, auto_limit_applied=auto_limit)
