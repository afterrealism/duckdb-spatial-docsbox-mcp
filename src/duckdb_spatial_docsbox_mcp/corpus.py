"""Offline doc-corpus loader (DuckDB + spatial reference, official docs links).

Two backends:
1. SQLite-backed corpus at ``$DUCKDB_DOCSBOX_CORPUS_DIR/sections.db`` with an
   accompanying ``blobs/`` directory containing zstd-compressed bodies.
2. Static TOML manifest shipped with the wheel as ``_data/manifest.toml``.

If neither is available, an empty corpus is returned and the docs tools
fall back to live HTTP fetches against duckdb.org / gdal.org.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

try:
    import tomllib  # py311+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Section:
    id: str
    title: str
    package: str
    url: str
    body: str | None = None


class Corpus:
    def list(self, package: str | None = None) -> list[Section]:
        raise NotImplementedError

    def get(self, section_id: str) -> Section | None:
        raise NotImplementedError


class _ManifestCorpus(Corpus):
    def __init__(self, sections: list[Section]) -> None:
        self._all = sections
        self._by_id = {s.id: s for s in sections}

    def list(self, package: str | None = None) -> list[Section]:
        if package is None:
            return list(self._all)
        pkg = package.lower()
        return [s for s in self._all if s.package.lower() == pkg]

    def get(self, section_id: str) -> Section | None:
        return self._by_id.get(section_id)


class _SqliteCorpus(Corpus):
    def __init__(self, db_path: Path, blobs_dir: Path) -> None:
        self._db_path = db_path
        self._blobs_dir = blobs_dir
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            f"file:{db_path}?mode=ro", uri=True, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row

    def list(self, package: str | None = None) -> list[Section]:
        with self._lock:
            cur = self._conn.cursor()
            if package is None:
                cur.execute("SELECT id, title, package, url FROM sections")
            else:
                cur.execute(
                    "SELECT id, title, package, url FROM sections WHERE LOWER(package)=?",
                    (package.lower(),),
                )
            rows = cur.fetchall()
        return [
            Section(id=r["id"], title=r["title"], package=r["package"], url=r["url"])
            for r in rows
        ]

    def get(self, section_id: str) -> Section | None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT id, title, package, url, blob_path FROM sections WHERE id=?",
                (section_id,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        body: str | None = None
        blob_path = row["blob_path"]
        if blob_path:
            try:
                import zstandard as zstd  # type: ignore[import-not-found]
            except ImportError:
                pass
            else:
                full = self._blobs_dir / blob_path
                try:
                    with full.open("rb") as fh:
                        body = zstd.ZstdDecompressor().decompress(fh.read()).decode("utf-8")
                except (OSError, UnicodeDecodeError, zstd.ZstdError) as exc:
                    logger.warning("failed to read blob for %s: %s", section_id, exc)
        return Section(
            id=row["id"], title=row["title"], package=row["package"],
            url=row["url"], body=body,
        )


class _EmptyCorpus(Corpus):
    def list(self, package: str | None = None) -> list[Section]:
        return []

    def get(self, section_id: str) -> Section | None:
        return None


def _load_manifest_text(text: str) -> list[Section]:
    data: dict[str, Any] = tomllib.loads(text)
    out: list[Section] = []
    for entry in data.get("section", []):
        try:
            out.append(
                Section(
                    id=entry["id"],
                    title=entry["title"],
                    package=entry.get("package", "unknown"),
                    url=entry["url"],
                )
            )
        except KeyError as exc:
            logger.warning("manifest entry missing key %s: %r", exc, entry)
    return out


_MANIFEST_LOAD_ERRORS = (OSError, tomllib.TOMLDecodeError, KeyError, ValueError)


def load_corpus(corpus_dir: str | None) -> Corpus:
    if corpus_dir:
        base = Path(corpus_dir).expanduser()
        db = base / "sections.db"
        blobs = base / "blobs"
        if db.exists():
            try:
                return _SqliteCorpus(db, blobs)
            except sqlite3.Error as exc:
                logger.warning("failed to open sqlite corpus at %s: %s", db, exc)
        manifest = base / "manifest.toml"
        if manifest.exists():
            try:
                return _ManifestCorpus(_load_manifest_text(manifest.read_text("utf-8")))
            except _MANIFEST_LOAD_ERRORS as exc:
                logger.warning("failed to read manifest at %s: %s", manifest, exc)

    try:
        text = (
            resources.files("duckdb_spatial_docsbox_mcp")
            .joinpath("_data/manifest.toml")
            .read_text("utf-8")
        )
        return _ManifestCorpus(_load_manifest_text(text))
    except (FileNotFoundError, ModuleNotFoundError, AttributeError):
        pass
    except _MANIFEST_LOAD_ERRORS as exc:
        logger.warning("failed to read bundled manifest: %s", exc)

    repo_manifest = Path(__file__).resolve().parents[2] / "corpus" / "manifest.toml"
    if repo_manifest.exists():
        try:
            return _ManifestCorpus(_load_manifest_text(repo_manifest.read_text("utf-8")))
        except _MANIFEST_LOAD_ERRORS as exc:
            logger.warning("failed to read repo manifest %s: %s", repo_manifest, exc)

    return _EmptyCorpus()
