#!/usr/bin/env python3
"""
CMS Provider Data - "Hospitals" theme incremental downloader

Flow
----
1. Read the CMS Provider Data metastore:
       https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items
2. Keep datasets whose `theme` includes "Hospitals".
3. Compare each dataset's `modified` value with the value recorded at its last
   successful load (DuckDB metadata database) and select only new/changed ones.
4. Download and process the selected files in parallel (thread pool):
   - stream the raw file to disk (constant memory)
   - DuckDB reads the CSV (all columns as VARCHAR, so values such as ZIP codes
     and CCNs keep their leading zeros) and COPYs it back out with snake_case
     column names
   - the output is published atomically (temp file + rename)
5. Record every run and every dataset outcome in DuckDB. A failed dataset does
   not advance its watermark, so it is retried on the next run.

Intended to run once a day from cron (Linux) or Task Scheduler (Windows).
Runs are idempotent and a lock file prevents overlapping runs.

Requires Python 3.9+, `requests` and `duckdb` (see requirements.txt).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
DEFAULT_THEME = "Hospitals"

log = logging.getLogger("cms_hospitals")


# --------------------------------------------------------------------------- #
# Column-name normalisation
# --------------------------------------------------------------------------- #
_APOSTROPHES = re.compile(r"['‘’ʼ`]")          # ' ‘ ’ ʼ `
_CAMEL_1 = re.compile(r"([a-z0-9])([A-Z])")                     # fooBar -> foo_Bar
_CAMEL_2 = re.compile(r"([A-Z]+)([A-Z][a-z])")                  # HTMLParser -> HTML_Parser
_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")


def to_snake_case(name: str) -> str:
    """
    Convert an arbitrary header to snake_case.

    >>> to_snake_case("Patients’ rating of the facility linear mean score")
    'patients_rating_of_the_facility_linear_mean_score'

    """
    s = unicodedata.normalize("NFKD", name.strip().lstrip("﻿"))
    s = s.encode("ascii", "ignore").decode("ascii") if not s.isascii() else s
    s = _APOSTROPHES.sub("", s)                 # "Patients'" -> "Patients"
    s = _CAMEL_2.sub(r"\1_\2", s)
    s = _CAMEL_1.sub(r"\1_\2", s)
    s = _NON_ALNUM.sub("_", s)                  # spaces, slashes, %, (), - ... -> _
    return s.strip("_").lower()


def normalize_headers(headers: list[str]) -> list[str]:
    """snake_case every header, filling blanks and de-duplicating collisions."""
    out: list[str] = []
    seen: dict[str, int] = {}
    for i, h in enumerate(headers, start=1):
        col = to_snake_case(h) or f"column_{i}"
        if col[0].isdigit():
            col = f"col_{col}"
        if col in seen:
            seen[col] += 1
            col = f"{col}_{seen[col]}"
        else:
            seen[col] = 1
        out.append(col)
    return out


def slugify(text: str, max_len: int = 60) -> str:
    return to_snake_case(text)[:max_len].rstrip("_") or "dataset"


# --------------------------------------------------------------------------- #
# Metadata store (DuckDB file)
# --------------------------------------------------------------------------- #
SCHEMA = """
CREATE SEQUENCE IF NOT EXISTS run_id_seq START 1;

CREATE TABLE IF NOT EXISTS runs (
    run_id            BIGINT PRIMARY KEY DEFAULT nextval('run_id_seq'),
    started_at_utc    TIMESTAMP NOT NULL,
    finished_at_utc   TIMESTAMP,
    status            VARCHAR NOT NULL,       -- running | success | partial | failed
    theme             VARCHAR NOT NULL,
    datasets_in_theme INTEGER,
    datasets_selected INTEGER,
    datasets_ok       INTEGER,
    datasets_failed   INTEGER,
    message           VARCHAR
);

CREATE TABLE IF NOT EXISTS datasets (
    identifier          VARCHAR PRIMARY KEY,
    title               VARCHAR,
    source_modified     VARCHAR,              -- metastore `modified` at last SUCCESSFUL load
    download_url        VARCHAR,
    output_file         VARCHAR,
    row_count           BIGINT,
    column_count        INTEGER,
    last_success_run_id BIGINT,
    last_success_at_utc TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dataset_runs (
    run_id          BIGINT NOT NULL,
    identifier      VARCHAR NOT NULL,
    source_modified VARCHAR,
    status          VARCHAR NOT NULL,         -- success | failed
    row_count       BIGINT,
    bytes           BIGINT,
    seconds         DOUBLE,
    error           VARCHAR,
    PRIMARY KEY (run_id, identifier)
);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


class MetadataStore:
    """All writes happen on the main thread; DuckDB is single-writer per file."""

    def __init__(self, path: Path):
        self.conn = duckdb.connect(str(path))
        self.conn.execute(SCHEMA)

    def start_run(self, theme: str) -> int:
        return self.conn.execute(
            "INSERT INTO runs (started_at_utc, status, theme) VALUES (?, 'running', ?) RETURNING run_id",
            [utcnow(), theme],
        ).fetchone()[0]

    def finish_run(self, run_id: int, status: str, **counts) -> None:
        self.conn.execute(
            """UPDATE runs SET finished_at_utc = ?, status = ?, datasets_in_theme = ?,
                   datasets_selected = ?, datasets_ok = ?, datasets_failed = ?, message = ?
               WHERE run_id = ?""",
            [utcnow(), status, counts.get("in_theme"), counts.get("selected"),
             counts.get("ok"), counts.get("failed"), counts.get("message"), run_id],
        )

    def last_modified_map(self) -> dict[str, str]:
        return dict(self.conn.execute("SELECT identifier, source_modified FROM datasets").fetchall())

    def record_result(self, run_id: int, r: "Result") -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO dataset_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [run_id, r.ds.identifier, r.ds.modified, r.status,
             r.row_count, r.bytes, round(r.seconds, 2), r.error],
        )
        if r.status == "success":   # only advance the watermark on success
            self.conn.execute(
                """INSERT INTO datasets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (identifier) DO UPDATE SET
                       title = excluded.title,
                       source_modified = excluded.source_modified,
                       download_url = excluded.download_url,
                       output_file = excluded.output_file,
                       row_count = excluded.row_count,
                       column_count = excluded.column_count,
                       last_success_run_id = excluded.last_success_run_id,
                       last_success_at_utc = excluded.last_success_at_utc""",
                [r.ds.identifier, r.ds.title, r.ds.modified, r.ds.download_url,
                 r.output_file, r.row_count, r.column_count, run_id, utcnow()],
            )

    def write_manifest(self, out_dir: Path, run_id: int) -> None:
        """Readable JSON snapshot of the metadata DB, refreshed after every run."""
        def rows(sql: str, params=()) -> list[dict]:
            cur = self.conn.execute(sql, list(params))
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

        payload = {
            "last_run": rows("SELECT * FROM runs WHERE run_id = ?", [run_id])[0],
            "datasets": rows("SELECT * FROM datasets ORDER BY title"),
        }
        (out_dir / "manifest.json").write_text(
            json.dumps(payload, indent=2, default=str), encoding="utf-8")

    def close(self) -> None:
        self.conn.close()


# --------------------------------------------------------------------------- #
# Metastore discovery
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Dataset:
    identifier: str
    title: str
    modified: str
    download_url: str


@dataclass
class Result:
    ds: Dataset
    status: str
    output_file: str | None = None
    row_count: int | None = None
    column_count: int | None = None
    bytes: int | None = None
    seconds: float = 0.0
    error: str | None = None


def build_session(retries: int = 5, pool_size: int = 16) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retries, backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers["User-Agent"] = "cms-hospitals-incremental-loader/1.0"
    return s


def _csv_url(item: dict) -> str | None:
    for dist in item.get("distribution") or []:
        d = dist.get("data", dist)      # tolerate both flat and nested shapes
        url = d.get("downloadURL")
        media = (d.get("mediaType") or "").lower()
        if url and (media == "text/csv" or url.lower().split("?")[0].endswith(".csv")):
            return url
    return None


def discover(session: requests.Session, url: str, theme: str) -> list[Dataset]:
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    wanted = theme.strip().lower()
    out: list[Dataset] = []
    for item in resp.json():
        themes = item.get("theme") or []
        if isinstance(themes, str):
            themes = [themes]
        if wanted not in {str(t).strip().lower() for t in themes}:
            continue
        csv_url = _csv_url(item)
        if not csv_url:
            log.warning("No CSV distribution for %s (%s) - skipped",
                        item.get("identifier"), item.get("title"))
            continue
        out.append(Dataset(
            identifier=item["identifier"],
            title=item.get("title", ""),
            modified=str(item.get("modified", "")),
            download_url=csv_url,
        ))
    return out


def select_changed(datasets: list[Dataset], previous: dict[str, str], force: bool) -> list[Dataset]:
    """New datasets, or ones whose metastore `modified` differs from the last successful load."""
    if force:
        return list(datasets)
    return [d for d in datasets if previous.get(d.identifier) != d.modified]


# --------------------------------------------------------------------------- #
# Download + transform (one worker)
# --------------------------------------------------------------------------- #
def _detect_encoding(path: Path) -> str:
    """CMS files are UTF-8 (sometimes with a BOM); fall back to latin-1 for legacy files."""
    with open(path, "rb") as fh:
        try:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                chunk.decode("utf-8")
        except UnicodeDecodeError:
            return "latin-1"
    return "utf-8"


def _read_header(path: Path, encoding: str) -> list[str]:
    """Original header row, parsed by DuckDB so quoting rules match the data read."""
    con = duckdb.connect()
    try:
        cur = con.execute(
            "SELECT * FROM read_csv(?, header = true, all_varchar = true, encoding = ?) LIMIT 0",
            [str(path), encoding],
        )
        return [d[0] for d in cur.description]
    finally:
        con.close()


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def process_dataset(session: requests.Session, ds: Dataset, out_dir: Path) -> Result:
    """Download one dataset, then let DuckDB rewrite it with snake_case column names."""
    t0 = time.perf_counter()
    final_path = out_dir / f"{ds.identifier}_{slugify(ds.title)}.csv"
    raw_path = out_dir / f".{ds.identifier}.raw.part"
    tmp_path = out_dir / f".{ds.identifier}.csv.part"
    try:
        # 1) stream bytes to disk
        size = 0
        with session.get(ds.download_url, stream=True, timeout=(15, 300)) as resp:
            resp.raise_for_status()
            with open(raw_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
                    size += len(chunk)
        if size == 0:
            raise ValueError("empty file")

        # 2) DuckDB: read as VARCHAR with renamed columns, COPY straight back out.
        #    One in-memory connection per worker - connections are not shared
        #    across threads.
        enc = _detect_encoding(raw_path)
        new_header = normalize_headers(_read_header(raw_path, enc))
        con = duckdb.connect()
        try:
            rows = con.execute(
                f"""COPY (
                        SELECT * FROM read_csv(?, header = true, all_varchar = true,
                                               names = ?, encoding = ?)
                    ) TO {_sql_literal(str(tmp_path))} (FORMAT csv, HEADER, DELIMITER ',')""",
                [str(raw_path), new_header, enc],
            ).fetchone()[0]
        finally:
            con.close()

        # 3) publish atomically (Windows and POSIX)
        os.replace(tmp_path, final_path)
        return Result(ds, "success", str(final_path), rows, len(new_header),
                      size, time.perf_counter() - t0)
    except Exception as exc:                        # recorded, retried next run
        tmp_path.unlink(missing_ok=True)
        return Result(ds, "failed", seconds=time.perf_counter() - t0,
                      error=f"{type(exc).__name__}: {exc}"[:1000])
    finally:
        raw_path.unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# Run lock (prevents overlapping scheduled runs)
# --------------------------------------------------------------------------- #
class RunLock:
    def __init__(self, path: Path, stale_after_s: int = 6 * 3600):
        self.path, self.stale_after_s = path, stale_after_s

    def __enter__(self):
        if self.path.exists() and time.time() - self.path.stat().st_mtime > self.stale_after_s:
            log.warning("Removing stale lock %s", self.path)
            self.path.unlink(missing_ok=True)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            raise SystemExit(f"Another run appears to be active (lock: {self.path}). Exiting.")
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return self

    def __exit__(self, *exc):
        self.path.unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def run(args: argparse.Namespace) -> int:
    out_dir = Path(args.output_dir).resolve()
    data_dir = out_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    with RunLock(out_dir / ".run.lock"):
        store = MetadataStore(out_dir / "metadata.duckdb")
        run_id = store.start_run(args.theme)
        log.info("Run %s started (theme=%r, workers=%d)", run_id, args.theme, args.workers)
        session = build_session(pool_size=max(args.workers, 4))
        try:
            datasets = discover(session, args.metastore_url, args.theme)
            todo = select_changed(datasets, store.last_modified_map(), args.force)
            log.info("%d datasets in theme; %d new/modified since last successful run",
                     len(datasets), len(todo))

            if args.dry_run:
                for d in todo:
                    log.info("  would download: %s  modified=%s  %s", d.identifier, d.modified, d.title)
                store.finish_run(run_id, "success", in_theme=len(datasets), selected=len(todo),
                                 ok=0, failed=0, message="dry run")
                return 0

            ok = failed = 0
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = [pool.submit(process_dataset, session, d, data_dir) for d in todo]
                for fut in as_completed(futures):
                    r = fut.result()
                    store.record_result(run_id, r)
                    if r.status == "success":
                        ok += 1
                        log.info("OK   %s  rows=%s cols=%s %.1fs  %s", r.ds.identifier,
                                 r.row_count, r.column_count, r.seconds, r.ds.title)
                    else:
                        failed += 1
                        log.error("FAIL %s  %s  (%s)", r.ds.identifier, r.error, r.ds.title)

            status = "success" if failed == 0 else ("partial" if ok else "failed")
            store.finish_run(run_id, status, in_theme=len(datasets), selected=len(todo),
                             ok=ok, failed=failed)
            store.write_manifest(out_dir, run_id)
            log.info("Run %s finished: %s (ok=%d, failed=%d, skipped-unchanged=%d)",
                     run_id, status, ok, failed, len(datasets) - len(todo))
            return 0 if failed == 0 else 2
        except Exception as exc:
            store.finish_run(run_id, "failed", message=f"{type(exc).__name__}: {exc}"[:1000])
            log.exception("Run %s failed", run_id)
            return 1
        finally:
            session.close()
            store.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Incremental CMS 'Hospitals' dataset loader (DuckDB).")
    p.add_argument("--output-dir", default="cms_output",
                   help="Where CSVs, metadata.duckdb and manifest.json are written (default: ./cms_output)")
    p.add_argument("--theme", default=DEFAULT_THEME, help='Metastore theme to filter on (default: "Hospitals")')
    p.add_argument("--workers", type=int, default=8, help="Parallel downloads (default: 8)")
    p.add_argument("--metastore-url", default=METASTORE_URL)
    p.add_argument("--force", action="store_true", help="Ignore history and re-download every dataset")
    p.add_argument("--dry-run", action="store_true", help="List what would be downloaded, download nothing")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-5s %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(out_dir / "job.log", encoding="utf-8")],
    )
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
