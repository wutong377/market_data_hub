"""SQLite WAL 原始存储、Parquet 压缩和 manifest。"""

from __future__ import annotations

from datetime import datetime, timezone
import ctypes
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import HubConfig, resolve_data_dir
from .quality import cadence_report, merge_quality_reports, validate_quotes
from .schema import CONTRACT_COLUMNS, QUOTE_COLUMNS, UNDERLYING_COLUMNS, as_utc, now_utc

logger = logging.getLogger(__name__)


def _release_allocator_memory() -> None:
    """把 glibc 分配器已释放但未归还系统的内存还给内核(降低 RSS 峰值)。

    仅 Linux 有效;其它平台或失败时静默忽略。分批压缩大帧循环后调用。
    """
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def _safe(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_", "."} else "_" for char in value)


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    temporary = Path(name)
    try:
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


class RawSqliteStore:
    """按交易日维护一个可恢复的 SQLite WAL 原始库。"""

    def __init__(self, root_dir: str | Path | None = None):
        self.root_dir = resolve_data_dir(root_dir)
        self.raw_root = self.root_dir / "raw"
        self._connections: dict[str, sqlite3.Connection] = {}

    def _path(self, trade_date: str) -> Path:
        path = self.raw_root / f"trade_date={trade_date}" / "capture.sqlite3"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _connection(self, trade_date: str) -> sqlite3.Connection:
        if trade_date in self._connections:
            return self._connections[trade_date]
        connection = sqlite3.connect(self._path(trade_date), timeout=30)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS captures (
                capture_id TEXT PRIMARY KEY,
                trade_date TEXT NOT NULL,
                underlying TEXT NOT NULL,
                tier TEXT NOT NULL,
                scheduled_at_utc TEXT NOT NULL,
                started_at_utc TEXT NOT NULL,
                completed_at_utc TEXT,
                expected_rows INTEGER NOT NULL DEFAULT 0,
                received_rows INTEGER NOT NULL DEFAULT 0,
                failed_batches INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                error TEXT
            );
            CREATE TABLE IF NOT EXISTS contracts (
                underlying TEXT NOT NULL,
                contract_code TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                chain_asof_utc TEXT NOT NULL,
                chain_hash TEXT NOT NULL,
                PRIMARY KEY (underlying, contract_code, chain_asof_utc)
            );
            CREATE TABLE IF NOT EXISTS option_quotes (
                capture_id TEXT NOT NULL,
                contract_code TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (capture_id, contract_code)
            );
            CREATE TABLE IF NOT EXISTS underlying_quotes (
                capture_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                underlying TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (capture_id, batch_id, underlying)
            );
            CREATE TABLE IF NOT EXISTS gaps (
                gap_id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date TEXT NOT NULL,
                capture_id TEXT NOT NULL,
                underlying TEXT NOT NULL,
                tier TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at_utc TEXT NOT NULL
            );
            """
        )
        connection.commit()
        self._connections[trade_date] = connection
        return connection

    def write_contracts(self, trade_date: str, contracts: pd.DataFrame) -> None:
        if contracts.empty:
            return
        connection = self._connection(trade_date)
        records = [
            (
                str(row["underlying"]), str(row["contract_code"]),
                json.dumps({key: row.get(key) for key in CONTRACT_COLUMNS}, ensure_ascii=False, default=str),
                str(row["chain_asof_utc"]), str(row["chain_hash"]),
            )
            for row in contracts.to_dict("records")
        ]
        with connection:
            connection.executemany(
                "INSERT OR REPLACE INTO contracts(underlying, contract_code, payload_json, chain_asof_utc, chain_hash) VALUES(?,?,?,?,?)",
                records,
            )

    def write_capture(
        self,
        *,
        trade_date: str,
        capture_id: str,
        underlying: str,
        tier: str,
        scheduled_at_utc: str,
        started_at_utc: str,
        completed_at_utc: str,
        expected_rows: int,
        quotes: pd.DataFrame,
        underlying_quotes: pd.DataFrame,
        failed_batches: Iterable[str] = (),
        error: str | None = None,
        finalize: bool = True,
    ) -> None:
        connection = self._connection(trade_date)
        failed = list(failed_batches)
        with connection:
            connection.execute(
                "INSERT OR REPLACE INTO captures VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    capture_id, trade_date, underlying, tier, scheduled_at_utc,
                    started_at_utc, completed_at_utc, int(expected_rows), 0,
                    len(failed), "running" if not finalize else "partial", error,
                ),
            )
            if not quotes.empty:
                records = [
                    (str(row["capture_id"]), str(row["contract_code"]), json.dumps(row, ensure_ascii=False, default=str))
                    for row in quotes.to_dict("records")
                ]
                connection.executemany(
                    "INSERT OR REPLACE INTO option_quotes(capture_id, contract_code, payload_json) VALUES(?,?,?)",
                    records,
                )
            if not underlying_quotes.empty:
                records = [
                    (str(row["capture_id"]), str(row["batch_id"]), str(row["underlying"]), json.dumps(row, ensure_ascii=False, default=str))
                    for row in underlying_quotes.to_dict("records")
                ]
                connection.executemany(
                    "INSERT OR REPLACE INTO underlying_quotes(capture_id, batch_id, underlying, payload_json) VALUES(?,?,?,?)",
                    records,
                )
            if failed:
                connection.executemany(
                    "INSERT INTO gaps(trade_date,capture_id,underlying,tier,reason,created_at_utc) VALUES(?,?,?,?,?,?)",
                    [(trade_date, capture_id, underlying, tier, batch, completed_at_utc) for batch in failed],
                )
            received_rows = int(connection.execute(
                "SELECT COUNT(*) FROM option_quotes WHERE capture_id = ?", (capture_id,)
            ).fetchone()[0])
            if finalize:
                status = "complete" if not failed and received_rows >= int(expected_rows * 0.99) else "partial"
                connection.execute(
                    "UPDATE captures SET completed_at_utc=?, received_rows=?, failed_batches=?, status=?, error=? WHERE capture_id=?",
                    (completed_at_utc, received_rows, len(failed), status, error, capture_id),
                )
            else:
                connection.execute(
                    "UPDATE captures SET received_rows=? WHERE capture_id=?",
                    (received_rows, capture_id),
                )
        logger.info(
            "原始采集写入完成: trade_date=%s capture_id=%s underlying=%s tier=%s expected=%s received=%s failed_batches=%s status=%s",
            trade_date, capture_id, underlying, tier, expected_rows, received_rows, len(failed),
            "complete" if finalize and not failed and received_rows >= int(expected_rows * 0.99) else "partial" if finalize else "running",
        )

    def finalize_capture(
        self,
        *,
        trade_date: str,
        capture_id: str,
        expected_rows: int,
        failed_batches: Iterable[str] = (),
        error: str | None = None,
        completed_at_utc: str | None = None,
    ) -> str:
        """只更新 capture 元数据，避免 finalize 时重复写入全部报价。"""
        connection = self._connection(trade_date)
        failed = list(failed_batches)
        completed_at_utc = completed_at_utc or now_utc().isoformat()
        with connection:
            received_rows = int(connection.execute(
                "SELECT COUNT(*) FROM option_quotes WHERE capture_id = ?", (capture_id,)
            ).fetchone()[0])
            connection.execute("DELETE FROM gaps WHERE capture_id = ?", (capture_id,))
            if failed:
                connection.executemany(
                    "INSERT INTO gaps(trade_date,capture_id,underlying,tier,reason,created_at_utc) "
                    "SELECT trade_date,capture_id,underlying,tier,?,? FROM captures WHERE capture_id=?",
                    [(batch, completed_at_utc, capture_id) for batch in failed],
                )
            status = "complete" if not failed and received_rows >= int(expected_rows * 0.99) else "partial"
            connection.execute(
                "UPDATE captures SET completed_at_utc=?, received_rows=?, failed_batches=?, status=?, error=? "
                "WHERE capture_id=?",
                (completed_at_utc, received_rows, len(failed), status, error, capture_id),
            )
        logger.info(
            "原始采集 finalize: trade_date=%s capture_id=%s expected=%s received=%s failed_batches=%s status=%s",
            trade_date, capture_id, expected_rows, received_rows, len(failed), status,
        )
        return status

    def read_table(self, trade_date: str, table: str) -> pd.DataFrame:
        if table not in {"captures", "contracts", "option_quotes", "underlying_quotes", "gaps"}:
            raise ValueError(f"不允许读取表: {table}")
        path = self._path(trade_date)
        if not path.exists():
            return pd.DataFrame()
        connection = self._connection(trade_date)
        frame = pd.read_sql_query(f"SELECT * FROM {table}", connection)
        if table in {"option_quotes", "underlying_quotes", "contracts"} and not frame.empty:
            frame["payload"] = frame["payload_json"].map(json.loads)
            payload = pd.DataFrame(frame.pop("payload").tolist())
            return payload
        return frame

    def _close_connection(self, trade_date: str, connection: sqlite3.Connection) -> None:
        """收尾并关闭单个连接；即使收尾失败也必须释放文件句柄。"""
        try:
            running = connection.execute(
                "SELECT capture_id, underlying, tier FROM captures WHERE status = 'running'"
            ).fetchall()
            for capture_id, underlying, tier in running:
                connection.execute(
                    "UPDATE captures SET status='partial', error=? WHERE capture_id=?",
                    ("process_interrupted", capture_id),
                )
                connection.execute(
                    "INSERT INTO gaps(trade_date,capture_id,underlying,tier,reason,created_at_utc) VALUES(?,?,?,?,?,?)",
                    (trade_date, capture_id, underlying, tier, "process_interrupted", now_utc().isoformat()),
                )
            connection.commit()
            connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except Exception:
            logger.warning("关闭前收尾 SQLite 连接异常: trade_date=%s", trade_date, exc_info=True)
        finally:
            try:
                connection.close()
            except Exception:
                logger.warning("关闭 SQLite 连接失败: trade_date=%s", trade_date, exc_info=True)

    def close_stale_dates(self, keep: str) -> list[str]:
        """关闭除 keep 之外所有交易日的缓存连接，返回被关闭的交易日。

        长跑进程若一直持有已被 prune 删除的 Raw 文件句柄，磁盘空间不会释放：
        句柄存活期间 unlink 只是去掉目录项，inode 与数据块仍被占用。
        因此每跨一个交易日就必须主动关闭上一交易日的连接。
        """
        closed: list[str] = []
        for trade_date in [item for item in self._connections if item != keep]:
            self._close_connection(trade_date, self._connections.pop(trade_date))
            closed.append(trade_date)
        return closed

    def close(self) -> None:
        for trade_date in list(self._connections):
            self._close_connection(trade_date, self._connections.pop(trade_date))


class ParquetCompactor:
    """把 SQLite 交易日数据压缩为 manifest 指向的版本化 Parquet。"""

    def __init__(self, root_dir: str | Path | None = None, *, config: HubConfig | None = None):
        self.root_dir = resolve_data_dir(root_dir)
        self.raw = RawSqliteStore(self.root_dir)
        self.config = config

    def compact(self, trade_date: str) -> dict[str, Any]:
        underlying = self.raw.read_table(trade_date, "underlying_quotes")
        contracts = self.raw.read_table(trade_date, "contracts")
        captures = self.raw.read_table(trade_date, "captures")
        # 期权报价表(整日数百万行)按 (underlying, tier) 分组合并,避免整表载入内存导致 OOM。
        connection = self.raw._connection(trade_date)
        groups = connection.execute(
            "SELECT DISTINCT underlying, tier FROM captures"
        ).fetchall()
        if not groups and underlying.empty and contracts.empty:
            raise FileNotFoundError(f"没有可压缩数据: trade_date={trade_date}")
        compact_id = datetime.now(timezone.utc).strftime("compact_%Y%m%dT%H%M%S%fZ")
        files: list[dict[str, Any]] = []
        quote_reports: list[dict[str, Any]] = []
        quote_rows = 0
        for underlying_code, tier in groups:
            directory = (
                self.root_dir / "curated" / "option_quotes"
                / f"trade_date={trade_date}"
                / f"underlying={_safe(str(underlying_code))}"
                / f"tier={_safe(str(tier))}"
            )
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"part-{compact_id}.parquet"
            # 流式分批解析 payload_json 并写 parquet:单批 5 万行,峰值内存与批大小成正比,
            # 远低于整组载入(此前 425 万行整表读取在 15G 内存机器上 OOM)。
            cursor = connection.execute(
                "SELECT q.capture_id, q.contract_code, q.payload_json "
                "FROM option_quotes q JOIN captures c ON q.capture_id = c.capture_id "
                "WHERE c.underlying=? AND c.tier=?",
                (underlying_code, tier),
            )
            writer: pq.ParquetWriter | None = None
            anchor_schema: pa.Schema | None = None
            group_rows = 0
            while True:
                rows = cursor.fetchmany(50_000)
                if not rows:
                    break
                records: list[dict[str, Any]] = []
                for capture_id, contract_code, payload in rows:
                    record = json.loads(payload)
                    record["capture_id"] = capture_id
                    record["contract_code"] = contract_code
                    records.append(record)
                if writer is None:
                    batch = pa.Table.from_pylist(records)
                    anchor_schema = batch.schema
                    writer = pq.ParquetWriter(path, anchor_schema, compression="snappy")
                else:
                    batch = pa.Table.from_pylist(records, schema=anchor_schema)
                writer.write_table(batch)
                group_rows += batch.num_rows
                quote_reports.append(validate_quotes(batch.to_pandas()))
                del batch
            del records
            if writer is None or group_rows == 0:
                continue
            writer.close()
            files.append({
                "dataset": "option_quotes",
                "path": str(path),
                "rows": group_rows,
                "content_hash": file_hash(path),
            })
            quote_rows += group_rows
            _release_allocator_memory()
        for dataset, frame, partition_columns in (
            ("underlying_quotes", underlying, ["underlying"]),
            ("contracts", contracts, ["underlying"]),
        ):
            if frame.empty:
                continue
            for keys, part in frame.groupby(partition_columns, dropna=False, sort=True):
                if not isinstance(keys, tuple):
                    keys = (keys,)
                directory = self.root_dir / "curated" / dataset / f"trade_date={trade_date}"
                for column, value in zip(partition_columns, keys):
                    directory /= f"{column}={_safe(str(value))}"
                directory.mkdir(parents=True, exist_ok=True)
                path = directory / f"part-{compact_id}.parquet"
                part.to_parquet(path, compression="snappy", index=False)
                files.append({
                    "dataset": dataset,
                    "path": str(path),
                    "rows": int(len(part)),
                    "content_hash": file_hash(path),
                })
        quality = merge_quality_reports(quote_reports) if quote_reports else {
            "passed": False,
            "errors": ["报价为空"],
            "warnings": [],
            "rows": 0,
            "duplicate_rows": 0,
            "valid_bid_ratio": 0.0,
            "valid_ask_ratio": 0.0,
            "valid_iv_ratio": 0.0,
            "positive_bid_ratio": 0.0,
            "valid_nbbo_ratio": 0.0,
            "fresh_provider_time_ratio": 0.0,
        }
        if self.config is not None:
            cadence = cadence_report(
                captures,
                trade_date=trade_date,
                underlyings=self.config.underlying_codes(),
                calendar_name=self.config.market.calendar,
                intervals={
                    "hot": self.config.collection.hot_interval_seconds,
                    "surface": self.config.collection.surface_interval_seconds,
                    "anchor": 0,
                },
                thresholds={"hot": 0.95, "surface": 0.98, "anchor": 1.0},
            )
            quality["cadence"] = cadence
            quality["slo_passed"] = cadence["slo_passed"]
        else:
            quality["slo_passed"] = True
        manifest = {
            "schema_version": 1,
            "trade_date": trade_date,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "compact_id": compact_id,
            "raw_db": str(self.raw._path(trade_date)),
            "datasets": files,
            "capture_count": int(len(captures)),
            "quote_rows": quote_rows,
            "underlying_rows": int(len(underlying)),
            "contract_rows": int(len(contracts)),
            "quality": quality,
            "slo_passed": bool(quality.get("slo_passed", True)),
        }
        manifest_path = self.root_dir / "manifests" / "daily" / f"{trade_date}_{compact_id}.json"
        _atomic_json(manifest_path, manifest)
        if manifest["quality"]["passed"]:
            latest = self.root_dir / "manifests" / "daily" / f"{trade_date}_latest.json"
            _atomic_json(latest, manifest)
            latest_valid = self.root_dir / "manifests" / "daily" / "latest_valid.json"
            _atomic_json(latest_valid, manifest)
        else:
            logger.error("日数据质量失败，保留版本但不更新 latest_valid: trade_date=%s", trade_date)
        logger.info(
            "日数据压缩完成: trade_date=%s compact_id=%s files=%s quotes=%s",
            trade_date, compact_id, len(files), quote_rows,
        )
        return manifest

    def has_current_manifest(self, trade_date: str) -> bool:
        """判断当前 Raw 是否已经有可用 manifest，避免服务重启重复压缩。"""
        manifest_path = self.root_dir / "manifests" / "daily" / f"{trade_date}_latest.json"
        if not manifest_path.exists():
            return False
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if not manifest.get("quality", {}).get("passed", False):
                return False
            raw_path = self.raw._path(trade_date)
            if manifest.get("raw_db") != str(raw_path) or not raw_path.exists():
                return False
            if not all(Path(item.get("path", "")).exists() for item in manifest.get("datasets", [])):
                return False
            # Raw 文件没有在 manifest 创建后发生变化时，直接视为同一版本；
            # 完整行数对账仍在 prune 前执行，避免服务重启每次扫描数百万行。
            created_at = datetime.fromisoformat(str(manifest["created_at_utc"])).timestamp()
            return raw_path.stat().st_mtime <= created_at + 1.0
        except Exception:
            logger.warning("检查当前 manifest 异常，将重新执行压缩: trade_date=%s", trade_date, exc_info=True)
            return False

    def prune(
        self,
        *,
        older_than_days: int = 7,
        trash_days: int = 3,
        now: datetime | None = None,
        apply: bool = False,
    ) -> list[str]:
        now = now or datetime.now(timezone.utc)
        moved: list[str] = []
        if not self.raw.raw_root.exists():
            return moved
        trash = self.root_dir / "trash" / "raw"
        for directory in sorted(self.raw.raw_root.glob("trade_date=*")):
            if not directory.is_dir():
                continue
            value = directory.name.split("=", 1)[-1]
            try:
                age = (now.date() - datetime.strptime(value, "%Y-%m-%d").date()).days
            except ValueError:
                continue
            if age < older_than_days:
                continue
            manifest_path = self.root_dir / "manifests" / "daily" / f"{value}_latest.json"
            if not manifest_path.exists():
                logger.warning("没有有效 manifest，跳过原始数据清理: trade_date=%s", value)
                continue
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                invalid = [
                    item.get("path") for item in manifest.get("datasets", [])
                    if not Path(item.get("path", "")).exists()
                    or (item.get("content_hash") and file_hash(Path(item["path"])) != item["content_hash"])
                ]
            except Exception:
                logger.warning("manifest 校验异常，跳过原始数据清理: trade_date=%s", value, exc_info=True)
                continue
            if invalid:
                logger.warning("manifest 文件校验失败，跳过原始数据清理: trade_date=%s files=%s", value, invalid)
                continue
            raw_path = directory / "capture.sqlite3"
            if not self._raw_matches_manifest(raw_path, manifest):
                logger.warning("Raw 与 manifest 行数或状态不一致，跳过原始数据清理: trade_date=%s", value)
                continue
            target = trash / directory.name
            moved.append(str(directory))
            if apply:
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    logger.error("trash 目标已存在，拒绝覆盖: target=%s", target)
                    continue
                os.replace(directory, target)
                logger.info("原始 SQLite 移入 trash: trade_date=%s path=%s", value, target)
        if trash.exists():
            for directory in sorted(trash.glob("trade_date=*")):
                if not directory.is_dir():
                    continue
                age = (now - datetime.fromtimestamp(directory.stat().st_mtime, tz=timezone.utc)).days
                if age < trash_days:
                    continue
                moved.append(str(directory))
                if apply:
                    shutil.rmtree(directory)
                    logger.info("trash 中的原始 SQLite 永久删除: path=%s", directory)
        return moved

    @staticmethod
    def _raw_matches_manifest(raw_path: Path, manifest: dict[str, Any]) -> bool:
        """清理前确认 Raw 已完整压缩，避免误删未完成交易日。"""
        if not raw_path.exists() or not manifest.get("quality", {}).get("passed", False):
            return False
        connection = None
        try:
            connection = sqlite3.connect(f"file:{raw_path}?mode=ro", uri=True, timeout=5)
            counts = {
                "capture_count": connection.execute("SELECT COUNT(*) FROM captures").fetchone()[0],
                "quote_rows": connection.execute("SELECT COUNT(*) FROM option_quotes").fetchone()[0],
                "underlying_rows": connection.execute("SELECT COUNT(*) FROM underlying_quotes").fetchone()[0],
                "contract_rows": connection.execute("SELECT COUNT(*) FROM contracts").fetchone()[0],
                "running": connection.execute(
                    "SELECT COUNT(*) FROM captures WHERE status != 'complete'"
                ).fetchone()[0],
            }
        except Exception:
            logger.warning("读取 Raw manifest 对账失败: raw=%s", raw_path, exc_info=True)
            return False
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    logger.warning("关闭对账连接失败: raw=%s", raw_path, exc_info=True)
        return (
            counts["running"] == 0
            and counts["capture_count"] == int(manifest.get("capture_count", -1))
            and counts["quote_rows"] == int(manifest.get("quote_rows", -1))
            and counts["underlying_rows"] == int(manifest.get("underlying_rows", -1))
            and counts["contract_rows"] == int(manifest.get("contract_rows", -1))
        )


def import_legacy_snapshots(source: str | Path, root_dir: str | Path | None = None) -> list[dict[str, Any]]:
    """导入 intraday_lab 旧格式快照，保持原文件 hash 和来源。"""
    root = resolve_data_dir(root_dir)
    source_path = Path(source)
    results: list[dict[str, Any]] = []
    for path in sorted(source_path.glob("*/date=*/*.parquet")):
        frame = pd.read_parquet(path)
        if frame.empty:
            continue
        collected = pd.to_datetime(frame["collected_at_utc"].iloc[0], utc=True)
        trade_date = collected.tz_convert("America/New_York").date().isoformat()
        underlying = str(frame["stock_owner"].iloc[0])
        capture_id = f"legacy_{underlying.replace('.', '_')}_{collected.strftime('%Y%m%dT%H%M%S%fZ')}"
        out = pd.DataFrame({
            "schema_version": 1,
            "provider": "legacy_intraday_lab",
            "capture_id": capture_id,
            "batch_id": "legacy_import",
            "tier": "legacy",
            "underlying": underlying,
            "contract_code": frame["code"].astype(str),
            "expiry_date": pd.to_datetime(frame["strike_time"]).dt.date.astype(str),
            "dte": pd.to_numeric(frame["option_expiry_date_distance"], errors="coerce"),
            "right": frame["option_type"].map(lambda value: "P" if "PUT" in str(value).upper() else "C"),
            "strike": pd.to_numeric(frame["option_strike_price"], errors="coerce"),
            "scheduled_at_utc": str(collected),
            "requested_at_utc": str(collected),
            "received_at_utc": str(collected),
            "provider_time_utc": frame["update_time"].map(as_utc),
            "last": pd.to_numeric(frame["last_price"], errors="coerce"),
            "bid": pd.to_numeric(frame["bid_price"], errors="coerce"),
            "ask": pd.to_numeric(frame["ask_price"], errors="coerce"),
            "bid_size": pd.to_numeric(frame["bid_vol"], errors="coerce"),
            "ask_size": pd.to_numeric(frame["ask_vol"], errors="coerce"),
            "open": pd.to_numeric(frame["open_price"], errors="coerce"),
            "high": pd.to_numeric(frame["high_price"], errors="coerce"),
            "low": pd.to_numeric(frame["low_price"], errors="coerce"),
            "prev_close": pd.to_numeric(frame["prev_close_price"], errors="coerce"),
            "volume": pd.to_numeric(frame["volume"], errors="coerce"),
            "turnover": pd.to_numeric(frame["turnover"], errors="coerce"),
            "open_interest": pd.to_numeric(frame["option_open_interest"], errors="coerce"),
            "iv": pd.to_numeric(frame["option_implied_volatility"], errors="coerce") / 100.0,
            "delta": pd.to_numeric(frame["option_delta"], errors="coerce"),
            "gamma": pd.to_numeric(frame["option_gamma"], errors="coerce"),
            "vega": pd.to_numeric(frame["option_vega"], errors="coerce"),
            "theta": pd.to_numeric(frame["option_theta"], errors="coerce"),
            "rho": pd.to_numeric(frame["option_rho"], errors="coerce"),
            "spot": pd.to_numeric(frame["underlying_last_price"], errors="coerce"),
            "underlying_quote_id": capture_id,
            "capture_status": "complete",
            "is_stale": False,
            "quality_flags": "legacy_import",
        })
        directory = root / "curated" / "option_quotes" / f"trade_date={trade_date}" / f"underlying={_safe(underlying)}" / "tier=legacy"
        directory.mkdir(parents=True, exist_ok=True)
        destination = directory / f"legacy-{path.stem}.parquet"
        if not destination.exists():
            out.to_parquet(destination, compression="snappy", index=False)
        manifest = {
            "schema_version": 1,
            "trade_date": trade_date,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "compact_id": "legacy_import",
            "datasets": [{"dataset": "option_quotes", "path": str(destination), "rows": len(out), "content_hash": file_hash(destination)}],
            "legacy_source_path": str(path),
            "legacy_source_hash": file_hash(path),
            "quote_rows": len(out),
        }
        _atomic_json(root / "manifests" / "daily" / f"{trade_date}_legacy_{path.stem}.json", manifest)
        latest_path = root / "manifests" / "daily" / f"{trade_date}_latest.json"
        if latest_path.exists():
            latest = json.loads(latest_path.read_text(encoding="utf-8"))
        else:
            latest = {
                "schema_version": 1,
                "trade_date": trade_date,
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "compact_id": "legacy_import",
                "datasets": [],
                "quote_rows": 0,
                "legacy_sources": [],
            }
        existing_paths = {item.get("path") for item in latest.get("datasets", [])}
        if str(destination) not in existing_paths:
            latest.setdefault("datasets", []).extend(manifest["datasets"])
            latest["quote_rows"] = int(latest.get("quote_rows", 0)) + int(len(out))
        sources = set(latest.get("legacy_sources", []))
        sources.add(str(path))
        latest["legacy_sources"] = sorted(sources)
        _atomic_json(latest_path, latest)
        results.append(manifest)
        logger.info("旧期权快照导入完成: source=%s underlying=%s rows=%s", path, underlying, len(out))
    return results
