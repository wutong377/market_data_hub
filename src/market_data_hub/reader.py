"""基于 daily manifest 的只读 Reader。"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .config import resolve_data_dir
from .storage import file_hash

logger = logging.getLogger(__name__)


class OptionDataReader:
    def __init__(self, data_root: str | Path | None = None):
        self.root_dir = resolve_data_dir(data_root)

    def _manifest(self, trade_date: str) -> dict[str, Any] | None:
        latest = self.root_dir / "manifests" / "daily" / f"{trade_date}_latest.json"
        if latest.exists():
            payload = json.loads(latest.read_text(encoding="utf-8"))
            if payload.get("quality", {}).get("passed", True):
                return payload
            logger.warning("latest manifest 质量未通过，继续寻找稳定版本: trade_date=%s", trade_date)
        candidates = sorted((self.root_dir / "manifests" / "daily").glob(f"{trade_date}_*.json"))
        for candidate in reversed(candidates):
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            if payload.get("quality", {}).get("passed", True):
                return payload
        return None

    @staticmethod
    def _read_files(manifest: dict[str, Any], dataset: str) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for item in manifest.get("datasets", []):
            if item.get("dataset") != dataset:
                continue
            path = Path(item["path"])
            if not path.exists():
                logger.error("manifest 指向文件不存在: dataset=%s path=%s", dataset, path)
                continue
            actual = file_hash(path)
            if item.get("content_hash") and actual != item["content_hash"]:
                logger.error("manifest 文件 hash 不匹配: dataset=%s path=%s expected=%s actual=%s", dataset, path, item.get("content_hash"), actual)
                continue
            frames.append(pd.read_parquet(path))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def quotes(
        self,
        *,
        underlyings: Iterable[str] | None = None,
        start: str | None = None,
        end: str | None = None,
        tiers: Iterable[str] | None = None,
        complete_only: bool = True,
        time_basis: str = "scheduled",
    ) -> pd.DataFrame:
        if time_basis not in {"scheduled", "received"}:
            raise ValueError(f"不支持的时间语义: time_basis={time_basis}")
        time_column = f"{time_basis}_at_utc"
        dates = self._resolve_dates(start, end)
        frames: list[pd.DataFrame] = []
        underlying_set = set(underlyings or [])
        tier_set = set(tiers or [])
        for trade_date in dates:
            manifest = self._manifest(trade_date)
            if not manifest:
                continue
            frame = self._read_files(manifest, "option_quotes")
            if frame.empty:
                continue
            if underlying_set:
                frame = frame[frame["underlying"].isin(underlying_set)]
            if tier_set:
                frame = frame[frame["tier"].isin(tier_set)]
            if complete_only and "capture_status" in frame:
                frame = frame[frame["capture_status"] == "complete"]
            frames.append(frame)
        if not frames:
            return pd.DataFrame()
        result = pd.concat(frames, ignore_index=True)
        if start and time_column in result:
            received = pd.to_datetime(result[time_column], utc=True, errors="coerce")
            start_timestamp = self._timestamp_boundary(start)
            result = result[received >= start_timestamp]
        if end and time_column in result:
            received = pd.to_datetime(result[time_column], utc=True, errors="coerce")
            end_timestamp = self._timestamp_boundary(end, end_boundary=True)
            if len(end) == 10:
                result = result[received < end_timestamp]
            else:
                result = result[received <= end_timestamp]
        return result.reset_index(drop=True)

    def chain(self, underlying: str, *, as_of: str | None = None) -> pd.DataFrame:
        dates = self._resolve_dates(as_of, as_of)
        for trade_date in reversed(dates):
            manifest = self._manifest(trade_date)
            if not manifest:
                continue
            frame = self._read_files(manifest, "contracts")
            if not frame.empty:
                return frame[frame["underlying"] == underlying].reset_index(drop=True)
        return pd.DataFrame()

    def snapshot(
        self,
        underlying: str,
        *,
        as_of: str,
        tier: str = "surface",
        time_basis: str = "scheduled",
    ) -> pd.DataFrame:
        frame = self.quotes(
            underlyings=[underlying], end=as_of, tiers=[tier], complete_only=True,
            time_basis=time_basis,
        )
        if frame.empty:
            return frame
        time_column = f"{time_basis}_at_utc"
        frame[time_column] = pd.to_datetime(frame[time_column], utc=True, errors="coerce")
        timestamp = self._timestamp_boundary(as_of, end_boundary=len(as_of) == 10)
        if len(as_of) == 10:
            timestamp -= pd.Timedelta(nanoseconds=1)
        frame = frame[frame[time_column] <= timestamp]
        if frame.empty:
            return frame
        latest_capture = frame.sort_values(time_column)["capture_id"].iloc[-1]
        return frame[frame["capture_id"] == latest_capture].reset_index(drop=True)

    def daily_quality(self, trade_date: str) -> dict[str, Any]:
        manifest = self._manifest(trade_date)
        if not manifest:
            return {"trade_date": trade_date, "passed": False, "error": "manifest 不存在"}
        return {
            "trade_date": trade_date,
            "passed": True,
            "capture_count": manifest.get("capture_count", 0),
            "quote_rows": manifest.get("quote_rows", 0),
            "quality": manifest.get("quality", {}),
            "slo_passed": manifest.get("slo_passed", manifest.get("quality", {}).get("slo_passed", True)),
            "datasets": manifest.get("datasets", []),
        }

    def _resolve_dates(self, start: str | None, end: str | None) -> list[str]:
        if start and len(start) >= 10:
            first = self._trade_date(start)
        elif end and len(end) >= 10:
            first = self._trade_date(end)
        else:
            first = pd.Timestamp.utcnow().date()
        if end and len(end) >= 10:
            last = self._trade_date(end)
        else:
            last = first
        return [value.date().isoformat() for value in pd.date_range(first, last, freq="D")]

    @staticmethod
    def _timestamp_boundary(value: str, *, end_boundary: bool = False) -> pd.Timestamp:
        timestamp = pd.Timestamp(value)
        if len(value) == 10 and timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("America/New_York")
        elif timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        timestamp = timestamp.tz_convert("UTC")
        if end_boundary and len(value) == 10:
            timestamp += pd.Timedelta(days=1)
        return timestamp

    @staticmethod
    def _trade_date(value: str):
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is not None:
            timestamp = timestamp.tz_convert("America/New_York")
        return timestamp.date()
