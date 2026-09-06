"""行情 Schema、时间和富途字段归一化。"""

from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)
SCHEMA_VERSION = 1
ET_TZ = "America/New_York"

CONTRACT_COLUMNS = (
    "schema_version", "provider", "underlying", "contract_code", "name",
    "expiry_date", "dte", "right", "strike", "contract_size",
    "option_area_type", "chain_asof_utc", "chain_hash",
)
QUOTE_COLUMNS = (
    "schema_version", "provider", "capture_id", "batch_id", "tier",
    "underlying", "contract_code", "expiry_date", "dte", "right", "strike",
    "scheduled_at_utc", "requested_at_utc", "received_at_utc", "provider_time_utc",
    "last", "bid", "ask", "bid_size", "ask_size", "open", "high", "low",
    "prev_close", "volume", "turnover", "open_interest", "iv", "delta",
    "gamma", "vega", "theta", "rho", "spot", "underlying_quote_id",
    "capture_status", "is_stale", "quality_flags",
)
UNDERLYING_COLUMNS = (
    "schema_version", "provider", "capture_id", "batch_id", "underlying",
    "scheduled_at_utc", "requested_at_utc", "received_at_utc", "provider_time_utc",
    "last", "bid", "ask", "open", "high", "low", "prev_close", "volume",
    "turnover", "market_state", "quality_flags",
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: Any, *, source_tz: str = ET_TZ) -> str | None:
    """把 datetime/字符串转为 UTC ISO 字符串。"""
    if value is None or value is pd.NA or value == "":
        return None
    try:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize(source_tz)
        return timestamp.tz_convert("UTC").isoformat()
    except (TypeError, ValueError, pd.errors.OutOfBoundsDatetime):
        logger.warning("无法解析行情时间: value=%r source_tz=%s", value, source_tz)
        return None


def trade_date(value: Any, *, source_tz: str = ET_TZ) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(source_tz)
    return str(timestamp.tz_convert(source_tz).date())


def right_from_value(value: Any) -> str:
    text = str(value or "").upper()
    if "PUT" in text or text in {"P", "2"}:
        return "P"
    if "CALL" in text or text in {"C", "1"}:
        return "C"
    return text[:1] if text else ""


def float_or_none(value: Any) -> float | None:
    try:
        if value is None or value is pd.NA or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def stable_hash(frame: pd.DataFrame, columns: list[str] | tuple[str, ...] | None = None) -> str:
    selected = frame[list(columns or frame.columns)].copy()
    selected = selected.sort_values(list(selected.columns)).reset_index(drop=True)
    payload = selected.to_csv(index=False).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def empty_frame(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))
