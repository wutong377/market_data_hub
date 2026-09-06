"""行情数据质量检查。"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def validate_quotes(frame: pd.DataFrame, *, expected_rows: int | None = None) -> dict[str, Any]:
    """检查报价帧并返回可序列化报告。"""
    errors: list[str] = []
    warnings: list[str] = []
    if frame.empty:
        errors.append("报价为空")
    duplicate_count = int(frame.duplicated(["capture_id", "contract_code"]).sum()) if not frame.empty else 0
    if duplicate_count:
        errors.append(f"自然键重复: {duplicate_count}")
    if expected_rows is not None and len(frame) < int(expected_rows * 0.99):
        warnings.append(f"报价覆盖不足: expected={expected_rows} received={len(frame)}")
    if not frame.empty:
        numeric_columns = [
            "last", "bid", "ask", "bid_size", "ask_size", "volume",
            "turnover", "open_interest",
        ]
        for column in numeric_columns:
            if column in frame:
                values = pd.to_numeric(frame[column], errors="coerce")
                negative = int((values < 0).sum())
                if negative:
                    errors.append(f"{column} 存在负值: {negative}")
        bid = pd.to_numeric(frame.get("bid"), errors="coerce")
        ask = pd.to_numeric(frame.get("ask"), errors="coerce")
        crossed = int(((bid.notna()) & (ask.notna()) & (bid > ask)).sum())
        if crossed:
            warnings.append(f"bid 大于 ask: {crossed}")
        missing_codes = int(frame["contract_code"].isna().sum()) if "contract_code" in frame else len(frame)
        if missing_codes:
            errors.append(f"合约代码缺失: {missing_codes}")
    result = {
        "passed": not errors,
        "errors": errors,
        "warnings": warnings,
        "rows": int(len(frame)),
        "duplicate_rows": duplicate_count,
        "valid_bid_ratio": _valid_ratio(frame, "bid"),
        "valid_ask_ratio": _valid_ratio(frame, "ask"),
        "valid_iv_ratio": _valid_ratio(frame, "iv"),
        "positive_bid_ratio": _positive_ratio(frame, "bid"),
        "valid_nbbo_ratio": _nbbo_ratio(frame),
        "fresh_provider_time_ratio": _fresh_ratio(frame),
    }
    if not result["passed"]:
        logger.error("报价质量校验失败: %s", result)
    elif warnings:
        logger.warning("报价质量校验有警告: %s", result)
    else:
        logger.info("报价质量校验通过: rows=%s", len(frame))
    return result


def _valid_ratio(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame:
        return 0.0
    return float(pd.to_numeric(frame[column], errors="coerce").notna().mean())


def _positive_ratio(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame:
        return 0.0
    values = pd.to_numeric(frame[column], errors="coerce")
    return float((values > 0).mean())


def _nbbo_ratio(frame: pd.DataFrame) -> float:
    if frame.empty or "bid" not in frame or "ask" not in frame:
        return 0.0
    bid = pd.to_numeric(frame["bid"], errors="coerce")
    ask = pd.to_numeric(frame["ask"], errors="coerce")
    return float(((bid > 0) & (ask > 0) & (bid <= ask)).mean())


def _fresh_ratio(frame: pd.DataFrame) -> float:
    if frame.empty or "is_stale" not in frame:
        return 0.0
    return float((~frame["is_stale"].fillna(True).astype(bool)).mean())


def cadence_report(
    captures: pd.DataFrame,
    *,
    trade_date: str,
    underlyings: tuple[str, ...] = (),
    calendar_name: str = "XNYS",
    intervals: dict[str, int] | None = None,
    thresholds: dict[str, float] | None = None,
) -> dict[str, Any]:
    """按交易所会话和计划时间计算采样覆盖率与调度延迟。"""
    intervals = intervals or {"hot": 5, "surface": 60, "anchor": 0}
    thresholds = thresholds or {"hot": 0.95, "surface": 0.98, "anchor": 1.0}
    try:
        import exchange_calendars as xcals
        calendar = xcals.get_calendar(calendar_name)
        opened = pd.Timestamp(calendar.session_open(pd.Timestamp(trade_date)))
        closed = pd.Timestamp(calendar.session_close(pd.Timestamp(trade_date)))
    except Exception:
        logger.warning("无法读取交易日历，cadence 使用 6.5 小时回退: trade_date=%s", trade_date, exc_info=True)
        opened = pd.Timestamp(f"{trade_date} 13:30:00", tz="UTC")
        closed = opened + pd.Timedelta(hours=6.5)
    session_seconds = max(1.0, (closed - opened).total_seconds())
    expected_underlyings = underlyings or tuple(
        sorted(captures["underlying"].dropna().astype(str).unique()) if "underlying" in captures else ()
    )
    rows: dict[str, dict[str, Any]] = {}
    slo_passed = True
    for tier, interval in intervals.items():
        expected_count = 3 if tier == "anchor" else max(1, int(session_seconds // interval))
        tier_rows: list[dict[str, Any]] = []
        for underlying in expected_underlyings:
            subset = captures[
                (captures.get("underlying") == underlying)
                & (captures.get("tier") == tier)
            ].copy() if not captures.empty else pd.DataFrame()
            complete = subset[subset["status"] == "complete"] if not subset.empty else subset
            coverage = float(len(complete) / expected_count) if expected_count else 0.0
            if coverage < thresholds.get(tier, 1.0):
                slo_passed = False
            scheduled = pd.to_datetime(
                complete.get("scheduled_at_utc", pd.Series(dtype=str)), utc=True, errors="coerce"
            ).dropna().sort_values()
            started = pd.to_datetime(
                complete.get("started_at_utc", pd.Series(dtype=str)), utc=True, errors="coerce"
            )
            planned = pd.to_datetime(
                complete.get("scheduled_at_utc", pd.Series(dtype=str)), utc=True, errors="coerce"
            )
            delays = (started - planned).dt.total_seconds().clip(lower=0).dropna()
            diffs = scheduled.diff().dt.total_seconds().dropna()
            max_interval = float(diffs.max()) if not diffs.empty else 0.0
            tier_rows.append({
                "underlying": underlying,
                "planned": expected_count,
                "complete": int(len(complete)),
                "partial": int(len(subset) - len(complete)),
                "coverage": round(coverage, 6),
                "p50_delay_seconds": round(float(delays.quantile(0.50)), 3) if not delays.empty else None,
                "p95_delay_seconds": round(float(delays.quantile(0.95)), 3) if not delays.empty else None,
                "max_interval_seconds": round(max_interval, 3),
                "max_gap_seconds": round(max(0.0, max_interval - interval), 3) if interval else 0.0,
            })
        rows[tier] = {
            "interval_seconds": interval,
            "planned_per_underlying": expected_count,
            "threshold": thresholds.get(tier, 1.0),
            "underlyings": tier_rows,
            "passed": all(item["coverage"] >= thresholds.get(tier, 1.0) for item in tier_rows),
        }
    return {
        "trade_date": trade_date,
        "session_seconds": session_seconds,
        "tiers": rows,
        "slo_passed": slo_passed,
    }


def merge_quality_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    """合并分片质量报告(分批压缩时各 (underlying, tier) 分片分别校验后汇总)。

    与 validate_quotes 输出的字段一致:rows/duplicates 直接求和,
    各比率按 rows 加权平均,errors/warnings 从各片文本聚合数值后重建。
    """
    if not reports:
        return {
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
    rows = sum(r["rows"] for r in reports)
    duplicate_rows = sum(r["duplicate_rows"] for r in reports)

    def _fetch(predicate: str) -> int:
        """从各片 errors/warnings 中按前缀文本累加数值,如 'last 存在负值: 3'。"""
        parts = []
        for report in reports:
            for item in report.get("errors", []) + report.get("warnings", []):
                if item.startswith(predicate):
                    text = item.rsplit(":", 1)[-1].strip()
                    try:
                        parts.append(int(text))
                    except ValueError:
                        pass
        return sum(parts)

    errors: list[str] = []
    neg_counts: dict[str, int] = {}
    for report in reports:
        for item in report.get("errors", []):
            # 形如 "<列名> 存在负值: N"
            if "存在负值" in item:
                column, value = item.rsplit(" 存在负值:", 1)
                neg_counts[column.strip()] = neg_counts.get(column.strip(), 0) + int(value.strip())
    for column, count in sorted(neg_counts.items()):
        errors.append(f"{column} 存在负值: {count}")
    if duplicate_rows:
        errors.append(f"自然键重复: {duplicate_rows}")
    missing = _fetch("合约代码缺失")
    if missing:
        errors.append(f"合约代码缺失: {missing}")

    warnings: list[str] = []
    crossed = _fetch("bid 大于 ask")
    if crossed:
        warnings.append(f"bid 大于 ask: {crossed}")

    fields = {
        "valid_bid_ratio": "valid_bid_ratio",
        "valid_ask_ratio": "valid_ask_ratio",
        "valid_iv_ratio": "valid_iv_ratio",
        "positive_bid_ratio": "positive_bid_ratio",
        "valid_nbbo_ratio": "valid_nbbo_ratio",
        "fresh_provider_time_ratio": "fresh_provider_time_ratio",
    }
    ratios: dict[str, float] = {}
    for field in fields:
        if rows:
            ratios[field] = round(sum(r[field] * r["rows"] for r in reports) / rows, 6)
        else:
            ratios[field] = 0.0
    result = {
        "passed": not errors,
        "errors": errors,
        "warnings": warnings,
        "rows": rows,
        "duplicate_rows": duplicate_rows,
        **ratios,
    }
    if not result["passed"]:
        logger.error("报价质量校验失败(合并): %s", result)
    elif warnings:
        logger.warning("报价质量校验有警告(合并): %s", result)
    else:
        logger.info("报价质量校验通过(合并): rows=%s", rows)
    return result
