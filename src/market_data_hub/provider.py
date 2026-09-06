"""富途 OpenD 期权 provider。"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import logging
import time
from typing import Any, Callable, Mapping

import pandas as pd

from .ratelimit import RateLimiterSet
from .schema import (
    CONTRACT_COLUMNS,
    QUOTE_COLUMNS,
    UNDERLYING_COLUMNS,
    as_utc,
    empty_frame,
    float_or_none,
    right_from_value,
    stable_hash,
)

logger = logging.getLogger(__name__)


class ProviderError(RuntimeError):
    """行情 provider 返回错误。"""


class FutuOptionProvider:
    """只读富途 provider；context 可注入假对象进行单元测试。"""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 11111,
        context: Any | None = None,
        limiter: RateLimiterSet | None = None,
        sleep: Callable[[float], None] = time.sleep,
        market_snapshot_hot_reserved: int = 8,
    ):
        self.host = host
        self.port = port
        self._context = context
        self.limiter = limiter or RateLimiterSet(market_snapshot_reserved=market_snapshot_hot_reserved)
        self.sleep = sleep
        self.metrics: dict[str, int] = {"retries": 0, "reconnects": 0}

    @property
    def context(self) -> Any:
        if self._context is not None:
            return self._context
        try:
            from futu import OpenQuoteContext
        except ImportError as exc:  # pragma: no cover
            raise ProviderError("缺少 futu-api，无法连接 OpenD") from exc
        try:
            self._context = OpenQuoteContext(host=self.host, port=self.port)
        except Exception:
            logger.exception("连接富途 OpenD 失败: host=%s port=%s", self.host, self.port)
            raise
        logger.info("富途 OpenD 已连接: host=%s port=%s", self.host, self.port)
        return self._context

    def close(self) -> None:
        if self._context is None:
            return
        try:
            self._context.close()
        except Exception:
            logger.warning("关闭富途 OpenD 异常", exc_info=True)
        self._context = None

    @staticmethod
    def _ret_ok() -> Any:
        try:
            from futu import RET_OK
            return RET_OK
        except ImportError:
            return 0

    def _check(self, ret: Any, data: Any, *, api: str, underlying: str) -> Any:
        if ret != self._ret_ok():
            raise ProviderError(f"富途接口失败 api={api} underlying={underlying} msg={data}")
        return data

    def discover_contracts(
        self,
        underlying: str,
        *,
        min_dte: int = 0,
        max_dte: int = 180,
        asof: date | None = None,
    ) -> pd.DataFrame:
        """按 30 天窗口发现当前未到期合约。"""
        if min_dte < 0 or max_dte < min_dte:
            raise ValueError(f"DTE 范围无效: min_dte={min_dte} max_dte={max_dte}")
        asof = asof or datetime.now(timezone.utc).date()
        ret, expiry_frame = self.context.get_option_expiration_date(underlying)
        expiry_frame = self._check(ret, expiry_frame, api="get_option_expiration_date", underlying=underlying)
        if expiry_frame is None or len(expiry_frame) == 0:
            logger.warning("期权到期日为空: underlying=%s", underlying)
            return empty_frame(CONTRACT_COLUMNS)
        expiries = pd.to_datetime(expiry_frame["strike_time"], errors="coerce").dt.date
        dte = pd.to_numeric(expiry_frame["option_expiry_date_distance"], errors="coerce")
        selected = sorted(set(expiry for expiry, distance in zip(expiries, dte) if pd.notna(expiry) and min_dte <= int(distance) <= max_dte))
        if not selected:
            return empty_frame(CONTRACT_COLUMNS)
        raw_frames: list[pd.DataFrame] = []
        cursor = selected[0]
        last = selected[-1]
        while cursor <= last:
            window_end = min(cursor + timedelta(days=29), last)
            self.limiter.option_chain.acquire()
            self.metrics["option_chain_calls"] = self.metrics.get("option_chain_calls", 0) + 1
            try:
                from futu import OptionType
                option_type = OptionType.ALL
            except ImportError:  # pragma: no cover
                option_type = None
            ret, frame = self.context.get_option_chain(
                underlying,
                start=cursor.isoformat(),
                end=window_end.isoformat(),
                option_type=option_type,
            )
            frame = self._check(ret, frame, api="get_option_chain", underlying=underlying)
            if isinstance(frame, pd.DataFrame) and not frame.empty:
                raw_frames.append(frame)
            cursor = window_end + timedelta(days=1)
        if not raw_frames:
            return empty_frame(CONTRACT_COLUMNS)
        raw = pd.concat(raw_frames, ignore_index=True).drop_duplicates(subset=["code"])
        expiry = pd.to_datetime(raw.get("strike_time"), errors="coerce").dt.date
        raw = raw[expiry.isin(selected)].copy()
        chain_asof = datetime.now(timezone.utc).isoformat()
        normalized: list[dict[str, Any]] = []
        for row in raw.to_dict("records"):
            contract_code = str(row.get("code") or "")
            expiry_date = pd.to_datetime(row.get("strike_time"), errors="coerce")
            if not contract_code or pd.isna(expiry_date):
                continue
            expiry_value = expiry_date.date()
            strike = float_or_none(row.get("strike_price", row.get("option_strike_price")))
            right = right_from_value(row.get("option_type"))
            normalized.append({
                "schema_version": 1,
                "provider": "futu_opend",
                "underlying": underlying,
                "contract_code": contract_code,
                "name": row.get("name"),
                "expiry_date": expiry_value.isoformat(),
                "dte": (expiry_value - asof).days,
                "right": right,
                "strike": strike,
                "contract_size": row.get("lot_size", row.get("option_contract_size")),
                "option_area_type": row.get("option_area_type"),
                "chain_asof_utc": chain_asof,
                "chain_hash": "",
            })
        result = pd.DataFrame(normalized, columns=list(CONTRACT_COLUMNS))
        if result.empty:
            return empty_frame(CONTRACT_COLUMNS)
        chain_hash = stable_hash(result, ["underlying", "contract_code", "expiry_date", "right", "strike"])
        result["chain_hash"] = chain_hash
        return result.sort_values(["expiry_date", "strike", "right", "contract_code"]).reset_index(drop=True)

    def snapshot(
        self,
        underlying: str,
        contracts: pd.DataFrame,
        *,
        capture_id: str,
        batch_prefix: str,
        tier: str,
        scheduled_at_utc: str,
        max_attempts: int = 3,
        on_batch: Callable[[pd.DataFrame, pd.DataFrame, str], None] | None = None,
        before_batch: Callable[[], None] | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
        """拉取合约快照，返回 option quotes、underlying quotes、失败批次。"""
        if contracts.empty:
            return empty_frame(QUOTE_COLUMNS), empty_frame(UNDERLYING_COLUMNS), []
        requested_at = datetime.now(timezone.utc).isoformat()
        contract_rows = contracts.to_dict("records")
        all_quotes: list[pd.DataFrame] = []
        all_underlyings: list[pd.DataFrame] = []
        failures: list[str] = []
        for offset in range(0, len(contract_rows), 399):
            if before_batch is not None:
                before_batch()
            batch_rows = contract_rows[offset : offset + 399]
            batch_id = f"{batch_prefix}_{offset:06d}"
            codes = [underlying] + [str(row["contract_code"]) for row in batch_rows]
            frame = None
            error: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    self.limiter.market_snapshot.acquire(priority="high" if tier == "hot" else "normal")
                    self.metrics["market_snapshot_calls"] = self.metrics.get("market_snapshot_calls", 0) + 1
                    ret, data = self.context.get_market_snapshot(codes)
                    frame = self._check(ret, data, api="get_market_snapshot", underlying=underlying)
                    error = None
                    break
                except Exception as exc:
                    error = exc
                    if attempt >= max_attempts:
                        break
                    self.metrics["retries"] += 1
                    delay = min(4.0, 0.5 * (2 ** (attempt - 1)))
                    logger.warning(
                        "市场快照批次失败，准备重试: underlying=%s batch_id=%s attempt=%s delay=%.2f error=%s",
                        underlying, batch_id, attempt, delay, exc,
                    )
                    self.sleep(delay)
            if error is not None or not isinstance(frame, pd.DataFrame):
                logger.error(
                    "市场快照批次失败: underlying=%s batch_id=%s error=%s",
                    underlying, batch_id, error,
                    exc_info=error is not None,
                )
                failures.append(batch_id)
                continue
            received_at = datetime.now(timezone.utc).isoformat()
            option_frame, underlying_frame = self._normalize_snapshot(
                frame, contracts=contracts, underlying=underlying, capture_id=capture_id,
                batch_id=batch_id, tier=tier, scheduled_at_utc=scheduled_at_utc,
                requested_at_utc=requested_at, received_at_utc=received_at,
            )
            all_quotes.append(option_frame)
            all_underlyings.append(underlying_frame)
            if on_batch is not None:
                on_batch(option_frame, underlying_frame, batch_id)
        quotes = pd.concat(all_quotes, ignore_index=True) if all_quotes else empty_frame(QUOTE_COLUMNS)
        underlyings = pd.concat(all_underlyings, ignore_index=True) if all_underlyings else empty_frame(UNDERLYING_COLUMNS)
        status = "complete" if not failures else "partial"
        if not quotes.empty:
            quotes["capture_status"] = status
        return quotes, underlyings, failures

    def snapshot_many(
        self,
        selections: Mapping[str, pd.DataFrame],
        *,
        capture_ids: Mapping[str, str],
        batch_prefix: str,
        tier: str,
        scheduled_at_utc: str,
        on_batch: Callable[[str, pd.DataFrame, pd.DataFrame, str], None] | None = None,
        max_attempts: int = 3,
    ) -> dict[str, tuple[pd.DataFrame, pd.DataFrame, list[str]]]:
        """合并多个标的的快照请求，并按标的拆分归一化结果。"""
        selected = {key: frame for key, frame in selections.items() if not frame.empty}
        result: dict[str, tuple[pd.DataFrame, pd.DataFrame, list[str]]] = {
            key: (empty_frame(QUOTE_COLUMNS), empty_frame(UNDERLYING_COLUMNS), [])
            for key in selections
        }
        if not selected:
            return result
        total_codes = sum(len(frame) + 1 for frame in selected.values())
        if total_codes > 400:
            raise ValueError(f"合并快照超过富途单次代码上限: codes={total_codes}")
        underlyings = list(selected)
        codes = underlyings + [
            str(row["contract_code"])
            for frame in selected.values()
            for row in frame.to_dict("records")
        ]
        requested_at = datetime.now(timezone.utc).isoformat()
        frame = None
        error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                self.limiter.market_snapshot.acquire(priority="high" if tier == "hot" else "normal")
                self.metrics["market_snapshot_calls"] = self.metrics.get("market_snapshot_calls", 0) + 1
                ret, data = self.context.get_market_snapshot(codes)
                frame = self._check(ret, data, api="get_market_snapshot", underlying=",".join(underlyings))
                error = None
                break
            except Exception as exc:
                error = exc
                if attempt >= max_attempts:
                    break
                self.metrics["retries"] += 1
                delay = min(4.0, 0.5 * (2 ** (attempt - 1)))
                logger.warning(
                    "合并市场快照失败，准备重试: underlyings=%s attempt=%s delay=%.2f error=%s",
                    ",".join(underlyings), attempt, delay, exc,
                )
                self.sleep(delay)
        if error is not None or not isinstance(frame, pd.DataFrame):
            logger.error(
                "合并市场快照失败: underlyings=%s error=%s",
                ",".join(underlyings), error, exc_info=error is not None,
            )
            for underlying in underlyings:
                result[underlying] = (empty_frame(QUOTE_COLUMNS), empty_frame(UNDERLYING_COLUMNS), [batch_prefix])
            return result

        received_at = datetime.now(timezone.utc).isoformat()
        for underlying in underlyings:
            capture_id = capture_ids[underlying]
            batch_id = f"{batch_prefix}_{underlying.replace('.', '_')}"
            option_frame, underlying_frame = self._normalize_snapshot(
                frame,
                contracts=selected[underlying],
                underlying=underlying,
                capture_id=capture_id,
                batch_id=batch_id,
                tier=tier,
                scheduled_at_utc=scheduled_at_utc,
                requested_at_utc=requested_at,
                received_at_utc=received_at,
            )
            if on_batch is not None:
                on_batch(underlying, option_frame, underlying_frame, batch_id)
            result[underlying] = (option_frame, underlying_frame, [])
        logger.info(
            "合并市场快照完成: underlyings=%s codes=%s calls=1",
            ",".join(underlyings), len(codes),
        )
        return result

    @staticmethod
    def _normalize_snapshot(
        frame: pd.DataFrame,
        *,
        contracts: pd.DataFrame,
        underlying: str,
        capture_id: str,
        batch_id: str,
        tier: str,
        scheduled_at_utc: str,
        requested_at_utc: str,
        received_at_utc: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        lookup = contracts.set_index("contract_code").to_dict("index")
        underlying_spot = None
        for raw in frame.to_dict("records"):
            if str(raw.get("code") or "") == underlying:
                underlying_spot = float_or_none(raw.get("last_price"))
                break
        option_rows: list[dict[str, Any]] = []
        underlying_rows: list[dict[str, Any]] = []
        for raw in frame.to_dict("records"):
            code = str(raw.get("code") or "")
            provider_time = as_utc(raw.get("update_time"))
            bid = float_or_none(raw.get("bid_price"))
            ask = float_or_none(raw.get("ask_price"))
            quality_flags: list[str] = []
            if bid is None:
                quality_flags.append("missing_bid")
            if ask is None:
                quality_flags.append("missing_ask")
            if bid == 0:
                quality_flags.append("zero_bid")
            if ask == 0:
                quality_flags.append("zero_ask")
            if bid is not None and ask is not None and bid > ask:
                quality_flags.append("crossed_market")
            is_stale = False
            if provider_time:
                try:
                    is_stale = (
                        pd.Timestamp(received_at_utc) - pd.Timestamp(provider_time)
                    ).total_seconds() > 300
                except (TypeError, ValueError):
                    quality_flags.append("invalid_provider_time")
            base = {
                "schema_version": 1,
                "provider": "futu_opend",
                "capture_id": capture_id,
                "batch_id": batch_id,
                "underlying": underlying,
                "scheduled_at_utc": scheduled_at_utc,
                "requested_at_utc": requested_at_utc,
                "received_at_utc": received_at_utc,
                "provider_time_utc": provider_time,
            }
            if code == underlying:
                underlying_rows.append({
                    **base,
                    "last": float_or_none(raw.get("last_price")),
                    "bid": bid,
                    "ask": ask,
                    "open": float_or_none(raw.get("open_price")),
                    "high": float_or_none(raw.get("high_price")),
                    "low": float_or_none(raw.get("low_price")),
                    "prev_close": float_or_none(raw.get("prev_close_price")),
                    "volume": float_or_none(raw.get("volume")),
                    "turnover": float_or_none(raw.get("turnover")),
                    "market_state": raw.get("sec_status"),
                    "quality_flags": ";".join(quality_flags),
                })
                continue
            meta = lookup.get(code)
            if meta is None:
                continue
            iv = float_or_none(raw.get("option_implied_volatility"))
            if iv is not None:
                iv /= 100.0
            option_rows.append({
                **base,
                "tier": tier,
                "contract_code": code,
                "expiry_date": meta.get("expiry_date"),
                "dte": meta.get("dte"),
                "right": meta.get("right"),
                "strike": meta.get("strike"),
                "last": float_or_none(raw.get("last_price")),
                "bid": bid,
                "ask": ask,
                "bid_size": float_or_none(raw.get("bid_vol")),
                "ask_size": float_or_none(raw.get("ask_vol")),
                "open": float_or_none(raw.get("open_price")),
                "high": float_or_none(raw.get("high_price")),
                "low": float_or_none(raw.get("low_price")),
                "prev_close": float_or_none(raw.get("prev_close_price")),
                "volume": float_or_none(raw.get("volume")),
                "turnover": float_or_none(raw.get("turnover")),
                "open_interest": float_or_none(raw.get("option_open_interest")),
                "iv": iv,
                "delta": float_or_none(raw.get("option_delta")),
                "gamma": float_or_none(raw.get("option_gamma")),
                "vega": float_or_none(raw.get("option_vega")),
                "theta": float_or_none(raw.get("option_theta")),
                "rho": float_or_none(raw.get("option_rho")),
                "spot": float_or_none(raw.get("underlying_last_price")) or underlying_spot,
                "underlying_quote_id": f"{capture_id}:{batch_id}:{underlying}",
                "capture_status": "complete",
                "is_stale": is_stale,
                "quality_flags": ";".join(quality_flags + (["stale_provider_time"] if is_stale else [])),
            })
        return (
            pd.DataFrame(option_rows, columns=list(QUOTE_COLUMNS)),
            pd.DataFrame(underlying_rows, columns=list(UNDERLYING_COLUMNS)),
        )

    def doctor(self) -> dict[str, Any]:
        """检查 OpenD 只读连接与额度。"""
        result: dict[str, Any] = {"provider": "futu_opend", "host": self.host, "port": self.port}
        try:
            try:
                import futu
                result["api_version"] = getattr(futu, "__version__", "unknown")
            except ImportError:
                result["api_version"] = "missing"
            ret, data = self.context.query_subscription()
            self._check(ret, data, api="query_subscription", underlying="*")
            result["subscription"] = data
            result["limiter"] = self.limiter.stats()
            result["metrics"] = self.metrics.copy()
            result["ok"] = True
        except Exception as exc:
            logger.exception("OpenD doctor 检查失败")
            result["ok"] = False
            result["error"] = str(exc)
        return result
