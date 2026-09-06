"""期权采集调度器。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import fcntl
import json
import logging
import os
from pathlib import Path
import signal
import shutil
import threading
import time as time_module
import uuid
from typing import Callable

import pandas as pd

from .config import HubConfig, resolve_data_dir
from .provider import FutuOptionProvider
from .quality import validate_quotes
from .schema import now_utc, trade_date
from .selection import select_hot, select_surface
from .storage import ParquetCompactor, RawSqliteStore

logger = logging.getLogger(__name__)


class SessionCalendar:
    """交易时段判断；exchange_calendars 不可用时使用工作日回退。"""

    def __init__(self, config: HubConfig):
        self.config = config
        self.timezone = config.market.timezone
        self._calendar = None
        try:
            import exchange_calendars as xcals
            self._calendar = xcals.get_calendar(config.market.calendar)
        except Exception:
            logger.warning("exchange_calendars 不可用，交易日历使用工作日回退", exc_info=True)

    def is_trading_day(self, local_date: date) -> bool:
        if self._calendar is None:
            return local_date.weekday() < 5
        try:
            return bool(self._calendar.is_session(pd.Timestamp(local_date)))
        except Exception:
            logger.warning("交易日历查询失败，使用工作日回退: date=%s", local_date, exc_info=True)
            return local_date.weekday() < 5

    def bounds(self, instant: datetime | None = None) -> tuple[datetime, datetime] | None:
        instant = instant or datetime.now(timezone.utc)
        timestamp = pd.Timestamp(instant)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        local = timestamp.tz_convert(self.timezone)
        local_date = local.date()
        if not self.is_trading_day(local_date):
            return None
        if self._calendar is not None:
            try:
                market_open = pd.Timestamp(self._calendar.session_open(pd.Timestamp(local_date))).tz_convert(self.timezone)
                market_close = pd.Timestamp(self._calendar.session_close(pd.Timestamp(local_date))).tz_convert(self.timezone)
                return market_open.to_pydatetime(), market_close.to_pydatetime()
            except Exception:
                logger.warning("读取交易所开收盘时间失败，使用配置时段: date=%s", local_date, exc_info=True)
        start_h, start_m = (int(value) for value in self.config.market.session_start.split(":"))
        end_h, end_m = (int(value) for value in self.config.market.session_end.split(":"))
        tz = local.tz
        start = pd.Timestamp(datetime.combine(local_date, time(start_h, start_m)), tz=tz).to_pydatetime()
        end = pd.Timestamp(datetime.combine(local_date, time(end_h, end_m)), tz=tz).to_pydatetime()
        return start, end

    def local_date(self, instant: datetime | None = None) -> date:
        timestamp = pd.Timestamp(instant or datetime.now(timezone.utc))
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        return timestamp.tz_convert(self.timezone).date()


@dataclass
class CollectorState:
    chain_asof: dict[str, str]
    contracts: dict[str, pd.DataFrame]
    spot: dict[str, float]
    last_hot_selection: datetime | None = None
    hot_selected: dict[str, pd.DataFrame] | None = None
    hot_selection_at: dict[str, datetime] | None = None
    hot_selection_spot: dict[str, float] | None = None


class OptionCollector:
    def __init__(
        self,
        config: HubConfig,
        *,
        provider: FutuOptionProvider | None = None,
        data_root: str | Path | None = None,
        clock=None,
    ):
        self.config = config
        self.root_dir = resolve_data_dir(data_root)
        self.provider = provider or FutuOptionProvider(
            host=config.provider.host,
            port=config.provider.port,
            market_snapshot_hot_reserved=config.provider.market_snapshot_hot_reserved,
        )
        self.raw = RawSqliteStore(self.root_dir)
        self.compactor = ParquetCompactor(self.root_dir, config=config)
        self.calendar = SessionCalendar(config)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.state = CollectorState(
            chain_asof={}, contracts={}, spot={}, hot_selected={},
            hot_selection_at={}, hot_selection_spot={},
        )
        self.lock_path = self.root_dir / "state" / "collector.lock"
        self._lock_handle = None
        self._stop_requested = False

    def _write_state(self, status: str, *, message: str | None = None) -> None:
        """原子写入状态和 heartbeat，供 launchd/运维查询。"""
        path = self.root_dir / "state" / "status.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "status": status,
            "updated_at_utc": now_utc().isoformat(),
            "pid": os.getpid(),
            "message": message,
        }
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temporary, path)
        heartbeat = path.with_name("heartbeat.json")
        heartbeat.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def acquire_lock(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_handle = self.lock_path.open("a+")
        try:
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._lock_handle.close()
            self._lock_handle = None
            raise RuntimeError(f"已有采集进程运行: lock={self.lock_path}") from exc

    def release_lock(self) -> None:
        if self._lock_handle is None:
            return
        try:
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
            self._lock_handle.close()
        except Exception:
            logger.warning("释放采集锁异常", exc_info=True)
        self._lock_handle = None

    def refresh_contracts(self, underlying: str, *, min_dte: int = 0, max_dte: int | None = None) -> pd.DataFrame:
        max_dte = max_dte if max_dte is not None else self.config.market.max_dte
        frame = self.provider.discover_contracts(
            underlying,
            min_dte=min_dte,
            max_dte=max_dte,
            asof=self.calendar.local_date(self.clock()),
        )
        self.state.contracts[underlying] = frame
        self.state.chain_asof[underlying] = now_utc().isoformat()
        if not frame.empty:
            self.raw.write_contracts(self.calendar.local_date(self.clock()).isoformat(), frame)
        logger.info("合约主数据刷新: underlying=%s rows=%s max_dte=%s", underlying, len(frame), max_dte)
        return frame

    def collect_once(
        self,
        underlying: str,
        tier: str,
        *,
        min_dte: int = 0,
        max_dte: int | None = None,
        scheduled_at: datetime | None = None,
        before_batch: Callable[[], None] | None = None,
    ) -> dict[str, object]:
        if tier not in {"anchor", "surface", "hot"}:
            raise ValueError(f"不支持的采集层级: {tier}")
        now = self.clock()
        bounds = self.calendar.bounds(now)
        local_date = self.calendar.local_date(now)
        scheduled_at = scheduled_at or now
        contracts = self.state.contracts.get(underlying)
        if contracts is None or contracts.empty or max_dte is not None:
            contracts = self.refresh_contracts(underlying, min_dte=min_dte, max_dte=max_dte)
        if contracts.empty:
            raise RuntimeError(f"没有可采集合约: underlying={underlying}")
        spot = self.state.spot.get(underlying)
        if spot is None:
            spot = self._underlying_spot(underlying)
            self.state.spot[underlying] = spot
        asof = local_date
        selected = self._select_contracts(underlying, tier, contracts, spot, asof)
        capture_id = f"{local_date.strftime('%Y%m%d')}_{tier}_{underlying.replace('.', '_')}_{uuid.uuid4().hex[:12]}"
        started = now_utc()
        scheduled_at_utc = scheduled_at.astimezone(timezone.utc).isoformat()

        self.raw.write_capture(
            trade_date=local_date.isoformat(), capture_id=capture_id, underlying=underlying,
            tier=tier, scheduled_at_utc=scheduled_at_utc,
            started_at_utc=started.isoformat(), completed_at_utc=started.isoformat(),
            expected_rows=len(selected), quotes=pd.DataFrame(), underlying_quotes=pd.DataFrame(),
            finalize=False,
        )

        def persist_batch(batch_quotes: pd.DataFrame, batch_underlyings: pd.DataFrame, _batch_id: str) -> None:
            self.raw.write_capture(
                trade_date=local_date.isoformat(), capture_id=capture_id, underlying=underlying,
                tier=tier, scheduled_at_utc=scheduled_at_utc,
                started_at_utc=started.isoformat(), completed_at_utc=now_utc().isoformat(),
                expected_rows=len(selected), quotes=batch_quotes, underlying_quotes=batch_underlyings,
                finalize=False,
            )

        quotes, underlyings, failures = self.provider.snapshot(
            underlying,
            selected,
            capture_id=capture_id,
            batch_prefix=capture_id,
            tier=tier,
            scheduled_at_utc=scheduled_at_utc,
            on_batch=persist_batch,
            before_batch=before_batch,
        )
        completed = now_utc()
        expected = len(selected)
        report = validate_quotes(quotes, expected_rows=expected)
        error = "; ".join(failures) if failures else None
        status = self.raw.finalize_capture(
            trade_date=local_date.isoformat(), capture_id=capture_id,
            expected_rows=expected, failed_batches=failures, error=error,
            completed_at_utc=completed.isoformat(),
        )
        if not underlyings.empty:
            last = pd.to_numeric(underlyings["last"], errors="coerce").dropna()
            if not last.empty:
                self.state.spot[underlying] = float(last.iloc[-1])
        result = {
            "capture_id": capture_id,
            "underlying": underlying,
            "tier": tier,
            "expected_rows": expected,
            "received_rows": len(quotes),
            "failed_batches": failures,
            "status": status,
            "quality": report,
        }
        logger.info("期权采集完成: %s", result)
        return result

    def _select_contracts(
        self,
        underlying: str,
        tier: str,
        contracts: pd.DataFrame,
        spot: float,
        asof: date,
    ) -> pd.DataFrame:
        if tier == "anchor":
            return contracts
        if tier == "surface":
            return select_surface(
                contracts, spot, asof=asof,
                min_dte=self.config.collection.surface_min_dte,
                max_dte=self.config.collection.surface_max_dte,
                min_moneyness=self.config.collection.surface_min_moneyness,
                max_moneyness=self.config.collection.surface_max_moneyness,
            )
        now = self.clock()
        selected_at = (self.state.hot_selection_at or {}).get(underlying)
        selected_spot = (self.state.hot_selection_spot or {}).get(underlying)
        threshold = self.config.collection.hot_reselect_price_change
        if (
            selected_at is not None
            and (now - selected_at).total_seconds() < self.config.collection.hot_reselect_seconds
            and selected_spot
            and abs(spot / selected_spot - 1.0) < threshold
            and self.state.hot_selected
            and underlying in self.state.hot_selected
        ):
            return self.state.hot_selected[underlying]
        selected = select_hot(
            contracts, spot, asof=asof,
            max_dte=self.config.collection.hot_max_dte,
            min_moneyness=self.config.collection.hot_moneyness_min,
            max_moneyness=self.config.collection.hot_moneyness_max,
            expiry_targets=self.config.collection.hot_expiry_targets,
            strikes_per_expiry=self.config.collection.hot_strikes_per_expiry,
            cap_per_underlying=self.config.collection.hot_cap_per_underlying,
        )
        if self.state.hot_selected is not None:
            self.state.hot_selected[underlying] = selected
        if self.state.hot_selection_at is not None:
            self.state.hot_selection_at[underlying] = now
        if self.state.hot_selection_spot is not None:
            self.state.hot_selection_spot[underlying] = spot
        return selected

    def collect_hot_cycle(self, *, scheduled_at: datetime | None = None) -> list[dict[str, object]]:
        """四个标的合并为一次市场快照，再拆成独立逻辑 capture。"""
        now = self.clock()
        local_date = self.calendar.local_date(now)
        scheduled_at = scheduled_at or now
        selections: dict[str, pd.DataFrame] = {}
        for item in self.config.underlyings:
            contracts = self.state.contracts.get(item.code)
            if contracts is None or contracts.empty:
                contracts = self.refresh_contracts(item.code)
            spot = self.state.spot.get(item.code)
            if spot is None:
                spot = self._underlying_spot(item.code)
                self.state.spot[item.code] = spot
            selections[item.code] = self._select_contracts(
                item.code, "hot", contracts, spot, local_date,
            )
        total_codes = sum(len(frame) + 1 for frame in selections.values())
        if total_codes > 400:
            raise RuntimeError(f"Hot 合并代码超过 400: codes={total_codes}")
        started = now_utc()
        scheduled_at_utc = scheduled_at.astimezone(timezone.utc).isoformat()
        capture_ids = {
            underlying: f"{local_date.strftime('%Y%m%d')}_hot_{underlying.replace('.', '_')}_{uuid.uuid4().hex[:12]}"
            for underlying in selections
        }
        for underlying, selected in selections.items():
            self.raw.write_capture(
                trade_date=local_date.isoformat(), capture_id=capture_ids[underlying],
                underlying=underlying, tier="hot", scheduled_at_utc=scheduled_at_utc,
                started_at_utc=started.isoformat(), completed_at_utc=started.isoformat(),
                expected_rows=len(selected), quotes=pd.DataFrame(), underlying_quotes=pd.DataFrame(),
                finalize=False,
            )

        def persist_batch(underlying: str, quotes: pd.DataFrame, underlyings: pd.DataFrame, batch_id: str) -> None:
            self.raw.write_capture(
                trade_date=local_date.isoformat(), capture_id=capture_ids[underlying],
                underlying=underlying, tier="hot", scheduled_at_utc=scheduled_at_utc,
                started_at_utc=started.isoformat(), completed_at_utc=now_utc().isoformat(),
                expected_rows=len(selections[underlying]), quotes=quotes,
                underlying_quotes=underlyings, finalize=False,
            )

        grouped = self.provider.snapshot_many(
            selections, capture_ids=capture_ids,
            batch_prefix=f"{local_date.strftime('%Y%m%d')}_hot_cycle_{uuid.uuid4().hex[:8]}",
            tier="hot", scheduled_at_utc=scheduled_at_utc, on_batch=persist_batch,
        )
        results: list[dict[str, object]] = []
        for underlying, selected in selections.items():
            quotes, underlyings, failures = grouped[underlying]
            report = validate_quotes(quotes, expected_rows=len(selected))
            status = self.raw.finalize_capture(
                trade_date=local_date.isoformat(), capture_id=capture_ids[underlying],
                expected_rows=len(selected), failed_batches=failures,
                error="; ".join(failures) if failures else None,
                completed_at_utc=now_utc().isoformat(),
            )
            if not underlyings.empty:
                last = pd.to_numeric(underlyings["last"], errors="coerce").dropna()
                if not last.empty:
                    self.state.spot[underlying] = float(last.iloc[-1])
            results.append({
                "capture_id": capture_ids[underlying], "underlying": underlying, "tier": "hot",
                "expected_rows": len(selected), "received_rows": len(quotes),
                "failed_batches": failures, "status": status, "quality": report,
            })
        logger.info("Hot 合并采集完成: codes=%s results=%s", total_codes, results)
        return results

    def _underlying_spot(self, underlying: str) -> float:
        self.provider.limiter.market_snapshot.acquire()
        self.provider.metrics["market_snapshot_calls"] = self.provider.metrics.get("market_snapshot_calls", 0) + 1
        ret, frame = self.provider.context.get_market_snapshot([underlying])
        frame = self.provider._check(ret, frame, api="get_market_snapshot", underlying=underlying)
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            raise RuntimeError(f"标的快照为空: underlying={underlying}")
        value = pd.to_numeric(frame.iloc[0].get("last_price"), errors="coerce")
        if pd.isna(value) or float(value) <= 0:
            raise RuntimeError(f"标的现价无效: underlying={underlying} data={frame.iloc[0].to_dict()}")
        return float(value)

    def run_forever(self) -> None:
        """全天运行；只在配置的美东交易时段生成三层采集任务。"""
        self.acquire_lock()
        self._stop_requested = False
        previous_sigterm = signal.getsignal(signal.SIGTERM)

        def request_stop(signum, _frame):
            self._stop_requested = True
            logger.info("收到停止信号，当前事务完成后退出: signal=%s", signum)

        signal.signal(signal.SIGTERM, request_stop)
        try:
            last_chain_date: date | None = None
            last_surface = 0.0
            next_hot_due = 0.0
            hot_running = False
            last_anchor: set[str] = set()
            last_compaction_attempt: date | None = None
            last_heartbeat = 0.0
            compaction_thread: threading.Thread | None = None
            compaction_error: str | None = None

            def run_compaction(trade_date_value: str) -> None:
                """在独立线程执行日终压缩，避免阻塞主循环心跳。"""
                nonlocal compaction_error
                try:
                    if self.compactor.has_current_manifest(trade_date_value):
                        logger.info("当前 Raw 已有有效 manifest，跳过重复 compaction: trade_date=%s", trade_date_value)
                        return
                    self.compactor.compact(trade_date_value)
                    self.compactor.prune(
                        older_than_days=self.config.retention.raw_days,
                        trash_days=self.config.retention.trash_days,
                        apply=True,
                    )
                    logger.info("收盘后自动 compaction 完成: trade_date=%s", trade_date_value)
                except FileNotFoundError:
                    logger.warning("收盘后没有可压缩数据: trade_date=%s", trade_date_value)
                except Exception as exc:
                    compaction_error = str(exc)
                    logger.exception("收盘后自动 compaction 失败: trade_date=%s", trade_date_value)
                finally:
                    # compactor 的 SQLite 连接由本线程创建，也必须由本线程关闭。
                    self.compactor.raw.close()

            def service_hot_if_due() -> None:
                nonlocal next_hot_due, hot_running
                if hot_running or not next_hot_due or time_module.monotonic() < next_hot_due:
                    return
                hot_running = True
                try:
                    self.collect_hot_cycle(scheduled_at=self.clock())
                except Exception:
                    logger.exception("Hot 合并采集失败")
                finally:
                    hot_running = False
                    next_hot_due = time_module.monotonic() + self.config.collection.hot_interval_seconds

            self._write_state("running")
            while not self._stop_requested:
                now = self.clock()
                monotonic_now = time_module.monotonic()
                if monotonic_now - last_heartbeat >= 60:
                    if compaction_thread is not None and compaction_thread.is_alive():
                        self._write_state("compacting", message="日终 Parquet 压缩进行中")
                    elif compaction_error:
                        self._write_state("degraded", message=f"日终压缩失败: {compaction_error}")
                    else:
                        self._write_state("running")
                    last_heartbeat = monotonic_now
                free_bytes = shutil.disk_usage(self.root_dir).free
                if free_bytes < self.config.retention.stop_free_gb * 1024**3:
                    message = f"磁盘剩余不足 {self.config.retention.stop_free_gb}GB: free_bytes={free_bytes}"
                    logger.error(message)
                    self._write_state("degraded", message=message)
                    time_module.sleep(60)
                    continue
                if free_bytes < self.config.retention.warn_free_gb * 1024**3:
                    logger.warning(
                        "磁盘剩余低于预警阈值: free_bytes=%s warn_gb=%s",
                        free_bytes, self.config.retention.warn_free_gb,
                    )
                bounds = self.calendar.bounds(now)
                if bounds is None:
                    time_module.sleep(30)
                    continue
                start, end = bounds
                current_timestamp = pd.Timestamp(now)
                if current_timestamp.tzinfo is None:
                    current_timestamp = current_timestamp.tz_localize("UTC")
                current_local = current_timestamp.tz_convert(self.config.market.timezone)
                elapsed = (current_local.to_pydatetime() - start).total_seconds()
                closing = (end - current_local.to_pydatetime()).total_seconds()
                if elapsed >= 0 and last_chain_date != current_local.date() and elapsed < 1800:
                    for item in self.config.underlyings:
                        try:
                            self.refresh_contracts(item.code)
                        except Exception:
                            logger.exception("合约刷新失败: underlying=%s", item.code)
                    last_chain_date = current_local.date()
                if 0 <= elapsed and closing >= 0:
                    if not next_hot_due:
                        next_hot_due = monotonic_now
                    service_hot_if_due()
                    if monotonic_now - last_surface >= self.config.collection.surface_interval_seconds:
                        for item in self.config.underlyings:
                            try:
                                self.collect_once(
                                    item.code, "surface", scheduled_at=now,
                                    before_batch=service_hot_if_due,
                                )
                            except Exception:
                                logger.exception("Surface 采集失败: underlying=%s", item.code)
                        last_surface = monotonic_now
                    midpoint_threshold = max(0, (end - start).total_seconds() / 2 - 30)
                    if closing <= 300:
                        anchor_name = "close"
                    elif elapsed >= midpoint_threshold:
                        anchor_name = "mid"
                    elif elapsed >= 300:
                        anchor_name = "open"
                    else:
                        anchor_name = ""
                    if anchor_name and anchor_name not in last_anchor:
                        for item in self.config.underlyings:
                            try:
                                self.collect_once(
                                    item.code, "anchor", scheduled_at=now,
                                    before_batch=service_hot_if_due,
                                )
                            except Exception:
                                logger.exception("Anchor 采集失败: underlying=%s anchor=%s", item.code, anchor_name)
                        last_anchor.add(anchor_name)
                if closing < 0:
                    last_anchor.clear()
                    if closing <= -1800 and last_compaction_attempt != current_local.date():
                        last_compaction_attempt = current_local.date()
                        compaction_error = None
                        self._write_state("compacting", message="日终 Parquet 压缩进行中")
                        compaction_thread = threading.Thread(
                            target=run_compaction,
                            args=(current_local.date().isoformat(),),
                            name="parquet-compaction",
                            daemon=False,
                        )
                        compaction_thread.start()
                time_module.sleep(1)
        finally:
            signal.signal(signal.SIGTERM, previous_sigterm)
            if compaction_thread is not None and compaction_thread.is_alive():
                logger.info("等待日终 compaction 完成后退出")
                compaction_thread.join()
            self._write_state("stopped", message="collector stopped")
            self.provider.close()
            self.raw.close()
            self.compactor.raw.close()
            self.release_lock()
