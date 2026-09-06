from datetime import datetime, timezone
from pathlib import Path

from market_data_hub.collector import OptionCollector, SessionCalendar
from market_data_hub.config import HubConfig, UnderlyingConfig
from market_data_hub.schema import QUOTE_COLUMNS, UNDERLYING_COLUMNS
import pandas as pd


def _config() -> HubConfig:
    return HubConfig(underlyings=(UnderlyingConfig("US.TEST"),))


def test_session_calendar_converts_dst_and_rejects_weekend():
    calendar = SessionCalendar(_config())
    start, end = calendar.bounds(datetime(2026, 3, 9, 14, 0, tzinfo=timezone.utc))
    assert start.hour == 9 and start.minute == 30
    assert start.utcoffset().total_seconds() == -4 * 3600
    assert end.hour == 16
    assert calendar.bounds(datetime(2026, 3, 7, 14, 0, tzinfo=timezone.utc)) is None


def test_collector_lock_is_exclusive(tmp_path: Path):
    first = OptionCollector(_config(), data_root=tmp_path)
    second = OptionCollector(_config(), data_root=tmp_path)
    first.acquire_lock()
    try:
        try:
            second.acquire_lock()
        except RuntimeError as exc:
            assert "已有采集进程运行" in str(exc)
        else:
            raise AssertionError("第二个采集器不应获得锁")
    finally:
        first.release_lock()
        second.release_lock()
        first.raw.close()
        second.raw.close()


def test_collector_hot_cycle_persists_each_underlying_from_one_provider_call(tmp_path: Path, monkeypatch):
    config = HubConfig(underlyings=(UnderlyingConfig("US.A"), UnderlyingConfig("US.B")))

    class FakeProvider:
        def __init__(self):
            self.calls = 0

        def snapshot_many(self, selections, **kwargs):
            self.calls += 1
            result = {}
            for underlying, contracts in selections.items():
                quote_rows = []
                for row in contracts.to_dict("records"):
                    quote_rows.append({
                        "schema_version": 1, "provider": "test", "capture_id": kwargs["capture_ids"][underlying],
                        "batch_id": "b", "tier": "hot", "underlying": underlying,
                        "contract_code": row["contract_code"], "expiry_date": "2026-08-10", "dte": 4,
                        "right": "C", "strike": 100.0, "scheduled_at_utc": kwargs["scheduled_at_utc"],
                        "requested_at_utc": kwargs["scheduled_at_utc"], "received_at_utc": kwargs["scheduled_at_utc"],
                        "provider_time_utc": kwargs["scheduled_at_utc"], "last": 1.0, "bid": .9, "ask": 1.1,
                        "bid_size": 1, "ask_size": 1, "open": 1, "high": 1, "low": 1, "prev_close": 1,
                        "volume": 1, "turnover": 1, "open_interest": 1, "iv": .5, "delta": .5,
                        "gamma": .1, "vega": .1, "theta": -.1, "rho": .01, "spot": 100,
                        "underlying_quote_id": "u", "capture_status": "complete", "is_stale": False,
                        "quality_flags": "",
                    })
                quote_frame = pd.DataFrame(quote_rows, columns=list(QUOTE_COLUMNS))
                underlying_frame = pd.DataFrame([{
                    "schema_version": 1, "provider": "test", "capture_id": kwargs["capture_ids"][underlying],
                    "batch_id": "b", "underlying": underlying,
                    "scheduled_at_utc": kwargs["scheduled_at_utc"],
                    "requested_at_utc": kwargs["scheduled_at_utc"],
                    "received_at_utc": kwargs["scheduled_at_utc"],
                    "provider_time_utc": kwargs["scheduled_at_utc"], "last": 100, "bid": 99,
                    "ask": 101, "open": 100, "high": 100, "low": 100, "prev_close": 100,
                    "volume": 1, "turnover": 1, "market_state": "OPEN", "quality_flags": "",
                }], columns=list(UNDERLYING_COLUMNS))
                kwargs["on_batch"](underlying, quote_frame, underlying_frame, "b")
                result[underlying] = (quote_frame, underlying_frame, [])
            return result

        def close(self):
            return None

    provider = FakeProvider()
    collector = OptionCollector(config, provider=provider, data_root=tmp_path)
    contracts = pd.DataFrame([
        {"underlying": "US.A", "contract_code": "A1"},
        {"underlying": "US.A", "contract_code": "A2"},
    ])
    collector.state.contracts = {"US.A": contracts, "US.B": contracts.assign(underlying="US.B", contract_code=["B1", "B2"])}
    collector.state.spot = {"US.A": 100.0, "US.B": 100.0}
    monkeypatch.setattr("market_data_hub.collector.select_hot", lambda frame, *args, **kwargs: frame)
    results = collector.collect_hot_cycle()
    assert provider.calls == 1
    assert {item["underlying"] for item in results} == {"US.A", "US.B"}
    captures = collector.raw.read_table(collector.calendar.local_date().isoformat(), "captures")
    assert set(captures["status"]) == {"complete"}
    collector.raw.close()
