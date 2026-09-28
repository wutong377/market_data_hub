import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from market_data_hub.schema import QUOTE_COLUMNS, UNDERLYING_COLUMNS
from market_data_hub.reader import OptionDataReader
from market_data_hub.storage import ParquetCompactor, RawSqliteStore, import_legacy_snapshots


def _quote_frame() -> pd.DataFrame:
    return pd.DataFrame([{
        "schema_version": 1, "provider": "test", "capture_id": "c1", "batch_id": "b1",
        "tier": "hot", "underlying": "US.TEST", "contract_code": "OPT1", "expiry_date": "2026-08-03",
        "dte": 0, "right": "C", "strike": 100.0, "scheduled_at_utc": "2026-08-03T13:30:00+00:00",
        "requested_at_utc": "2026-08-03T13:30:00+00:00", "received_at_utc": "2026-08-03T13:30:01+00:00",
        "provider_time_utc": "2026-08-03T13:30:00+00:00", "last": 1.0, "bid": .9, "ask": 1.1,
        "bid_size": 10, "ask_size": 12, "open": 1.0, "high": 1.1, "low": .8, "prev_close": .95,
        "volume": 100, "turnover": 10000, "open_interest": 200, "iv": .5, "delta": .5, "gamma": .1,
        "vega": .2, "theta": -.1, "rho": .01, "spot": 100, "underlying_quote_id": "u1",
        "capture_status": "complete", "is_stale": False, "quality_flags": "",
    }], columns=list(QUOTE_COLUMNS))


def test_sqlite_capture_and_compaction(tmp_path: Path):
    store = RawSqliteStore(tmp_path)
    quotes = _quote_frame()
    underlying = pd.DataFrame([{
        "schema_version": 1, "provider": "test", "capture_id": "c1", "batch_id": "b1",
        "underlying": "US.TEST", "scheduled_at_utc": "2026-08-03T13:30:00+00:00",
        "requested_at_utc": "2026-08-03T13:30:00+00:00", "received_at_utc": "2026-08-03T13:30:01+00:00",
        "provider_time_utc": "2026-08-03T13:30:00+00:00", "last": 100, "bid": 99.9, "ask": 100.1,
        "open": 100, "high": 101, "low": 99, "prev_close": 100, "volume": 1000, "turnover": 100000,
        "market_state": "OPEN", "quality_flags": "",
    }], columns=list(UNDERLYING_COLUMNS))
    store.write_capture(
        trade_date="2026-08-03", capture_id="c1", underlying="US.TEST", tier="hot",
        scheduled_at_utc="2026-08-03T13:30:00+00:00", started_at_utc="2026-08-03T13:30:00+00:00",
        completed_at_utc="2026-08-03T13:30:01+00:00", expected_rows=1, quotes=quotes,
        underlying_quotes=underlying,
    )
    assert len(store.read_table("2026-08-03", "option_quotes")) == 1
    store.close()
    compactor = ParquetCompactor(tmp_path)
    manifest = compactor.compact("2026-08-03")
    assert manifest["quote_rows"] == 1
    assert (tmp_path / "manifests" / "daily" / "2026-08-03_latest.json").exists()
    assert compactor.has_current_manifest("2026-08-03") is True
    reader = OptionDataReader(tmp_path)
    assert len(reader.quotes(end="2026-08-03T23:59:59Z", complete_only=True)) == 1
    assert len(reader.quotes(start="2026-08-03", end="2026-08-03", complete_only=True)) == 1
    compactor.raw.close()


def test_finalize_capture_does_not_rewrite_or_duplicate_rows(tmp_path: Path):
    store = RawSqliteStore(tmp_path)
    quotes = _quote_frame()
    store.write_capture(
        trade_date="2026-08-03", capture_id="c1", underlying="US.TEST", tier="hot",
        scheduled_at_utc="2026-08-03T13:30:00+00:00", started_at_utc="2026-08-03T13:30:00+00:00",
        completed_at_utc="2026-08-03T13:30:00+00:00", expected_rows=1,
        quotes=pd.DataFrame(), underlying_quotes=pd.DataFrame(), finalize=False,
    )
    store.write_capture(
        trade_date="2026-08-03", capture_id="c1", underlying="US.TEST", tier="hot",
        scheduled_at_utc="2026-08-03T13:30:00+00:00", started_at_utc="2026-08-03T13:30:00+00:00",
        completed_at_utc="2026-08-03T13:30:01+00:00", expected_rows=1,
        quotes=quotes, underlying_quotes=pd.DataFrame(), finalize=False,
    )
    assert store.finalize_capture(trade_date="2026-08-03", capture_id="c1", expected_rows=1) == "complete"
    assert len(store.read_table("2026-08-03", "option_quotes")) == 1
    store.close()


def test_reader_time_basis_scheduled_includes_late_received_batch(tmp_path: Path):
    store = RawSqliteStore(tmp_path)
    quotes = _quote_frame()
    quotes["received_at_utc"] = "2026-08-03T20:00:00.100000+00:00"
    store.write_capture(
        trade_date="2026-08-03", capture_id="c1", underlying="US.TEST", tier="hot",
        scheduled_at_utc="2026-08-03T19:59:59.900000+00:00", started_at_utc="2026-08-03T19:59:59.900000+00:00",
        completed_at_utc="2026-08-03T20:00:00.100000+00:00", expected_rows=1,
        quotes=quotes, underlying_quotes=pd.DataFrame(),
    )
    store.close()
    compactor = ParquetCompactor(tmp_path)
    compactor.compact("2026-08-03")
    reader = OptionDataReader(tmp_path)
    assert len(reader.quotes(end="2026-08-03T20:00:00Z", time_basis="scheduled")) == 1
    assert len(reader.quotes(end="2026-08-03T20:00:00Z", time_basis="received")) == 0
    compactor.raw.close()


def test_legacy_import_is_idempotent_and_converts_iv(tmp_path: Path):
    source = tmp_path / "legacy" / "US_TQQQ" / "date=2026-08-03"
    source.mkdir(parents=True)
    frame = pd.DataFrame([{
        "collected_at_utc": "2026-08-03T13:30:00+00:00", "stock_owner": "US.TQQQ",
        "code": "OPT1", "strike_time": "2026-08-08", "option_expiry_date_distance": 5,
        "option_type": "CALL", "option_strike_price": 100, "update_time": "2026-08-03 09:30:00",
        "last_price": 1, "bid_price": .9, "ask_price": 1.1, "bid_vol": 1, "ask_vol": 2,
        "open_price": 1, "high_price": 1, "low_price": 1, "prev_close_price": 1,
        "volume": 1, "turnover": 1, "option_open_interest": 3,
        "option_implied_volatility": 90, "option_delta": .5, "option_gamma": .1,
        "option_vega": .2, "option_theta": -.1, "option_rho": .01,
        "underlying_last_price": 100,
    }])
    path = source / "one.parquet"
    frame.to_parquet(path, index=False)
    import_legacy_snapshots(tmp_path / "legacy", tmp_path / "hub")
    import_legacy_snapshots(tmp_path / "legacy", tmp_path / "hub")
    quotes = OptionDataReader(tmp_path / "hub").quotes(
        start="2026-08-03", end="2026-08-03", complete_only=False,
    )
    assert len(quotes) == 1
    assert quotes["iv"].iloc[0] == .9


def test_interrupted_running_capture_becomes_partial_with_gap(tmp_path: Path):
    store = RawSqliteStore(tmp_path)
    store.write_capture(
        trade_date="2026-08-03", capture_id="running", underlying="US.TEST", tier="hot",
        scheduled_at_utc="2026-08-03T13:30:00+00:00", started_at_utc="2026-08-03T13:30:00+00:00",
        completed_at_utc="2026-08-03T13:30:01+00:00", expected_rows=2,
        quotes=_quote_frame(), underlying_quotes=pd.DataFrame(), finalize=False,
    )
    store.close()
    recovered = RawSqliteStore(tmp_path)
    captures = recovered.read_table("2026-08-03", "captures")
    gaps = recovered.read_table("2026-08-03", "gaps")
    assert captures.loc[captures["capture_id"] == "running", "status"].iloc[0] == "partial"
    assert "process_interrupted" in gaps["reason"].tolist()
    recovered.close()


def test_close_stale_dates_releases_previous_day_connection(tmp_path: Path):
    """跨交易日后必须关掉上一日连接：句柄存活期间被 prune 删除的 Raw 文件占用的磁盘不会被释放。"""
    store = RawSqliteStore(tmp_path)
    quotes = _quote_frame()
    for trade_date in ("2026-08-03", "2026-08-04"):
        store.write_capture(
            trade_date=trade_date, capture_id=f"c-{trade_date}", underlying="US.TEST", tier="hot",
            scheduled_at_utc=f"{trade_date}T13:30:00+00:00", started_at_utc=f"{trade_date}T13:30:00+00:00",
            completed_at_utc=f"{trade_date}T13:30:01+00:00", expected_rows=1, quotes=quotes,
            underlying_quotes=pd.DataFrame(), finalize=True,
        )
    assert set(store._connections) == {"2026-08-03", "2026-08-04"}
    previous_day = store._connections["2026-08-03"]

    closed = store.close_stale_dates("2026-08-04")

    assert closed == ["2026-08-03"]
    assert set(store._connections) == {"2026-08-04"}
    with pytest.raises(sqlite3.ProgrammingError):
        previous_day.execute("SELECT 1")
    store.close()
