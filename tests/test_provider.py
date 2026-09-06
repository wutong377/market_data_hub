from datetime import date
import sys
import types

import pandas as pd

from market_data_hub.provider import FutuOptionProvider
from market_data_hub.ratelimit import RateLimiterSet


class _FakeContext:
    def __init__(self):
        self.snapshot_batches = []

    def get_option_expiration_date(self, underlying):
        return 0, pd.DataFrame({
            "strike_time": ["2026-08-03", "2026-08-10", "2026-09-07"],
            "option_expiry_date_distance": [0, 7, 35],
        })

    def get_option_chain(self, underlying, start, end, option_type):
        rows = []
        for expiry, dte in (("2026-08-03", 0), ("2026-08-10", 7), ("2026-09-07", 35)):
            if start <= expiry <= end:
                rows.extend([
                    {"code": f"{underlying}-{expiry}-100-C", "strike_time": expiry, "strike_price": 100, "option_type": "CALL", "lot_size": 100},
                    {"code": f"{underlying}-{expiry}-100-P", "strike_time": expiry, "strike_price": 100, "option_type": "PUT", "lot_size": 100},
                ])
        return 0, pd.DataFrame(rows)

    def get_market_snapshot(self, codes):
        self.snapshot_batches.append(list(codes))
        rows = [{"code": codes[0], "last_price": 100.0, "bid_price": 99.9, "ask_price": 100.1, "update_time": "2026-08-03 09:30:00"}]
        rows.extend({
            "code": code,
            "last_price": 1.0,
            "bid_price": 0.9,
            "ask_price": 1.1,
            "option_implied_volatility": 90.0,
            "option_open_interest": 10,
            "option_delta": 0.5,
            "update_time": "2026-08-03 09:30:00",
        } for code in codes[1:])
        return 0, pd.DataFrame(rows)

    def close(self):
        return None


def test_provider_discovers_30_day_windows_and_batches_snapshot(monkeypatch):
    fake_futu = types.ModuleType("futu")
    fake_futu.RET_OK = 0
    fake_futu.OptionType = types.SimpleNamespace(ALL="ALL")
    monkeypatch.setitem(sys.modules, "futu", fake_futu)

    context = _FakeContext()
    provider = FutuOptionProvider(
        context=context,
        limiter=RateLimiterSet(market_snapshot_limit=50, option_chain_limit=8),
        sleep=lambda _: None,
    )
    contracts = provider.discover_contracts("US.TEST", max_dte=35, asof=date(2026, 8, 3))
    assert len(contracts) == 6
    assert contracts["right"].tolist().count("C") == 3

    expanded = pd.concat([contracts] * 70, ignore_index=True)
    expanded["contract_code"] = [f"OPT-{index}" for index in range(len(expanded))]
    quotes, underlyings, failures = provider.snapshot(
        "US.TEST", expanded, capture_id="capture-1", batch_prefix="batch-1", tier="hot",
        scheduled_at_utc="2026-08-03T13:30:00+00:00",
    )
    assert not failures
    assert len(context.snapshot_batches) == 2
    assert max(len(batch) for batch in context.snapshot_batches) <= 400
    assert len(quotes) == len(expanded)
    assert len(underlyings) == 2
    assert quotes["iv"].iloc[0] == 0.9
    assert quotes["spot"].notna().all()
    assert provider.metrics["market_snapshot_calls"] == 2
    assert provider.limiter.stats()["get_market_snapshot"]["calls"] == 2


def test_provider_snapshot_many_uses_one_request_and_splits_results():
    class MultiContext:
        def __init__(self):
            self.calls = []

        def get_market_snapshot(self, codes):
            self.calls.append(list(codes))
            underlyings = {"US.A", "US.B"}
            rows = []
            for code in codes:
                if code in underlyings:
                    rows.append({
                        "code": code, "last_price": 100.0, "bid_price": 99.9,
                        "ask_price": 100.1, "update_time": "2026-08-03 09:30:00",
                    })
                else:
                    rows.append({
                        "code": code, "last_price": 1.0, "bid_price": .9,
                        "ask_price": 1.1, "option_implied_volatility": 90.0,
                        "update_time": "2026-08-03 09:30:00",
                    })
            return 0, pd.DataFrame(rows)

    context = MultiContext()
    provider = FutuOptionProvider(
        context=context,
        limiter=RateLimiterSet(market_snapshot_limit=50, option_chain_limit=8),
        sleep=lambda _: None,
    )
    contracts = pd.DataFrame([
        {"contract_code": "A1", "expiry_date": "2026-08-03", "dte": 0, "right": "C", "strike": 100},
        {"contract_code": "A2", "expiry_date": "2026-08-03", "dte": 0, "right": "P", "strike": 100},
    ])
    selections = {"US.A": contracts, "US.B": contracts.assign(contract_code=["B1", "B2"])}
    result = provider.snapshot_many(
        selections,
        capture_ids={"US.A": "ca", "US.B": "cb"},
        batch_prefix="cycle",
        tier="hot",
        scheduled_at_utc="2026-08-03T13:30:00+00:00",
    )
    assert len(context.calls) == 1
    assert len(context.calls[0]) == 6
    assert len(result["US.A"][0]) == 2
    assert len(result["US.B"][0]) == 2
    assert len(result["US.A"][1]) == 1
    assert len(result["US.B"][1]) == 1
    assert provider.limiter.stats()["get_market_snapshot"]["calls"] == 1
