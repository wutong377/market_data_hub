from datetime import date

import pandas as pd

from market_data_hub.selection import select_hot, select_surface


def _contracts() -> pd.DataFrame:
    rows = []
    for dte in (0, 7, 21, 45, 60):
        expiry = (date(2026, 8, 3) + pd.Timedelta(days=dte)).isoformat()
        for strike in range(90, 111):
            for right in ("C", "P"):
                rows.append({
                    "underlying": "US.TEST",
                    "contract_code": f"US.TEST{dte}{strike}{right}",
                    "expiry_date": expiry,
                    "dte": dte,
                    "right": right,
                    "strike": float(strike),
                })
    return pd.DataFrame(rows)


def test_hot_selection_is_capped_and_balanced():
    result = select_hot(
        _contracts(), 100.0, asof=date(2026, 8, 3),
        expiry_targets=(0, 7, 14, 28), strikes_per_expiry=12, cap_per_underlying=96,
    )
    assert len(result) <= 96
    assert set(result["right"]) == {"C", "P"}
    assert result["dte"].nunique() == 4


def test_surface_selection_respects_dte_and_moneyness():
    result = select_surface(
        _contracts(), 100.0, asof=date(2026, 8, 3),
        min_dte=0, max_dte=21, min_moneyness=0.95, max_moneyness=1.05,
    )
    assert result["dte"].between(0, 21).all()
    assert result["strike"].between(95, 105).all()
