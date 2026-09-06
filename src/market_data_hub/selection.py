"""期权曲面和 Hot 合约选择。"""

from __future__ import annotations

from datetime import date
import logging
import math

import pandas as pd

logger = logging.getLogger(__name__)


def _eligible(frame: pd.DataFrame, spot: float, *, max_dte: int, low: float, high: float, asof: date) -> pd.DataFrame:
    if frame.empty or spot <= 0:
        return frame.iloc[0:0].copy()
    out = frame.copy()
    out["expiry_date"] = pd.to_datetime(out["expiry_date"], errors="coerce").dt.date
    out["dte"] = (pd.to_datetime(out["expiry_date"]) - pd.Timestamp(asof)).dt.days
    out["strike"] = pd.to_numeric(out["strike"], errors="coerce")
    out = out[out["dte"].between(0, max_dte) & out["strike"].notna()]
    out = out[(out["strike"] / float(spot)).between(low, high)]
    return out.drop_duplicates("contract_code").copy()


def select_surface(
    contracts: pd.DataFrame,
    spot: float,
    *,
    asof: date,
    min_dte: int = 0,
    max_dte: int = 90,
    min_moneyness: float = 0.70,
    max_moneyness: float = 1.30,
) -> pd.DataFrame:
    """选择动态波动率曲面合约。"""
    return _eligible(
        contracts, spot, max_dte=max_dte, low=min_moneyness, high=max_moneyness, asof=asof
    ).loc[lambda frame: frame["dte"].between(min_dte, max_dte)].sort_values(
        ["dte", "strike", "right", "contract_code"]
    ).reset_index(drop=True)


def select_hot(
    contracts: pd.DataFrame,
    spot: float,
    *,
    asof: date,
    max_dte: int = 45,
    min_moneyness: float = 0.85,
    max_moneyness: float = 1.15,
    expiry_targets: tuple[int, ...] = (0, 7, 14, 28),
    strikes_per_expiry: int = 12,
    cap_per_underlying: int = 96,
) -> pd.DataFrame:
    """选择四个期限、每期限近价执行价的 Hot 合约，保证不超过上限。"""
    eligible = _eligible(
        contracts, spot, max_dte=max_dte, low=min_moneyness, high=max_moneyness, asof=asof
    )
    if eligible.empty:
        return eligible
    selected_codes: list[str] = []
    expiry_values = sorted(int(value) for value in eligible["dte"].dropna().unique())
    chosen_expiries: list[int] = []
    for target in expiry_targets:
        if not expiry_values:
            break
        candidate = min(expiry_values, key=lambda value: (abs(value - target), value))
        if candidate not in chosen_expiries:
            chosen_expiries.append(candidate)
    for dte in chosen_expiries:
        bucket = eligible[eligible["dte"] == dte].copy()
        bucket["distance"] = (bucket["strike"] / float(spot)).map(lambda value: abs(math.log(value)))
        strikes = sorted(bucket["strike"].dropna().unique(), key=lambda value: abs(float(value) / spot - 1.0))
        for strike in strikes[:strikes_per_expiry]:
            selected_codes.extend(bucket.loc[bucket["strike"] == strike, "contract_code"].astype(str).tolist())
    if len(selected_codes) < cap_per_underlying:
        rest = eligible[~eligible["contract_code"].isin(selected_codes)].copy()
        rest["distance"] = (rest["strike"] / float(spot)).map(lambda value: abs(math.log(value)))
        rest = rest.sort_values(["distance", "dte", "right", "contract_code"])
        selected_codes.extend(rest["contract_code"].astype(str).tolist())
    selected_codes = list(dict.fromkeys(selected_codes))[:cap_per_underlying]
    result = eligible[eligible["contract_code"].isin(selected_codes)].copy()
    order = {code: index for index, code in enumerate(selected_codes)}
    result["_order"] = result["contract_code"].map(order)
    return result.sort_values("_order").drop(columns="_order").reset_index(drop=True)
