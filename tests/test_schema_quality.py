import pandas as pd

from market_data_hub.quality import cadence_report, validate_quotes
from market_data_hub.schema import as_utc


def test_as_utc_localizes_futu_eastern_time():
    assert as_utc("2026-08-03 09:30:00") == "2026-08-03T13:30:00+00:00"


def test_quality_flags_crossed_market_but_does_not_fail():
    frame = pd.DataFrame([{
        "capture_id": "c1", "contract_code": "x", "bid": 2.0, "ask": 1.0,
        "last": 1.5, "volume": 0, "open_interest": 0, "iv": None,
    }])
    report = validate_quotes(frame)
    assert report["passed"]
    assert report["warnings"]


def test_cadence_report_separates_frequency_slo_from_integrity():
    captures = pd.DataFrame([
        {
            "underlying": "US.TEST", "tier": "hot", "status": "complete",
            "scheduled_at_utc": "2026-08-03T13:30:00+00:00",
            "started_at_utc": "2026-08-03T13:30:01+00:00",
        },
    ])
    report = cadence_report(
        captures, trade_date="2026-08-03", underlyings=("US.TEST",),
        intervals={"hot": 5}, thresholds={"hot": .95},
    )
    assert report["slo_passed"] is False
    assert report["tiers"]["hot"]["passed"] is False
