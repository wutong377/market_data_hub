from pathlib import Path

from market_data_hub.config import HubConfig


def test_default_config_loads_all_targets():
    config = HubConfig.load(Path(__file__).parents[1] / "configs" / "options.yaml")
    assert config.underlying_codes() == ("US.QQQ", "US.TQQQ", "US.MSTR", "US.TSLA")
    assert config.provider.market_snapshot_limit == 50
    assert config.collection.hot_cap_per_underlying == 96
    assert config.collection.hot_expiry_targets == (0, 7, 14, 28)
