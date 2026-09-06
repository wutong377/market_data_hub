"""配置模型与默认路径。"""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "options.yaml"
DEFAULT_DATA_DIR = Path("/Users/wutong/workspaces/py/market_data_hub_data")


@dataclass(frozen=True)
class ProviderConfig:
    name: str = "futu_opend"
    host: str = "127.0.0.1"
    port: int = 11111
    market_snapshot_limit: int = 50
    option_chain_limit: int = 8
    market_snapshot_hot_reserved: int = 8


@dataclass(frozen=True)
class MarketConfig:
    timezone: str = "America/New_York"
    calendar: str = "XNYS"
    session_start: str = "09:30"
    session_end: str = "16:00"
    max_dte: int = 180


@dataclass(frozen=True)
class CollectionConfig:
    surface_interval_seconds: int = 60
    hot_interval_seconds: int = 5
    hot_reselect_seconds: int = 300
    surface_min_dte: int = 0
    surface_max_dte: int = 90
    surface_min_moneyness: float = 0.70
    surface_max_moneyness: float = 1.30
    hot_max_dte: int = 45
    hot_moneyness_min: float = 0.85
    hot_moneyness_max: float = 1.15
    hot_expiry_targets: tuple[int, ...] = (0, 7, 14, 28)
    hot_strikes_per_expiry: int = 12
    hot_cap_per_underlying: int = 96
    hot_reselect_price_change: float = 0.01


@dataclass(frozen=True)
class RetentionConfig:
    """Raw 与 trash 的生命周期配置。"""

    raw_days: int = 7
    trash_days: int = 3
    warn_free_gb: int = 30
    stop_free_gb: int = 10


@dataclass(frozen=True)
class UnderlyingConfig:
    code: str
    name: str = ""


@dataclass(frozen=True)
class HubConfig:
    version: int = 1
    provider: ProviderConfig = field(default_factory=ProviderConfig)
    market: MarketConfig = field(default_factory=MarketConfig)
    collection: CollectionConfig = field(default_factory=CollectionConfig)
    retention: RetentionConfig = field(default_factory=RetentionConfig)
    underlyings: tuple[UnderlyingConfig, ...] = ()
    config_path: Path | None = None

    @classmethod
    def load(cls, path: str | Path | None = None) -> "HubConfig":
        config_path = Path(path or os.getenv("MARKET_DATA_HUB_CONFIG", DEFAULT_CONFIG))
        with config_path.open("r", encoding="utf-8") as handle:
            payload: dict[str, Any] = yaml.safe_load(handle) or {}
        provider = ProviderConfig(**(payload.get("provider") or {}))
        market = MarketConfig(**(payload.get("market") or {}))
        collection_payload = dict(payload.get("collection") or {})
        if "hot_expiry_targets" in collection_payload:
            collection_payload["hot_expiry_targets"] = tuple(
                int(item) for item in collection_payload["hot_expiry_targets"]
            )
        collection = CollectionConfig(**collection_payload)
        retention = RetentionConfig(**(payload.get("retention") or {}))
        underlyings = tuple(
            UnderlyingConfig(**item) for item in (payload.get("underlyings") or [])
        )
        if not underlyings:
            raise ValueError(f"配置未定义 underlyings: {config_path}")
        return cls(
            version=int(payload.get("version", 1)),
            provider=provider,
            market=market,
            collection=collection,
            retention=retention,
            underlyings=underlyings,
            config_path=config_path,
        )

    def underlying_codes(self) -> tuple[str, ...]:
        return tuple(item.code for item in self.underlyings)


def resolve_data_dir(path: str | Path | None = None) -> Path:
    """解析数据目录，优先使用参数，其次使用环境变量。"""
    return Path(path or os.getenv("MARKET_DATA_HUB_DATA_DIR", DEFAULT_DATA_DIR))
