"""支持 python -m market_data_hub 启动 CLI。"""

from .cli import main


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
