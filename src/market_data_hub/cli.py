"""market-data CLI。"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import shutil

from .collector import OptionCollector, SessionCalendar
from .config import HubConfig, resolve_data_dir
from .logging_config import configure_logging
from .provider import FutuOptionProvider
from .quality import cadence_report, validate_quotes
from .reader import OptionDataReader
from .storage import ParquetCompactor, RawSqliteStore, import_legacy_snapshots

logger = logging.getLogger(__name__)


def _config(args) -> HubConfig:
    return HubConfig.load(args.config)


def cmd_doctor(args) -> int:
    config = _config(args)
    root = resolve_data_dir(args.data_dir)
    provider = FutuOptionProvider(
        host=config.provider.host,
        port=config.provider.port,
        market_snapshot_hot_reserved=config.provider.market_snapshot_hot_reserved,
    )
    try:
        result = provider.doctor()
        result["data_root"] = str(root)
        result["free_bytes"] = shutil.disk_usage(root if root.exists() else root.parent).free
        result["timezone"] = config.market.timezone
        result["calendar"] = config.market.calendar
        result["session_bounds"] = [
            value.isoformat()
            for value in SessionCalendar(config).bounds(datetime.now(timezone.utc)) or ()
        ]
        if result["free_bytes"] < 10 * 1024**3:
            result["ok"] = False
            result["disk_error"] = "剩余磁盘空间低于 10GB"
        logger.info("OpenD doctor 结果: %s", result)
        return 0 if result.get("ok") else 1
    finally:
        provider.close()


def cmd_options_run(args) -> int:
    config = _config(args)
    collector = OptionCollector(config, data_root=args.data_dir)
    try:
        collector.run_forever()
    except KeyboardInterrupt:
        logger.info("收到退出信号，采集服务停止")
        return 0
    except Exception:
        logger.exception("实时采集服务异常退出")
        return 1
    return 0


def cmd_collect_once(args) -> int:
    config = _config(args)
    collector = OptionCollector(config, data_root=args.data_dir)
    try:
        collector.acquire_lock()
        result = collector.collect_once(
            args.underlying,
            args.tier,
            min_dte=args.min_dte,
            max_dte=args.max_dte,
        )
        logger.info("单次采集结果: %s", result)
        return 0 if result["status"] == "complete" else 2
    except Exception:
        logger.exception("单次期权采集失败: underlying=%s tier=%s", args.underlying, args.tier)
        return 1
    finally:
        collector.provider.close()
        collector.raw.close()
        collector.release_lock()


def cmd_collect_hot_cycle(args) -> int:
    """一次性验证多标的合并 Hot 请求。"""
    config = _config(args)
    collector = OptionCollector(config, data_root=args.data_dir)
    try:
        now = collector.clock()
        bounds = collector.calendar.bounds(now)
        if bounds is None or not (bounds[0] <= now <= bounds[1]):
            logger.error("当前不在美股常规交易时段，拒绝执行 Hot cycle smoke")
            return 2
        collector.acquire_lock()
        results = collector.collect_hot_cycle()
        return 0 if all(item["status"] == "complete" for item in results) else 2
    except Exception:
        logger.exception("合并 Hot cycle 失败")
        return 1
    finally:
        collector.provider.close()
        collector.raw.close()
        collector.release_lock()


def cmd_compact(args) -> int:
    config = _config(args)
    compactor = ParquetCompactor(args.data_dir, config=config)
    try:
        manifest = compactor.compact(args.trade_date)
        logger.info("压缩 manifest: %s", manifest)
        return 0
    except Exception:
        logger.exception("日数据压缩失败: trade_date=%s", args.trade_date)
        return 1
    finally:
        compactor.raw.close()


def cmd_validate(args) -> int:
    config = _config(args)
    reader = OptionDataReader(args.data_dir)
    frame = reader.quotes(start=args.trade_date, end=args.trade_date, complete_only=False)
    manifest_report = reader.daily_quality(args.trade_date)
    report = {**validate_quotes(frame), **(manifest_report.get("quality") or {})}
    if "cadence" not in report:
        raw = RawSqliteStore(args.data_dir)
        try:
            captures = raw.read_table(args.trade_date, "captures")
            report["cadence"] = cadence_report(
                captures,
                trade_date=args.trade_date,
                underlyings=config.underlying_codes(),
                calendar_name=config.market.calendar,
                intervals={
                    "hot": config.collection.hot_interval_seconds,
                    "surface": config.collection.surface_interval_seconds,
                    "anchor": 0,
                },
                thresholds={"hot": 0.95, "surface": 0.98, "anchor": 1.0},
            )
            report["slo_passed"] = report["cadence"]["slo_passed"]
        finally:
            raw.close()
    report["reader_rows"] = int(len(frame))
    report["trade_date"] = args.trade_date
    logger.info("质量报告: %s", report)
    output = resolve_data_dir(args.data_dir) / "reports" / "quality" / f"{args.trade_date}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if not report["passed"]:
        return 1
    if not report.get("slo_passed", True):
        return 2
    return 0


def cmd_status(args) -> int:
    root = resolve_data_dir(args.data_dir)
    state = {
        "data_root": str(root),
        "exists": root.exists(),
        "free_bytes": shutil.disk_usage(root if root.exists() else root.parent).free,
        "latest_valid": None,
        "runtime": None,
    }
    latest = root / "manifests" / "daily" / "latest_valid.json"
    if latest.exists():
        state["latest_valid"] = json.loads(latest.read_text(encoding="utf-8"))
    runtime = root / "state" / "status.json"
    if runtime.exists():
        state["runtime"] = json.loads(runtime.read_text(encoding="utf-8"))
    if args.json:
        logger.info("状态 JSON: %s", json.dumps(state, ensure_ascii=False))
    else:
        logger.info("数据平台状态: %s", state)
    return 0


def cmd_import_legacy(args) -> int:
    try:
        results = import_legacy_snapshots(args.source, args.data_dir)
        logger.info("旧数据导入完成: files=%s", len(results))
        return 0
    except Exception:
        logger.exception("旧期权数据导入失败: source=%s", args.source)
        return 1


def cmd_prune(args) -> int:
    compactor = ParquetCompactor(args.data_dir)
    try:
        paths = compactor.prune(
            older_than_days=args.days, trash_days=args.trash_days, apply=args.apply,
        )
        logger.info(
            "原始数据清理结果: apply=%s raw_days=%s trash_days=%s candidates=%s",
            args.apply, args.days, args.trash_days, paths,
        )
        return 0
    finally:
        compactor.raw.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="market-data", description="美股期权研究数据平台")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--data-dir", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    doctor = sub.add_parser("doctor", help="检查 OpenD 和本地数据环境")
    doctor.add_argument("--config", type=Path, default=None)
    doctor.set_defaults(func=cmd_doctor)

    options = sub.add_parser("options", help="期权行情采集")
    option_sub = options.add_subparsers(dest="options_command", required=True)
    run = option_sub.add_parser("run", help="运行常驻采集服务")
    run.add_argument("--config", type=Path, default=None)
    run.set_defaults(func=cmd_options_run)
    once = option_sub.add_parser("collect-once", help="采集一次逻辑快照")
    once.add_argument("--config", type=Path, default=None)
    once.add_argument("--underlying", required=True)
    once.add_argument("--tier", choices=["anchor", "surface", "hot"], default="anchor")
    once.add_argument("--min-dte", type=int, default=0)
    once.add_argument("--max-dte", type=int, default=None)
    once.set_defaults(func=cmd_collect_once)
    hot_cycle = option_sub.add_parser("collect-hot-cycle", help="合并采集所有标的 Hot 快照")
    hot_cycle.add_argument("--config", type=Path, default=None)
    hot_cycle.set_defaults(func=cmd_collect_hot_cycle)
    compact = option_sub.add_parser("compact", help="压缩交易日原始库")
    compact.add_argument("--trade-date", required=True)
    compact.add_argument("--config", type=Path, default=None)
    compact.set_defaults(func=cmd_compact)
    validate = option_sub.add_parser("validate", help="校验交易日行情")
    validate.add_argument("--trade-date", required=True)
    validate.add_argument("--config", type=Path, default=None)
    validate.set_defaults(func=cmd_validate)
    status = option_sub.add_parser("status", help="显示采集状态")
    status.add_argument("--json", action="store_true")
    status.set_defaults(func=cmd_status)
    legacy = option_sub.add_parser("import-legacy", help="导入 intraday_lab 旧快照")
    legacy.add_argument("--source", required=True, type=Path)
    legacy.set_defaults(func=cmd_import_legacy)

    retention = sub.add_parser("retention", help="原始库保留治理")
    retention_sub = retention.add_subparsers(dest="retention_command", required=True)
    prune = retention_sub.add_parser("prune", help="移入 trash")
    prune.add_argument("--days", type=int, default=7)
    prune.add_argument("--trash-days", type=int, default=3)
    prune.add_argument("--apply", action="store_true")
    prune.set_defaults(func=cmd_prune)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    log_path = resolve_data_dir(args.data_dir) / "logs" / "collector.log"
    configure_logging(args.log_level, log_path=log_path)
    return int(args.func(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
