"""严格比对 Raw SQLite 与 Curated Parquet 的数据一致性。

用途：确认 Parquet 是 Raw 的无损等价表示。清理前的对账只校验「行数 + SHA256」，
本脚本进一步做逐行逐字段的精确比较，用于在调整压缩逻辑或缩短 Raw 保留期之前建立信心。

Curated 侧按 manifest 列出的文件读取，与 ``OptionDataReader`` 的生产读取路径一致；
同时检测目录中未被 manifest 引用的孤儿 parquet（重复压缩会留下，占用磁盘但不影响读取）。

用法::

    python ops/verify_raw_vs_curated.py                    # 复核所有可用的交易日
    python ops/verify_raw_vs_curated.py 2026-09-09         # 复核指定交易日
    python ops/verify_raw_vs_curated.py --data-dir /path 2026-09-08 2026-09-09

数据目录取 ``--data-dir`` > ``MARKET_DATA_HUB_DATA_DIR`` > 包内默认值。
逐行比对按 (underlying, tier) 分区进行，最大分区会临时载入约 2M 行(
实测峰值内存数 GB)，建议在内存充足的机器上执行。
退出码：0 表示全部一致，1 表示存在不一致或孤儿文件。
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
import sqlite3
from pathlib import Path

import pandas as pd

from market_data_hub.config import resolve_data_dir

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("verify_raw_vs_curated")

KEY = ["capture_id", "contract_code"]


def load_raw(connection: sqlite3.Connection, underlying: str, tier: str) -> pd.DataFrame:
    """读取指定 (underlying, tier) 的全部 Raw 行并展开 payload_json。"""
    rows = connection.execute(
        "SELECT q.capture_id, q.contract_code, q.payload_json FROM option_quotes q "
        "JOIN captures c ON q.capture_id = c.capture_id "
        "WHERE c.underlying=? AND c.tier=?",
        (underlying, tier),
    ).fetchall()
    records: list[dict[str, object]] = []
    for capture_id, contract_code, payload in rows:
        record = json.loads(payload)
        # payload 内已含这两个键，此处以表列为权威值覆盖，与压缩逻辑保持一致。
        record["capture_id"] = capture_id
        record["contract_code"] = contract_code
        records.append(record)
    return pd.DataFrame(records)


def partition_dir(data_root: Path, trade_date: str, underlying: str, tier: str) -> Path:
    """某个 (underlying, tier) 分区的 Curated 目录。"""
    return (
        data_root / "curated" / "option_quotes" / f"trade_date={trade_date}"
        / f"underlying={underlying}" / f"tier={tier}"
    )


def manifest_paths(manifest: dict, directory: Path) -> list[Path]:
    """manifest 中属于该分区的 Parquet 路径（生产读取路径与之一致）。"""
    return sorted(
        Path(item["path"]) for item in manifest.get("datasets", [])
        if item.get("dataset") == "option_quotes"
        and Path(item.get("path", "")).parent == directory
    )


def load_curated(paths: list[Path]) -> pd.DataFrame:
    """读取 manifest 指定的 Curated Parquet 分片。"""
    if not paths:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(path) for path in paths], ignore_index=True)


def compare(raw_df: pd.DataFrame, curated_df: pd.DataFrame) -> list[str]:
    """逐行逐字段比对，返回问题描述列表；空列表表示严格一致。"""
    issues: list[str] = []
    if len(raw_df) != len(curated_df):
        issues.append(f"行数不一致 raw={len(raw_df)} parquet={len(curated_df)}")
        return issues
    raw_columns, curated_columns = set(raw_df.columns), set(curated_df.columns)
    if raw_columns != curated_columns:
        issues.append(
            f"列集合不一致 仅raw={sorted(raw_columns - curated_columns)} "
            f"仅parquet={sorted(curated_columns - raw_columns)}"
        )
    columns = sorted(raw_columns & curated_columns)
    raw_sorted = raw_df.sort_values(KEY).reset_index(drop=True)[columns]
    curated_sorted = curated_df.sort_values(KEY).reset_index(drop=True)[columns]

    key_mismatch = int((raw_sorted[KEY] != curated_sorted[KEY]).any(axis=1).sum())
    if key_mismatch:
        issues.append(f"主键逐行不匹配: {key_mismatch} 行")
        return issues

    for column in columns:
        left, right = raw_sorted[column], curated_sorted[column]
        try:
            pd.testing.assert_series_equal(
                left, right, check_dtype=False, check_names=False, check_exact=True,
            )
        except AssertionError:
            diff_mask = ~((left == right) | (left.isna() & right.isna()))
            sample_index = list(diff_mask[diff_mask].index[:3])
            samples = [(left.iloc[index], right.iloc[index]) for index in sample_index]
            issues.append(
                f"列 {column} 有 {int(diff_mask.sum())} 行不一致, 样例 raw/parquet={samples}"
            )
    return issues


def available_dates(data_root: Path) -> list[str]:
    """列出 Raw 中存在的交易日。"""
    raw_root = data_root / "raw"
    if not raw_root.exists():
        return []
    return sorted(
        directory.name.split("=", 1)[-1]
        for directory in raw_root.glob("trade_date=*")
        if directory.is_dir()
    )


def load_manifest(data_root: Path, trade_date: str) -> dict | None:
    """读取交易日的最新 manifest。"""
    path = data_root / "manifests" / "daily" / f"{trade_date}_latest.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="严格比对 Raw 与 Curated 数据一致性")
    parser.add_argument("trade_dates", nargs="*", help="交易日，默认复核全部可用日期")
    parser.add_argument("--data-dir", type=Path, default=None, help="数据目录")
    args = parser.parse_args()

    data_root = resolve_data_dir(args.data_dir)
    trade_dates = args.trade_dates or available_dates(data_root)
    if not trade_dates:
        logger.error("没有可复核的交易日: data_root=%s", data_root)
        return 1

    total_rows = 0
    total_issues = 0
    total_orphans = 0
    for trade_date in trade_dates:
        db_path = data_root / "raw" / f"trade_date={trade_date}" / "capture.sqlite3"
        if not db_path.exists():
            logger.error("Raw 库不存在，跳过: trade_date=%s path=%s", trade_date, db_path)
            total_issues += 1
            continue
        manifest = load_manifest(data_root, trade_date)
        if manifest is None:
            logger.error("没有 latest manifest，无法比对: trade_date=%s", trade_date)
            total_issues += 1
            continue
        connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        logger.info("===== 比对 trade_date=%s =====", trade_date)
        try:
            groups = connection.execute(
                "SELECT DISTINCT underlying, tier FROM captures ORDER BY underlying, tier"
            ).fetchall()
            for underlying, tier in groups:
                directory = partition_dir(data_root, trade_date, underlying, tier)
                paths = manifest_paths(manifest, directory)
                raw_df = load_raw(connection, underlying, tier)
                curated_df = load_curated(paths)
                issues = compare(raw_df, curated_df)
                # 孤儿文件：磁盘上存在但 manifest 未引用（重复压缩会遗留），不影响读取但占空间。
                listed = set(paths)
                orphans = sorted(set(directory.glob("*.parquet")) - listed) if directory.exists() else []
                total_orphans += len(orphans)
                total_rows += len(raw_df)
                total_issues += len(issues)
                logger.info(
                    "%s %-7s raw=%-8d parquet=%-8d %s",
                    underlying, tier, len(raw_df), len(curated_df),
                    "OK" if not issues else "FAIL",
                )
                for issue in issues:
                    logger.error("    %s", issue)
                if orphans:
                    logger.warning(
                        "    孤儿 parquet %d 个（manifest 未引用, 不影响读取但占磁盘）: %s",
                        len(orphans), [path.name for path in orphans[:3]],
                    )
                del raw_df, curated_df
                gc.collect()
        finally:
            connection.close()

    logger.info(
        "===== 合计 %d 行, 数据不一致 %d 项, 孤儿文件 %d 个 =====",
        total_rows, total_issues, total_orphans,
    )
    return 1 if (total_issues or total_orphans) else 0


if __name__ == "__main__":
    raise SystemExit(main())
