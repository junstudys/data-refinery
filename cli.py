"""Unified CLI entrypoint."""

import argparse
import re
from pathlib import Path

import yaml

from core.pipeline import DataPipeline
from processors.content_extractor import extract_content
from processors.field_aggregator import array_agg_optimized
from processors.field_cleaner import clean_csv_files
from processors.field_extractor import extract_fields
from processors.field_replacer import replace_fields
from processors.file_flattener import extract_files
from processors.header_detector import HeaderConfig, batch_process
from processors.excel_preprocessor import PreprocessConfig, preprocess_excel_files
from processors.xlsx_converter import xlsx_to_csv_parallel


def _load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _split_columns(raw: str) -> list[str]:
    return [c.strip() for c in re.split(r"[,，;；]", raw) if c.strip()]


def run() -> None:
    parser = argparse.ArgumentParser(description="DataRefinery CLI")
    parser.add_argument("--config", default="config/pipeline.yaml", help="配置文件路径")

    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("pipeline", help="运行完整流水线")

    xlsx_parser = subparsers.add_parser("xlsx-to-csv", help="Excel 转 CSV")
    xlsx_parser.add_argument(
        "--mode",
        choices=["all", "failed"],
        default=None,
        help="转换模式：all=全量, failed=只重跑失败",
    )
    xlsx_parser.add_argument(
        "--preprocess",
        action="store_true",
        help="启用 Excel 预处理（公式文本/错误值审计）",
    )
    subparsers.add_parser("flatten", help="平铺 CSV 结果目录")
    subparsers.add_parser("find-header", help="表头识别")
    subparsers.add_parser("extract-fields", help="字段提取")
    subparsers.add_parser("array-agg", help="字段聚合")
    subparsers.add_parser("field-replace", help="字段替换")

    extract_parser = subparsers.add_parser("extract-content", help="内容提取")
    extract_parser.add_argument("--columns", help="字段列表，支持中英文逗号分隔")
    extract_parser.add_argument("--merge", action="store_true", help="合并输出")

    subparsers.add_parser("field-clean", help="字段清洗")

    args = parser.parse_args()
    config = _load_config(args.config)
    paths = config.get("paths", {})

    command = args.command or "pipeline"

    if command == "pipeline":
        DataPipeline(config_path=args.config).run()
        return

    if command == "xlsx-to-csv":
        excel_folder = Path(paths.get("excel_folder", "excel_folder"))
        preprocess_folder = Path(
            paths.get("excel_preprocess_folder", "excel_preprocessed")
        )
        results_folder = Path(paths.get("csv_results_folder", "csv_results_folder"))
        max_workers = config.get("performance", {}).get("max_workers", 4)
        xlsx_cfg = config.get("xlsx_to_csv", {})
        retry_mode = args.mode or xlsx_cfg.get("retry_mode", "all")
        preprocess_cfg = config.get("excel_preprocess", {})

        if args.preprocess or preprocess_cfg.get("enabled", False):
            preprocess_excel_files(
                excel_folder.glob("*.xlsx"),
                preprocess_folder,
                PreprocessConfig(
                    enabled=True,
                    in_place=bool(preprocess_cfg.get("in_place", False)),
                    audit_log_path=preprocess_cfg.get(
                        "audit_log_path", "logs/excel_error_audit.csv"
                    ),
                    error_mode=preprocess_cfg.get("error_handling", {}).get(
                        "mode", "keep"
                    ),
                    preserve_formula_text=preprocess_cfg.get("formula_text", {}).get(
                        "preserve", True
                    ),
                    preserve_if_contains_chinese=preprocess_cfg.get(
                        "formula_text", {}
                    ).get("preserve_if_contains_chinese", True),
                    preserve_if_no_parentheses=preprocess_cfg.get(
                        "formula_text", {}
                    ).get("preserve_if_no_parentheses", True),
                ),
            )
            excel_folder = preprocess_folder
        xlsx_to_csv_parallel(
            excel_folder,
            results_folder,
            max_workers=max_workers,
            retry_mode=retry_mode,
            failed_list_path=Path(
                xlsx_cfg.get("failed_list_path", "mid_files/failed_xlsx.csv")
            ),
            log_detail=bool(xlsx_cfg.get("log_detail", False)),
            failure_markers=xlsx_cfg.get("failure_markers", []),
            failure_scan_columns=xlsx_cfg.get("failure_scan_columns", []),
        )
        return

    if command == "flatten":
        extract_files(paths.get("csv_results_folder", "csv_results_folder"))
        return

    if command == "find-header":
        field_cfg = config.get("field_detection", {})
        encoding_cfg = config.get("encoding", {})
        header_cfg = HeaderConfig(
            max_header_search_rows=field_cfg.get("max_rows_to_check", 4),
            tracking_keywords=field_cfg.get("keywords", []),
            encoding_fallback_list=encoding_cfg.get("fallback", ["utf-8"]),
            min_header_columns=field_cfg.get("min_header_columns", 2),
            max_standalone_keyword_length=field_cfg.get(
                "max_standalone_keyword_length", 20
            ),
            default_first_row_when_not_found=field_cfg.get(
                "default_first_row_when_not_found", False
            ),
        )
        batch_process(
            paths.get("csv_results_folder", "csv_results_folder"),
            paths.get("tmp_find_header_row", "mid_files/tmp_find_header_row"),
            header_cfg,
        )
        return

    if command == "extract-fields":
        extract_fields(
            paths.get("tmp_find_header_row", "mid_files/tmp_find_header_row"),
            str(Path(paths.get("mid_files", "mid_files")) / "field_info.csv"),
        )
        return

    if command == "array-agg":
        input_file = str(Path(paths.get("mid_files", "mid_files")) / "field_info.csv")
        output_file = str(Path(paths.get("mid_files", "mid_files")) / "agg.csv")
        array_agg_optimized(input_file, "Field Names", "File Name", output_file)
        return

    if command == "field-replace":
        replace_fields(
            dict_path="config/dict_zh.xlsx",
            sheet_name="dict",
            orig_folder=paths.get(
                "tmp_find_header_row", "mid_files/tmp_find_header_row"
            ),
            result_folder=paths.get("tmp_field_replace", "mid_files/tmp_field_replace"),
        )
        return

    if command == "extract-content":
        content_cfg = config.get("content_extraction", {})
        columns_cfg = content_cfg.get("columns", [])
        merge_flag = bool(content_cfg.get("merge", True))

        if args.columns:
            columns_cfg = _split_columns(args.columns)
        elif not columns_cfg:
            raw = input("请输入需要提取的字段名，多个字段使用逗号分隔：")
            columns_cfg = _split_columns(raw)
            merge_flag = input(
                "是否要将所有文件合并成一个 CSV 文件？(是/1 或 否/0): "
            ).lower() in ["是", "1"]
        if args.merge:
            merge_flag = True

        extract_content(
            folder_path=paths.get("tmp_field_replace", "mid_files/tmp_field_replace"),
            result_path=paths.get("result_files", "Result_files"),
            columns=[c.strip() for c in columns_cfg if c.strip()],
            merge=merge_flag,
            clear_output=bool(config.get("dir_policies", {}).get("result_files", True)),
        )
        return

    if command == "field-clean":
        order_cfg = config.get("order_clean", {})
        clean_csv_files(
            path=str(Path(paths.get("result_files", "Result_files")) / "merge.csv"),
            config=order_cfg,
        )
        return

    parser.print_help()


if __name__ == "__main__":
    run()
