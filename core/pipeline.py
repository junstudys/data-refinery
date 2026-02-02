from dataclasses import dataclass
import re
from pathlib import Path
from typing import Callable, List, Optional

import yaml

from processors.content_extractor import extract_content
from processors.field_aggregator import array_agg_optimized
from processors.field_extractor import extract_fields
from processors.field_replacer import replace_fields
from processors.file_flattener import extract_files
from processors.header_detector import HeaderConfig, batch_process
from processors.field_cleaner import clean_csv_files
from processors.xlsx_converter import read_failed_list, xlsx_to_csv_parallel
from processors.excel_preprocessor import PreprocessConfig, preprocess_excel_files
from utils.logger import setup_logger
from utils.path_manager import apply_dir_policies, remove_failed_output_folders


@dataclass
class PipelineStep:
    name: str
    func: Callable[[], None]
    description: str
    required: bool = True


class DataPipeline:
    def __init__(self, config_path: str = "config/pipeline.yaml"):
        self.config = self._load_config(config_path)
        logging_cfg = self.config.get("logging", {})
        self.logger = setup_logger(
            name="datarefinery",
            level=logging_cfg.get("level", "INFO"),
            log_file=logging_cfg.get("file"),
        )
        self.manual_continue_after_repair = bool(
            self.config.get("pipeline", {}).get("manual_continue_after_repair", False)
        )
        self.manual_continue_after_convert = bool(
            self.config.get("pipeline", {}).get("manual_continue_after_convert", False)
        )
        self.manual_continue_after_dict_edit = bool(
            self.config.get("pipeline", {}).get(
                "manual_continue_after_dict_edit", False
            )
        )
        self.stop_pipeline = False
        self.steps = self._init_steps()

    def _load_config(self, config_path: str) -> dict:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _init_steps(self) -> List[PipelineStep]:
        return [
            PipelineStep("xlsx_to_csv", self._step_xlsx_to_csv, "Excel转CSV"),
            PipelineStep("extract_nested", self._step_extract_nested, "文件平铺"),
            PipelineStep("find_header", self._step_find_header, "表头识别"),
            PipelineStep("extract_field_info", self._step_extract_info, "字段提取"),
            PipelineStep("array_agg", self._step_array_agg, "字段聚合"),
            PipelineStep(
                "field_replace", self._step_field_replace, "字段替换", required=False
            ),
            PipelineStep("extract_content", self._step_extract_content, "内容提取"),
            PipelineStep("order_clean", self._step_order_clean, "运单清洗"),
        ]

    def run(self, from_step: int = 0, to_step: Optional[int] = None) -> None:
        paths = self.config.get("paths", {})
        dir_policies = self.config.get("dir_policies", {})
        xlsx_cfg = self.config.get("xlsx_to_csv", {})
        retry_mode = xlsx_cfg.get("retry_mode", "all")
        failed_list_path = Path(
            xlsx_cfg.get("failed_list_path", "mid_files/failed_xlsx.csv")
        )
        allow_flatten_on_success_only = bool(
            xlsx_cfg.get("allow_flatten_on_success_only", True)
        )

        if retry_mode == "failed":
            dir_policies = {**dir_policies}
            dir_policies["csv_results_folder"] = False
            dir_policies["excel_preprocess_folder"] = False
            dir_policies["mid_files"] = False
            dir_policies["tmp_find_header_row"] = False
            dir_policies["tmp_field_replace"] = False
            dir_policies["result_files"] = False
        if paths:
            apply_dir_policies(paths, dir_policies)
        end = to_step or len(self.steps)
        for i, step in enumerate(self.steps[from_step:end], start=from_step):
            if self.stop_pipeline:
                self.logger.warning("流程已停止，跳过后续步骤")
                break
            if (
                step.name != "xlsx_to_csv"
                and allow_flatten_on_success_only
                and failed_list_path.exists()
            ):
                with open(failed_list_path, "r", encoding="utf-8") as f:
                    if len(f.readlines()) > 1:
                        self.logger.warning("存在转换失败文件，跳过后续所有步骤")
                        self.stop_pipeline = True
                        break
            self.logger.info(f"[{i + 1}/{len(self.steps)}] {step.description}...")
            try:
                step.func()
                self.logger.info(f"✓ {step.name} 完成")
            except Exception as exc:
                self.logger.error(f"✗ {step.name} 失败: {exc}")
                if step.required:
                    raise

    def _step_xlsx_to_csv(self) -> None:
        paths = self.config.get("paths", {})
        perf = self.config.get("performance", {})
        xlsx_cfg = self.config.get("xlsx_to_csv", {})
        excel_folder = Path(paths.get("excel_folder", "excel_folder"))
        preprocess_folder = Path(
            paths.get("excel_preprocess_folder", "excel_preprocessed")
        )
        results_folder = Path(paths.get("csv_results_folder", "csv_results_folder"))
        max_workers = perf.get("max_workers", 4)
        preprocess_cfg = self.config.get("excel_preprocess", {})
        cleanup_cfg = self.config.get("cleanup_on_failed", {})
        failed_list_path = Path(
            xlsx_cfg.get("failed_list_path", "mid_files/failed_xlsx.csv")
        )
        retry_mode = xlsx_cfg.get("retry_mode", "all")

        if retry_mode == "all":
            xlsx_to_csv_parallel(
                excel_folder,
                results_folder,
                max_workers=max_workers,
                retry_mode=retry_mode,
                failed_list_path=failed_list_path,
                log_detail=bool(xlsx_cfg.get("log_detail", False)),
                failure_markers=xlsx_cfg.get("failure_markers", []),
                failure_scan_columns=xlsx_cfg.get("failure_scan_columns", []),
            )
            failed_files = read_failed_list(failed_list_path)
            if failed_files:
                self.logger.warning(f"失败文件数: {len(failed_files)}")
                self.logger.warning("失败文件列表: " + ", ".join(failed_files))
                if cleanup_cfg.get("enabled", True) and cleanup_cfg.get(
                    "remove_failed_output_folders", True
                ):
                    remove_failed_output_folders(results_folder, failed_files)
                if preprocess_cfg.get("fallback_on_failed", False):
                    preprocess_excel_files(
                        (excel_folder / f for f in failed_files),
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
                            preserve_formula_text=preprocess_cfg.get(
                                "formula_text", {}
                            ).get("preserve", True),
                            preserve_if_contains_chinese=preprocess_cfg.get(
                                "formula_text", {}
                            ).get("preserve_if_contains_chinese", True),
                            preserve_if_no_parentheses=preprocess_cfg.get(
                                "formula_text", {}
                            ).get("preserve_if_no_parentheses", False),
                        ),
                    )
                    xlsx_to_csv_parallel(
                        preprocess_folder,
                        results_folder,
                        max_workers=max_workers,
                        retry_mode="failed",
                        failed_list_path=failed_list_path,
                        log_detail=bool(xlsx_cfg.get("log_detail", False)),
                        failure_markers=[],
                        failure_scan_columns=[],
                    )
                    retry_failed = read_failed_list(failed_list_path)
                    if retry_failed:
                        self.logger.warning(f"补救后仍失败文件数: {len(retry_failed)}")
                        if cleanup_cfg.get("enabled", True) and cleanup_cfg.get(
                            "remove_failed_output_folders", True
                        ):
                            remove_failed_output_folders(results_folder, retry_failed)
                        self.stop_pipeline = True
                    else:
                        if self.manual_continue_after_repair:
                            user_input = (
                                input("补救已成功，是否继续执行后续步骤？(y/n): ")
                                .strip()
                                .lower()
                            )
                            if user_input not in ["y", "yes"]:
                                self.logger.warning("用户取消后续步骤")
                                self.stop_pipeline = True
                                return
                        if self.manual_continue_after_convert:
                            user_input = (
                                input("转换完成，是否继续执行后续步骤？(y/n): ")
                                .strip()
                                .lower()
                            )
                            if user_input not in ["y", "yes"]:
                                self.logger.warning("用户取消后续步骤")
                                self.stop_pipeline = True
                                return
            return

        if preprocess_cfg.get("enabled", False):
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
            failed_list_path=failed_list_path,
            log_detail=bool(xlsx_cfg.get("log_detail", False)),
            failure_markers=xlsx_cfg.get("failure_markers", []),
            failure_scan_columns=xlsx_cfg.get("failure_scan_columns", []),
        )

    def _step_extract_nested(self) -> None:
        paths = self.config.get("paths", {})
        extract_files(paths.get("csv_results_folder", "csv_results_folder"))

    def _step_find_header(self) -> None:
        paths = self.config.get("paths", {})
        field_cfg = self.config.get("field_detection", {})
        encoding_cfg = self.config.get("encoding", {})
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

    def _step_extract_info(self) -> None:
        paths = self.config.get("paths", {})
        extract_fields(
            paths.get("tmp_find_header_row", "mid_files/tmp_find_header_row"),
            str(Path(paths.get("mid_files", "mid_files")) / "field_info.csv"),
        )

    def _step_array_agg(self) -> None:
        paths = self.config.get("paths", {})
        input_file = str(Path(paths.get("mid_files", "mid_files")) / "field_info.csv")
        output_file = str(Path(paths.get("mid_files", "mid_files")) / "agg.csv")
        array_agg_optimized(input_file, "Field Names", "File Name", output_file)
        if self.manual_continue_after_dict_edit:
            user_input = (
                input("字段聚合完成，请编辑 dict_zh.xlsx 后继续 (y/n): ")
                .strip()
                .lower()
            )
            if user_input not in ["y", "yes"]:
                self.logger.warning("用户取消后续步骤")
                self.stop_pipeline = True

    def _step_field_replace(self) -> None:
        replace_fields(
            dict_path="config/dict_zh.xlsx",
            sheet_name="dict",
            orig_folder=self.config.get("paths", {}).get(
                "tmp_find_header_row", "mid_files/tmp_find_header_row"
            ),
            result_folder=self.config.get("paths", {}).get(
                "tmp_field_replace", "mid_files/tmp_field_replace"
            ),
        )

    def _step_extract_content(self) -> None:
        content_cfg = self.config.get("content_extraction", {})
        columns_cfg = content_cfg.get("columns", [])
        merge_flag = bool(content_cfg.get("merge", True))
        clear_output = bool(
            self.config.get("dir_policies", {}).get("result_files", True)
        )

        if not columns_cfg:
            raw_input = input("请输入需要提取的字段名，多个字段使用逗号分隔：")
            columns_cfg = re.split(r"[,，;；]", raw_input)
            merge = input(
                "是否要将所有文件合并成一个 CSV 文件？(是/1 或 否/0): "
            ).lower()
            merge_flag = merge in ["是", "1"]
        paths = self.config.get("paths", {})
        extract_content(
            folder_path=paths.get("tmp_field_replace", "mid_files/tmp_field_replace"),
            result_path=paths.get("result_files", "Result_files"),
            columns=[c.strip() for c in columns_cfg if c.strip()],
            merge=merge_flag,
            clear_output=clear_output,
        )

    def _step_order_clean(self) -> None:
        paths = self.config.get("paths", {})
        order_cfg = self.config.get("order_clean", {})
        clean_csv_files(
            path=str(Path(paths.get("result_files", "Result_files")) / "merge.csv"),
            config=order_cfg,
        )
