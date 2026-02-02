from pathlib import Path

import yaml

from core.pipeline import DataPipeline


def test_pipeline_reads_content_extraction_config(tmp_path: Path):
    config = {
        "pipeline": {
            "manual_continue_after_repair": True,
            "manual_continue_after_convert": True,
            "manual_continue_after_dict_edit": True,
        },
        "paths": {
            "excel_folder": "excel_folder",
            "csv_results_folder": "csv_results_folder",
            "mid_files": "mid_files",
            "result_files": "Result_files",
            "tmp_find_header_row": "mid_files/tmp_find_header_row",
            "tmp_field_replace": "mid_files/tmp_field_replace",
        },
        "field_detection": {
            "keywords": ["运单号"],
            "max_rows_to_check": 4,
            "min_header_columns": 2,
            "max_standalone_keyword_length": 20,
            "default_first_row_when_not_found": False,
        },
        "encoding": {"fallback": ["utf-8"]},
        "performance": {"max_workers": 1},
        "logging": {"level": "INFO"},
        "content_extraction": {"columns": ["运单号"], "merge": True},
        "dir_policies": {"result_files": False},
    }

    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(yaml.safe_dump(config, allow_unicode=True), encoding="utf-8")

    pipeline = DataPipeline(config_path=str(config_path))
    assert pipeline.config["content_extraction"]["columns"] == ["运单号"]
