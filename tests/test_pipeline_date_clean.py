from pathlib import Path

import yaml

from core.pipeline import DataPipeline


def _write_config(path: Path, result_dir: Path, enabled: bool = True) -> None:
    config = {
        "paths": {
            "result_files": str(result_dir),
        },
        "date_cleaning": {
            "enabled": enabled,
            "config_file": "config/date_formats.yaml",
            "columns": ["创建时间"],
        },
        "logging": {
            "level": "INFO",
        },
    }
    path.write_text(yaml.dump(config, allow_unicode=True), encoding="utf-8")


def test_pipeline_prefers_merge_cleaned(monkeypatch, tmp_path):
    result_dir = tmp_path / "results"
    result_dir.mkdir()
    merge_cleaned = result_dir / "merge_cleaned.csv"
    merge_cleaned.write_text("col\n1\n", encoding="utf-8")
    merge = result_dir / "merge.csv"
    merge.write_text("col\n2\n", encoding="utf-8")

    config_path = tmp_path / "pipeline.yaml"
    _write_config(config_path, result_dir)

    called = []

    def spy_process(self, input_path, output_path, columns=None):
        called.append((Path(input_path), Path(output_path), columns))
        return True

    monkeypatch.setattr(
        "processors.date_cleaner_processor.DateCleaningProcessor.process_csv_file",
        spy_process,
    )

    pipeline = DataPipeline(config_path=str(config_path))
    pipeline._step_date_clean()

    assert called[0][0] == merge_cleaned
    assert called[0][1] == merge_cleaned
    assert called[0][2] == ["创建时间"]


def test_pipeline_uses_merge_when_no_merge_cleaned(monkeypatch, tmp_path):
    result_dir = tmp_path / "results"
    result_dir.mkdir()
    merge = result_dir / "merge.csv"
    merge.write_text("col\n2\n", encoding="utf-8")

    config_path = tmp_path / "pipeline.yaml"
    _write_config(config_path, result_dir)

    called = []

    def spy_process(self, input_path, output_path, columns=None):
        called.append((Path(input_path), Path(output_path), columns))
        return True

    monkeypatch.setattr(
        "processors.date_cleaner_processor.DateCleaningProcessor.process_csv_file",
        spy_process,
    )

    pipeline = DataPipeline(config_path=str(config_path))
    pipeline._step_date_clean()

    assert called[0][0] == merge
    assert called[0][1] == result_dir / "merge_cleaned.csv"
    assert called[0][2] == ["创建时间"]


def test_pipeline_skips_when_disabled(monkeypatch, tmp_path):
    result_dir = tmp_path / "results"
    result_dir.mkdir()

    config_path = tmp_path / "pipeline.yaml"
    _write_config(config_path, result_dir, enabled=False)

    called = []

    def spy_process(self, input_path, output_path, columns=None):
        called.append(True)
        return True

    monkeypatch.setattr(
        "processors.date_cleaner_processor.DateCleaningProcessor.process_csv_file",
        spy_process,
    )

    pipeline = DataPipeline(config_path=str(config_path))
    pipeline._step_date_clean()

    assert called == []
