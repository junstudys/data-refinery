import sys
from pathlib import Path

import yaml

import cli


def _write_config(path: Path, result_dir: Path, enabled: bool = True) -> None:
    config = {
        "paths": {
            "result_files": str(result_dir),
        },
        "date_cleaning": {
            "enabled": enabled,
            "config_file": "config/date_formats.yaml",
        },
    }
    path.write_text(yaml.dump(config, allow_unicode=True), encoding="utf-8")


def test_cli_prefers_merge_cleaned(monkeypatch, tmp_path, capsys):
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
    monkeypatch.setattr(
        sys, "argv", ["cli.py", "--config", str(config_path), "date-clean"]
    )

    cli.run()

    assert called[0][0] == merge_cleaned
    assert called[0][1] == merge_cleaned
    assert "日期清洗完成" in capsys.readouterr().out


def test_cli_uses_merge_when_no_merge_cleaned(monkeypatch, tmp_path, capsys):
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
    monkeypatch.setattr(
        sys, "argv", ["cli.py", "--config", str(config_path), "date-clean"]
    )

    cli.run()

    assert called[0][0] == merge
    assert called[0][1] == result_dir / "merge_cleaned.csv"
    assert "日期清洗完成" in capsys.readouterr().out


def test_cli_reports_missing_files(monkeypatch, tmp_path, capsys):
    result_dir = tmp_path / "results"
    result_dir.mkdir()

    config_path = tmp_path / "pipeline.yaml"
    _write_config(config_path, result_dir)

    monkeypatch.setattr(
        sys, "argv", ["cli.py", "--config", str(config_path), "date-clean"]
    )

    cli.run()

    assert "未找到 merge.csv 或 merge_cleaned.csv" in capsys.readouterr().out


def test_cli_columns_override_passed(monkeypatch, tmp_path):
    result_dir = tmp_path / "results"
    result_dir.mkdir()
    merge = result_dir / "merge.csv"
    merge.write_text("col\n2\n", encoding="utf-8")

    config_path = tmp_path / "pipeline.yaml"
    _write_config(config_path, result_dir)

    called = []

    def spy_process(self, input_path, output_path, columns=None):
        called.append(columns)
        return True

    monkeypatch.setattr(
        "processors.date_cleaner_processor.DateCleaningProcessor.process_csv_file",
        spy_process,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cli.py",
            "--config",
            str(config_path),
            "date-clean",
            "--columns",
            "创建时间,结算日期",
        ],
    )

    cli.run()

    assert called[0] == ["创建时间", "结算日期"]


def test_cli_skips_when_disabled(monkeypatch, tmp_path, capsys):
    result_dir = tmp_path / "results"
    result_dir.mkdir()

    config_path = tmp_path / "pipeline.yaml"
    _write_config(config_path, result_dir, enabled=False)

    monkeypatch.setattr(
        sys, "argv", ["cli.py", "--config", str(config_path), "date-clean"]
    )

    cli.run()

    assert "日期清洗已禁用" in capsys.readouterr().out
