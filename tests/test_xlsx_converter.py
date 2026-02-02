from pathlib import Path

from utils.path_manager import remove_failed_output_folders
from processors.xlsx_converter import (
    _write_failed_list,
    _read_failed_list,
    _contains_error_markers_in_file,
)


def test_failed_list_roundtrip(tmp_path: Path):
    file_path = tmp_path / "failed.csv"
    failures = [{"filename": "a.xlsx", "error": "FAILED: boom", "timestamp": "t"}]

    _write_failed_list(file_path, failures)
    results = _read_failed_list(file_path)

    assert results == ["a.xlsx"]


def test_remove_failed_output_folders(tmp_path: Path):
    results_folder = tmp_path / "results"
    results_folder.mkdir()
    target = results_folder / "a"
    target.mkdir()
    (target / "file.csv").write_text("x", encoding="utf-8")
    stray = results_folder / "a_sheet1.csv"
    stray.write_text("x", encoding="utf-8")

    remove_failed_output_folders(results_folder, ["a.xlsx"])
    assert not target.exists()
    assert not stray.exists()


def test_failure_markers_trigger_failure(tmp_path: Path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("col\n#NAME?\n", encoding="utf-8")
    marker = _contains_error_markers_in_file(csv_path, ["#NAME?"], ["col"])
    assert marker == "#NAME?"


def test_failure_markers_can_be_disabled(tmp_path: Path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("col\n#NAME?\n", encoding="utf-8")
    marker = _contains_error_markers_in_file(csv_path, [], ["col"])
    assert marker == ""
