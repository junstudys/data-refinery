from pathlib import Path

import pytest

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

    remove_failed_output_folders(results_folder, ["a.xlsx"], workspace_root=tmp_path)
    assert not target.exists()
    assert not stray.exists()


def test_remove_failed_output_folders_rejects_malicious_names_atomically(tmp_path: Path):
    results_folder = tmp_path / "results"
    results_folder.mkdir()
    target = results_folder / "safe"
    target.mkdir()
    marker = target / "marker"
    marker.write_text("keep", encoding="utf-8")

    with pytest.raises(ValueError):
        remove_failed_output_folders(
            results_folder,
            ["safe.xlsx", "../outside.xlsx"],
            workspace_root=tmp_path,
        )

    assert marker.exists()


@pytest.mark.parametrize("filename", ["safe.xls", "safe", "safe/a.xlsx", "*.xlsx", "../safe.xlsx"])
def test_remove_failed_output_folders_rejects_invalid_names(
    tmp_path: Path, filename: str
):
    results_folder = tmp_path / "results"
    results_folder.mkdir()
    with pytest.raises(ValueError):
        remove_failed_output_folders(results_folder, [filename], workspace_root=tmp_path)


def test_remove_failed_output_folders_does_not_follow_symlink(tmp_path: Path):
    results_folder = tmp_path / "results"
    results_folder.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    link = results_folder / "safe"
    link.symlink_to(outside, target_is_directory=True)

    remove_failed_output_folders(results_folder, ["safe.xlsx"], workspace_root=tmp_path)

    assert link.is_symlink()
    assert outside.exists()


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
