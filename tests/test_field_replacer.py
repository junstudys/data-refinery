from pathlib import Path

import pandas as pd
import pytest

from processors.field_replacer import replace_fields


def test_replace_fields_preserves_csv_values(tmp_path: Path):
    dict_path = tmp_path / "dict.xlsx"
    with pd.ExcelWriter(dict_path) as writer:
        pd.DataFrame(
            {"old_field": ["旧列"], "new_field": ["新列"], "priority": [1]}
        ).to_excel(writer, sheet_name="dict", index=False)
    orig_folder = tmp_path / "orig"
    result_folder = tmp_path / "result"
    orig_folder.mkdir()
    source = orig_folder / "sample.csv"
    source.write_text(
        "旧列,备注,科学\n"
        "3511111100002011122,00123,1E+18\n"
        ",NA,A.0\n", encoding="utf-8"
    )

    replace_fields(
        dict_path=str(dict_path),
        orig_folder=str(orig_folder),
        result_folder=str(result_folder),
        workspace_root=tmp_path,
    )

    raw = (result_folder / "sample.csv").read_text(encoding="utf-8")
    assert "3511111100002011122,00123,1E+18" in raw
    assert ",NA,A.0" in raw
    actual = pd.read_csv(result_folder / "sample.csv", dtype=str, keep_default_na=False)
    assert list(actual.columns) == ["新列", "备注", "科学"]
    assert actual.values.tolist() == [
        ["3511111100002011122", "00123", "1E+18"],
        ["", "NA", "A.0"],
    ]


def test_replace_fields_does_not_clear_output_by_default(tmp_path: Path):
    dict_path = tmp_path / "dict.xlsx"
    data = pd.DataFrame(
        {
            "old_field": ["旧列"],
            "new_field": ["新列"],
            "priority": [1],
        }
    )
    with pd.ExcelWriter(dict_path) as writer:
        data.to_excel(writer, sheet_name="dict", index=False)

    orig_folder = tmp_path / "orig"
    result_folder = tmp_path / "result"
    orig_folder.mkdir()
    result_folder.mkdir()
    (result_folder / "keep.txt").write_text("keep", encoding="utf-8")

    sample_csv = orig_folder / "sample.csv"
    pd.DataFrame({"旧列": ["1"]}).to_csv(sample_csv, index=False)

    replace_fields(
        dict_path=str(dict_path),
        sheet_name="dict",
        orig_folder=str(orig_folder),
        result_folder=str(result_folder),
        workspace_root=tmp_path,
    )

    assert (result_folder / "keep.txt").exists()


def test_replace_fields_rejects_unsafe_clear_target(tmp_path: Path):
    dict_path = tmp_path / "dict.xlsx"
    pd.DataFrame(
        {"old_field": ["旧列"], "new_field": ["新列"], "priority": [1]}
    ).to_excel(dict_path, sheet_name="dict", index=False)
    orig_folder = tmp_path / "orig"
    orig_folder.mkdir()
    (orig_folder / "sample.csv").write_text("旧列\n1\n", encoding="utf-8")
    marker = tmp_path / "keep.txt"
    marker.write_text("keep", encoding="utf-8")

    with pytest.raises(ValueError):
        replace_fields(
            dict_path=str(dict_path),
            orig_folder=str(orig_folder),
            result_folder=str(tmp_path),
            clear_output=True,
            workspace_root=tmp_path,
        )

    assert marker.exists()
