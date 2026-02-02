from pathlib import Path

import pandas as pd

from processors.field_replacer import replace_fields


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
    )

    assert (result_folder / "keep.txt").exists()
