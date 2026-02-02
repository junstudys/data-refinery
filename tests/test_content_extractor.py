from pathlib import Path

import pandas as pd

from processors.content_extractor import extract_content


def test_extract_content_empty_folder_creates_merge(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    extract_content(
        folder_path=str(input_dir),
        result_path=str(output_dir),
        columns=["运单号"],
        merge=True,
    )

    merge_file = output_dir / "merge.csv"
    assert merge_file.exists()

    df = pd.read_csv(merge_file)
    assert list(df.columns) == ["运单号", "source"]
    assert df.empty


def test_extract_content_matches_columns_case_insensitive(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    pd.DataFrame({"运单号": ["A1"]}).to_csv(input_dir / "data.csv", index=False)

    extract_content(
        folder_path=str(input_dir),
        result_path=str(output_dir),
        columns=[" 运单号 "],
        merge=True,
    )

    df = pd.read_csv(output_dir / "merge.csv")
    assert df[" 运单号 "].tolist() == ["A1"]


def test_pipeline_input_split_supports_chinese_comma():
    import re

    raw_input = "单号，客户名称"
    columns = [c for c in re.split(r"[,，;；]", raw_input) if c]
    assert columns == ["单号", "客户名称"]
