from pathlib import Path

import pandas as pd
import pytest

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
        workspace_root=tmp_path,
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
        workspace_root=tmp_path,
    )

    df = pd.read_csv(output_dir / "merge.csv")
    assert df[" 运单号 "].tolist() == ["A1"]


def test_extract_content_preserves_business_text_and_raw_csv(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    source = input_dir / "business.csv"
    source.write_text(
        "运单号,代码,备注\n"
        "3511111100002011122,00123,A.0\n"
        ",NA,中文\n",
        encoding="utf-8",
    )

    extract_content(
        folder_path=str(input_dir),
        result_path=str(output_dir),
        columns=["运单号", "代码", "备注"],
        merge=True,
        workspace_root=tmp_path,
    )

    raw = (output_dir / "merge.csv").read_text(encoding="utf-8")
    assert "3511111100002011122" in raw
    assert "00123" in raw
    assert "A.0" in raw
    assert ",NA,中文" in raw
    actual = pd.read_csv(output_dir / "merge.csv", dtype=str, keep_default_na=False)
    assert actual.iloc[0].tolist() == ["3511111100002011122", "00123", "A.0", "business.csv"]
    assert actual.iloc[1].tolist() == ["", "NA", "中文", "business.csv"]


def test_extract_content_preserves_excel_text_cells(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    with pd.ExcelWriter(input_dir / "business.xlsx", engine="openpyxl") as writer:
        pd.DataFrame(
            {"运单号": ["3511111100002011122", "00123", "", "NA"]}
        ).to_excel(writer, index=False)

    extract_content(
        folder_path=str(input_dir),
        result_path=str(output_dir),
        columns=["运单号"],
        merge=True,
        workspace_root=tmp_path,
    )

    actual = pd.read_csv(output_dir / "merge.csv", dtype=str, keep_default_na=False)
    assert actual["运单号"].tolist() == ["3511111100002011122", "00123", "", "NA"]


def test_extract_content_rejects_workspace_root_output(tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    marker = tmp_path / "keep.txt"
    marker.write_text("keep", encoding="utf-8")

    with pytest.raises(ValueError):
        extract_content(
            folder_path=str(input_dir),
            result_path=str(tmp_path),
            columns=["运单号"],
            merge=True,
            workspace_root=tmp_path,
        )

    assert marker.exists()


def test_extract_content_rejects_input_inside_output(tmp_path: Path):
    output_dir = tmp_path / "output"
    input_dir = output_dir / "input"
    input_dir.mkdir(parents=True)
    marker = input_dir / "data.csv"
    marker.write_text("运单号\nA1\n", encoding="utf-8")

    with pytest.raises(ValueError):
        extract_content(
            folder_path=str(input_dir),
            result_path=str(output_dir),
            columns=["运单号"],
            merge=True,
            workspace_root=tmp_path,
        )

    assert marker.exists()


def test_pipeline_input_split_supports_chinese_comma():
    import re

    raw_input = "单号，客户名称"
    columns = [c for c in re.split(r"[,，;；]", raw_input) if c]
    assert columns == ["单号", "客户名称"]
