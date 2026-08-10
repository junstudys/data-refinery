from pathlib import Path

import pandas as pd

from processors.field_cleaner import (
    clean_order_vectorized,
    clean_csv_files,
    resolve_order_column,
)


def test_clean_order_vectorized_filters_invalid_rows():
    df = pd.DataFrame(
        {
            "运单号": ["123456789012", "abc", "123456789012345678", "含中文123"],
        }
    )

    cleaned = clean_order_vectorized(df, "运单号", min_length=12, max_length=18)

    assert cleaned["运单号"].tolist() == ["123456789012", "123456789012345678"]


def test_resolve_order_column_aliases():
    df = pd.DataFrame({"单号": ["1"]})
    resolved = resolve_order_column(df, "运单号", ["单号"])
    assert resolved == "单号"


def test_clean_order_only_strips_integer_like_decimal_suffix():
    df = pd.DataFrame(
        {
            "运单号": ["3511111100002011122.0", "A.0"],
            "备注": ["keep", "NA"],
        }
    )
    cleaned = clean_order_vectorized(df, "运单号", min_length=1, max_length=32, remove_chinese=False)
    assert cleaned["运单号"].tolist() == ["3511111100002011122", "A.0"]
    assert cleaned["备注"].tolist() == ["keep", "NA"]


def test_clean_csv_files_add_column_mode(tmp_path: Path):
    file_path = tmp_path / "merge.csv"
    pd.DataFrame({"单号": ["123456789012", "abc"]}).to_csv(file_path, index=False)

    config = {
        "output_mode": "add_column",
        "fields": [
            {
                "name": "运单号",
                "aliases": ["单号"],
                "min_length": 12,
                "max_length": 18,
                "allow_chinese": False,
                "allowed_pattern": "^[A-Za-z0-9]+$",
            }
        ],
    }

    clean_csv_files(str(file_path), config)

    cleaned = pd.read_csv(tmp_path / "merge_cleaned.csv")
    assert "运单号_cleaned" in cleaned.columns
    assert cleaned["运单号_cleaned"].iloc[0] == "123456789012"
