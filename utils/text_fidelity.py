"""Text-preserving readers for business data files.

Business identifiers are lexical values, not numbers. These helpers disable
pandas' default NA and dtype inference so blank strings and a literal ``NA``
remain distinguishable and values such as leading-zero or long identifiers are
not reformatted on a read/write round trip.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


_TEXT_READ_OPTIONS: dict[str, Any] = {
    "dtype": str,
    "keep_default_na": False,
    "na_filter": False,
}
DEFAULT_IDENTIFIER_MAX_LENGTH = 32


def read_business_csv(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    """Read a business CSV without numeric inference or NA conversion."""
    options = {**kwargs, **_TEXT_READ_OPTIONS}
    return pd.read_csv(path, **options)


def read_business_excel(
    path: str | Path, *, sheet_name: str | int = 0, **kwargs: Any
) -> pd.DataFrame:
    """Read a business Excel sheet as text, preserving blanks and literal NA."""
    options = {**kwargs, **_TEXT_READ_OPTIONS}
    return pd.read_excel(path, sheet_name=sheet_name, **options)


def read_business_excel_file(path: str | Path, **kwargs: Any) -> pd.ExcelFile:
    """Open an Excel workbook for iterating sheets."""
    return pd.ExcelFile(path, **kwargs)
