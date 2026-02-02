from datetime import datetime
from typing import Iterable, List, Tuple

import pandas as pd


def _convert_excel_date(serial_series: pd.Series) -> pd.Series:
    excel_epoch = datetime(1899, 12, 30)
    days = pd.to_numeric(serial_series, errors="coerce")
    converted = excel_epoch + pd.to_timedelta(days, unit="D")
    return converted


def clean_date_vectorized_v2(
    date_series: pd.Series, date_formats: Iterable[Tuple[str, str]]
) -> pd.Series:
    result = pd.Series(index=date_series.index, dtype="datetime64[ns]")
    remaining_mask = pd.Series(True, index=date_series.index)

    cleaned = date_series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    auto_parsed = pd.to_datetime(cleaned, errors="coerce")
    mask = auto_parsed.notna()
    result[mask] = auto_parsed[mask]
    remaining_mask &= ~mask

    for fmt, pattern in date_formats:
        if not remaining_mask.any():
            break
        pattern_mask = cleaned.str.match(pattern, na=False) & remaining_mask
        if pattern_mask.any():
            parsed = pd.to_datetime(cleaned[pattern_mask], format=fmt, errors="coerce")
            result[pattern_mask] = parsed
            remaining_mask &= ~pattern_mask

    excel_mask = cleaned.str.match(r"^\d{1,5}$", na=False) & remaining_mask
    if excel_mask.any():
        result[excel_mask] = _convert_excel_date(cleaned[excel_mask])

    return result.dt.strftime("%Y-%m-%d %H:%M:%S")


DEFAULT_DATE_FORMATS: List[Tuple[str, str]] = [
    ("%Y%m%d", r"^\d{8}$"),
    ("%Y.%m.%d", r"^\d{4}\.\d{2}\.\d{2}$"),
    ("%Y年%m月%d日", r"^\d{4}年\d{1,2}月\d{1,2}日$"),
    ("%Y年%m月%d号", r"^\d{4}年\d{1,2}月\d{1,2}号$"),
    ("%Y年%m月", r"^\d{4}年\d{1,2}月$"),
    ("%Y%m", r"^\d{6}$"),
    ("%Y-%m", r"^\d{4}-\d{1,2}$"),
]
