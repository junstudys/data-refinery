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
            if fmt is None:
                # Excel 序列日期特殊处理
                result[pattern_mask] = _convert_excel_date(cleaned[pattern_mask])
            else:
                parsed = pd.to_datetime(cleaned[pattern_mask], format=fmt, errors="coerce")
                result[pattern_mask] = parsed
            remaining_mask &= ~pattern_mask

    # 备用：如果没有通过配置匹配，尝试纯数字模式
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


def add_custom_format(format_tuple: Tuple[str, str]) -> None:
    """添加自定义日期格式到全局列表"""
    DEFAULT_DATE_FORMATS.append(format_tuple)


def get_supported_formats() -> List[str]:
    """获取支持的日期格式描述"""
    return [
        "Excel 序列日期",
        "斜杠分隔 (2020/1/1)",
        "紧凑格式 (20200123)",
        "点分隔 (2020.1.24)",
        "中文年月日 (2024年1月2日)",
        "中文年月号 (2024年1月2号)",
        "中文年月 (2023年2月)",
        "ISO 日期时间 (2023-01-02 23:30:31)",
        "紧凑年月 (202301)",
        "横杠年月 (2023-01)",
    ]
