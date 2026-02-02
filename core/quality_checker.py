from dataclasses import dataclass
from typing import Dict, List

import pandas as pd


@dataclass
class QualityReport:
    file_name: str
    total_rows: int
    null_counts: Dict[str, int]
    duplicate_rows: int
    invalid_order_numbers: int
    quality_score: float


class DataQualityChecker:
    def __init__(self, order_column: str = "运单号"):
        self.order_column = order_column

    def check(self, df: pd.DataFrame, file_name: str) -> QualityReport:
        total_rows = len(df)
        null_counts = df.isnull().sum().to_dict()
        duplicate_rows = df.duplicated().sum()

        if self.order_column in df.columns:
            order_series = df[self.order_column].astype(str)
            invalid = (
                (order_series.str.len() < 12)
                | (order_series.str.len() > 18)
                | order_series.str.contains(r"[\u4e00-\u9fff]", na=False)
            ).sum()
        else:
            invalid = 0

        completeness = (
            1 - sum(null_counts.values()) / (total_rows * len(df.columns))
            if total_rows
            else 1
        )
        uniqueness = 1 - duplicate_rows / total_rows if total_rows else 1
        validity = 1 - invalid / total_rows if total_rows else 1
        score = (completeness * 0.4 + uniqueness * 0.3 + validity * 0.3) * 100

        return QualityReport(
            file_name=file_name,
            total_rows=total_rows,
            null_counts=null_counts,
            duplicate_rows=duplicate_rows,
            invalid_order_numbers=invalid,
            quality_score=round(score, 2),
        )

    def generate_report(self, reports: List[QualityReport]) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "文件名": r.file_name,
                    "总行数": r.total_rows,
                    "重复行": r.duplicate_rows,
                    "无效运单号": r.invalid_order_numbers,
                    "质量分数": r.quality_score,
                }
                for r in reports
            ]
        )
