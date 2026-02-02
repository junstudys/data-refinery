import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


@dataclass
class HeaderConfig:
    max_header_search_rows: int = 4
    encoding_fallback_list: List[str] = field(
        default_factory=lambda: [
            "utf-8",
            "utf-8-sig",
            "gbk",
            "gb18030",
            "gb2312",
            "latin1",
            "iso-8859-1",
        ]
    )
    tracking_keywords: List[str] = field(
        default_factory=lambda: [
            "单号",
            "运单号",
            "运单编号",
            "快递单号",
            "工作单号",
            "mailno",
            "原订单快递单号",
            "物流单号",
            "物流编号",
            "ship_id",
        ]
    )
    min_header_columns: int = 2
    max_standalone_keyword_length: int = 20
    default_first_row_when_not_found: bool = False


class CSVHeaderDetector:
    def __init__(self, config: HeaderConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def find_header_row(self, rows: List[List[str]]) -> Optional[int]:
        if not rows:
            return None
        search_range = min(len(rows), self.config.max_header_search_rows)

        candidates = []
        for i in range(search_range):
            row = rows[i]
            non_empty_cols = sum(1 for cell in row if cell and str(cell).strip())
            if self._is_header_row(row):
                candidates.append(
                    {
                        "index": i,
                        "row": row,
                        "non_empty_cols": non_empty_cols,
                        "has_standalone_keyword": self._has_standalone_keyword(row),
                    }
                )

        if not candidates:
            return None

        best = self._select_best_candidate(candidates)
        return best["index"] if best else None

    def _select_best_candidate(self, candidates: List[Dict]) -> Optional[Dict]:
        best = None
        for candidate in candidates:
            if best is None:
                best = candidate
                continue

            if (
                candidate["has_standalone_keyword"]
                and not best["has_standalone_keyword"]
            ):
                best = candidate
                continue
            if (
                not candidate["has_standalone_keyword"]
                and best["has_standalone_keyword"]
            ):
                continue

            if best["non_empty_cols"] <= 1 and candidate["non_empty_cols"] > 1:
                best = candidate
                continue

            if (
                candidate["has_standalone_keyword"]
                and best["has_standalone_keyword"]
                and candidate["non_empty_cols"] > best["non_empty_cols"]
            ):
                best = candidate
                continue

        if best and best["non_empty_cols"] < self.config.min_header_columns:
            return None
        return best

    def _is_header_row(self, row: List[str]) -> bool:
        if not row:
            return False
        row_text = "".join(str(cell).lower() for cell in row if cell)
        for keyword in self.config.tracking_keywords:
            if keyword.lower() in row_text:
                return True
        return False

    def _has_standalone_keyword(self, row: List[str]) -> bool:
        if not row:
            return False
        for cell in row:
            if not cell:
                continue
            cell_text = str(cell).strip().lower()
            if not cell_text:
                continue
            if len(cell_text) > self.config.max_standalone_keyword_length:
                continue
            for keyword in self.config.tracking_keywords:
                keyword_lower = keyword.lower()
                if cell_text == keyword_lower:
                    return True
                if len(cell_text) <= len(keyword_lower) + 5:
                    if cell_text.endswith(keyword_lower) or cell_text.startswith(
                        keyword_lower
                    ):
                        return True
                if (
                    keyword_lower in cell_text
                    and len(keyword_lower) >= len(cell_text) * 0.5
                ):
                    return True
        return False


class CSVFileProcessor:
    def __init__(self, config: HeaderConfig):
        self.config = config
        self.detector = CSVHeaderDetector(config)

    def read_rows_with_fallback(self, file_path: Path) -> Tuple[List[List[str]], str]:
        errors = {}
        for encoding in self.config.encoding_fallback_list:
            try:
                with open(
                    file_path, "r", encoding=encoding, newline="", errors="strict"
                ) as f:
                    reader = csv.reader(f)
                    rows = [row for row in reader if any(cell.strip() for cell in row)]
                return rows, encoding
            except UnicodeDecodeError as exc:
                errors[encoding] = str(exc)
                continue
        raise UnicodeDecodeError(
            "multiple",
            b"",
            0,
            1,
            f"无法读取文件 {file_path}，尝试编码: {list(errors.keys())}",
        )

    def process_file(
        self, file_path: Path
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, object]]:
        metadata = {
            "file_name": file_path.name,
            "encoding": None,
            "header_row": None,
            "data_rows": 0,
            "status": "unknown",
            "message": "",
        }

        rows, encoding = self.read_rows_with_fallback(file_path)
        metadata["encoding"] = encoding
        if not rows:
            metadata["status"] = "empty"
            metadata["message"] = f"文件 {file_path.name} 为空"
            return None, metadata

        header_idx = self.detector.find_header_row(rows)
        if header_idx is None:
            if self.config.default_first_row_when_not_found:
                header_idx = 0
            else:
                metadata["status"] = "header_not_found"
                metadata["message"] = f"文件 {file_path.name} 未找到表头"
                return None, metadata

        metadata["header_row"] = header_idx + 1
        df = self._build_dataframe(rows, header_idx)
        metadata["data_rows"] = len(df)
        metadata["status"] = "success"
        return df, metadata

    def _build_dataframe(self, rows: List[List[str]], header_idx: int) -> pd.DataFrame:
        if header_idx >= len(rows) - 1:
            raise ValueError(f"表头索引 {header_idx} 无效，文件仅有 {len(rows)} 行")
        headers = [str(h).strip() for h in rows[header_idx]]
        data_rows = rows[header_idx + 1 :]

        seen = {}
        unique_headers = []
        for h in headers:
            if h in seen:
                seen[h] += 1
                unique_headers.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                unique_headers.append(h)

        df = pd.DataFrame(data_rows, columns=unique_headers)
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].apply(
                    lambda x: x.strip() if isinstance(x, str) else x
                )
        return df


def batch_process(
    input_dir: str, output_dir: str, config: HeaderConfig
) -> Dict[str, object]:
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    stats = {"total": 0, "success": 0, "failed": 0, "empty": 0, "files_detail": []}
    processor = CSVFileProcessor(config)

    for file_path in sorted(input_path.glob("*.csv")):
        stats["total"] += 1
        df, metadata = processor.process_file(file_path)
        stats["files_detail"].append(metadata)
        if df is not None and not df.empty:
            output_file = output_path / file_path.name
            df.to_csv(
                output_file,
                index=False,
                encoding="utf-8-sig",
                quoting=csv.QUOTE_MINIMAL,
            )
            stats["success"] += 1
        elif metadata["status"] == "empty":
            stats["empty"] += 1
        else:
            stats["failed"] += 1

    return stats
