import csv
from pathlib import Path

from processors.header_detector import HeaderConfig, CSVFileProcessor


def test_header_not_found_returns_metadata(tmp_path: Path):
    file_path = tmp_path / "data.csv"
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["no", "header", "keywords"])
        writer.writerow(["1", "2", "3"])

    config = HeaderConfig(
        tracking_keywords=["运单号"], default_first_row_when_not_found=False
    )
    processor = CSVFileProcessor(config)

    df, metadata = processor.process_file(file_path)

    assert df is None
    assert metadata["status"] == "header_not_found"
