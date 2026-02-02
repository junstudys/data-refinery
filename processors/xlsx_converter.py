import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from xlsx2csv import Xlsx2csv


def _contains_error_markers_in_file(
    csv_path: Path, markers: List[str], columns: List[str]
) -> str:
    with open(csv_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return ""
        scan_columns = columns or reader.fieldnames
        for row in reader:
            for col in scan_columns:
                value = row.get(col, "")
                for marker in markers:
                    if value == marker:
                        return marker
    return ""


def _convert_single_file(
    excel_file: Path,
    output_folder: Path,
    failure_markers: List[str],
    failure_scan_columns: List[str],
) -> Tuple[str, str, int]:
    try:
        Xlsx2csv(
            str(excel_file),
            outputencoding="utf-8",
            dateformat="%Y-%m-%d %H:%M:%S",
            exclude_hidden_sheets=True,
            skip_hidden_rows=False,
        ).convert(str(output_folder), sheetid=0)

        workbook_name = excel_file.stem
        count = 0
        for csv_file in output_folder.glob("*.csv"):
            new_name = f"{workbook_name}_{csv_file.stem}.csv"
            csv_file.rename(output_folder / new_name)
            count += 1

        if failure_markers:
            for csv_file in output_folder.glob("*.csv"):
                marker = _contains_error_markers_in_file(
                    csv_file, failure_markers, failure_scan_columns
                )
                if marker:
                    return (
                        excel_file.name,
                        f"FAILED: error_marker {marker}",
                        count,
                    )

        return excel_file.name, "SUCCESS", count
    except Exception as exc:
        return excel_file.name, f"FAILED: {exc}", 0


def _read_failed_list(path: Path) -> List[str]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row.get("filename", "") for row in reader if row.get("filename")]


def read_failed_list(path: Path) -> List[str]:
    return _read_failed_list(path)


def _write_failed_list(path: Path, failures: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["filename", "error", "timestamp"])
        writer.writeheader()
        for row in failures:
            writer.writerow(row)


def xlsx_to_csv_parallel(
    excel_folder: Path,
    results_folder: Path,
    max_workers: int = 4,
    retry_mode: str = "all",
    failed_list_path: Path | None = None,
    log_detail: bool = False,
    failure_markers: List[str] | None = None,
    failure_scan_columns: List[str] | None = None,
) -> Iterable[Tuple[str, str]]:
    results_folder.mkdir(parents=True, exist_ok=True)
    excel_files = list(excel_folder.glob("*.xlsx"))
    if not excel_files:
        return []

    if retry_mode == "failed" and failed_list_path:
        failed_names = set(_read_failed_list(failed_list_path))
        excel_files = [f for f in excel_files if f.name in failed_names]
        if not excel_files:
            return []

    outputs = []
    failure_markers = failure_markers or []
    failure_scan_columns = failure_scan_columns or []
    failures: List[Dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for excel_file in excel_files:
            output_folder = results_folder / excel_file.stem
            output_folder.mkdir(parents=True, exist_ok=True)
            futures[
                executor.submit(
                    _convert_single_file,
                    excel_file,
                    output_folder,
                    failure_markers,
                    failure_scan_columns,
                )
            ] = excel_file

        for future in as_completed(futures):
            name, status, count = future.result()
            outputs.append((name, status))
            if log_detail:
                if status == "SUCCESS":
                    print(f"✅ 成功: {name} -> 输出 {count} 个 CSV")
                else:
                    print(f"❌ 失败: {name} -> {status}")
            if status.startswith("FAILED"):
                failures.append(
                    {
                        "filename": name,
                        "error": status,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

    if failed_list_path:
        _write_failed_list(failed_list_path, failures)

    return outputs


def run_default(excel_folder: str, results_folder: str, max_workers: int = 4) -> None:
    start = time.time()
    results = xlsx_to_csv_parallel(
        Path(excel_folder), Path(results_folder), max_workers=max_workers
    )
    for name, status in results:
        print(f"{name}: {status}")
    elapsed = (time.time() - start) / 60.0
    print(f"Completed in {elapsed:.2f} minutes")
