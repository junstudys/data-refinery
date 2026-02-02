import os
import csv
from typing import List, Tuple

import pandas as pd


def is_file_empty(file_path: str) -> bool:
    with open(file_path, "r") as read_obj:
        return not read_obj.read(1)


def read_files(file_path: str) -> List[Tuple[str, str, str]]:
    files_info = []
    for filename in os.listdir(file_path):
        full_path = os.path.join(file_path, filename)
        if filename.endswith(".csv"):
            if is_file_empty(full_path):
                print(f"File {filename} is empty. Skipping...")
                continue
            df = pd.read_csv(full_path)
            for column in df.columns:
                files_info.append((filename, "csv", column))
        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            xls = pd.ExcelFile(full_path)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name)
                if df.empty:
                    print(f"Sheet {sheet_name} in {filename} is empty. Skipping...")
                    continue
                for column in df.columns:
                    files_info.append((f"{filename}_{sheet_name}", "excel", column))
    return files_info


def save_to_csv(files_info: List[Tuple[str, str, str]], result_file: str) -> None:
    if os.path.exists(result_file):
        os.remove(result_file)
    with open(result_file, "w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["File Name", "File Type", "Field Names"])
        for info in files_info:
            csv_writer.writerow(info)


def extract_fields(input_dir: str, output_file: str) -> None:
    files_info = read_files(input_dir)
    save_to_csv(files_info, output_file)
