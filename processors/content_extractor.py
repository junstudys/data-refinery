import os
from pathlib import Path
from typing import Dict, List

import pandas as pd

from utils.path_manager import ensure_dir
from utils.text_fidelity import read_business_csv, read_business_excel


def _normalize_column(name: str) -> str:
    return str(name).strip().lower().replace("\ufeff", "")


def _build_column_lookup(columns: List[str]) -> Dict[str, str]:
    lookup = {}
    for col in columns:
        lookup[_normalize_column(col)] = col
    return lookup


def extract_content(
    folder_path: str,
    result_path: str,
    columns: List[str],
    merge: bool = True,
    clear_output: bool = True,
    workspace_root: str | os.PathLike[str] | None = None,
) -> None:
    input_path = Path(folder_path).resolve()
    result_dir = Path(result_path)
    result_resolved = result_dir.resolve(strict=False)
    if input_path == result_resolved or input_path.is_relative_to(result_resolved):
        raise ValueError("输入目录不能与输出目录相同，也不能位于输出目录内")
    if not input_path.is_dir():
        raise FileNotFoundError(f"输入目录不存在: {folder_path}")

    files = os.listdir(folder_path)
    ensure_dir(result_dir, clear=clear_output, workspace_root=workspace_root)

    dfs = []
    for file in files:
        if file.endswith(".csv") or file.endswith(".xlsx"):
            file_path = os.path.join(folder_path, file)
            if os.path.getsize(file_path) == 0:
                continue
            if file.endswith(".csv"):
                df = read_business_csv(file_path)
                column_lookup = _build_column_lookup(list(df.columns))
                for col in columns:
                    normalized = _normalize_column(col)
                    if normalized in column_lookup:
                        actual_col = column_lookup[normalized]
                        if actual_col != col:
                            df.rename(columns={actual_col: col}, inplace=True)
                            column_lookup[normalized] = col
                    elif col not in df.columns:
                        df[col] = ""
                if merge:
                    df["source"] = file
                df = df[columns + ["source"]] if "source" in df.columns else df[columns]
                dfs.append(df)
                if not merge:
                    df.to_csv(
                        os.path.join(result_path, file[:-4] + "_new.csv"), index=False
                    )

            elif file.endswith(".xlsx"):
                xls = pd.ExcelFile(file_path)
                for sheet_name in xls.sheet_names:
                    df = read_business_excel(file_path, sheet_name=sheet_name)
                    if df.empty:
                        continue
                    column_lookup = _build_column_lookup(list(df.columns))
                    for col in columns:
                        normalized = _normalize_column(col)
                        if normalized in column_lookup:
                            actual_col = column_lookup[normalized]
                            if actual_col != col:
                                df.rename(columns={actual_col: col}, inplace=True)
                                column_lookup[normalized] = col
                        elif col not in df.columns:
                            df[col] = ""
                    if merge:
                        df["source"] = file + "+" + sheet_name
                    df = (
                        df[columns + ["source"]]
                        if "source" in df.columns
                        else df[columns]
                    )
                    dfs.append(df)
                    if not merge:
                        df.to_csv(
                            os.path.join(
                                result_path, file[:-5] + "_" + sheet_name + "_new.csv"
                            ),
                            index=False,
                        )

    if merge:
        if not dfs:
            empty_df = pd.DataFrame(columns=columns + ["source"])
            empty_df.to_csv(os.path.join(result_path, "merge.csv"), index=False)
            return
        final_df = pd.concat(dfs, ignore_index=True)
        final_df.to_csv(os.path.join(result_path, "merge.csv"), index=False)
