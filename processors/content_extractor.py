import os
from typing import Dict, List

import pandas as pd


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
) -> None:
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"输入目录不存在: {folder_path}")

    files = os.listdir(folder_path)

    if not os.path.exists(result_path):
        os.makedirs(result_path)
    elif clear_output:
        for filename in os.listdir(result_path):
            file_path = os.path.join(result_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
            except Exception as exc:
                print(f"Failed to delete {file_path}. Reason: {exc}")

    dfs = []
    for file in files:
        if file.endswith(".csv") or file.endswith(".xlsx"):
            file_path = os.path.join(folder_path, file)
            if os.path.getsize(file_path) == 0:
                continue
            if file.endswith(".csv"):
                df = pd.read_csv(file_path)
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
                    df = _normalize_int_columns(df)
                    df.to_csv(
                        os.path.join(result_path, file[:-4] + "_new.csv"), index=False
                    )

            elif file.endswith(".xlsx"):
                xls = pd.ExcelFile(file_path)
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name)
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
                        df = _normalize_int_columns(df)
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
        final_df = _normalize_int_columns(final_df)
        final_df.to_csv(os.path.join(result_path, "merge.csv"), index=False)


def _normalize_int_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if (df[col].dtype == "float64") and (df[col] % 1 == 0).all():
            df[col] = df[col].astype(int)
    return df
