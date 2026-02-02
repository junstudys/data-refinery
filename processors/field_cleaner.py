from pathlib import Path
from typing import Dict, List

import pandas as pd


def _vectorized_clean(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.str.replace(r"\.0$", "", regex=True)
    return cleaned


def _normalize_column(name: str) -> str:
    return str(name).strip().lower().replace("\ufeff", "")


def resolve_order_column(df: pd.DataFrame, preferred: str, aliases: List[str]) -> str:
    column_lookup = {_normalize_column(col): col for col in df.columns}
    candidates = [preferred] + [a for a in aliases if a != preferred]
    for candidate in candidates:
        normalized = _normalize_column(candidate)
        if normalized in column_lookup:
            return column_lookup[normalized]
    return ""


def _build_rule(rule_cfg: Dict) -> Dict:
    return {
        "name": rule_cfg.get("name", ""),
        "aliases": rule_cfg.get("aliases", []),
        "min_length": rule_cfg.get("min_length", 1),
        "max_length": rule_cfg.get("max_length", 999),
        "allow_chinese": bool(rule_cfg.get("allow_chinese", True)),
        "allowed_pattern": rule_cfg.get("allowed_pattern"),
    }


def _build_clean_mask(series: pd.Series, rule: Dict) -> pd.Series:
    cleaned = _vectorized_clean(series)
    mask = cleaned.str.len().between(rule["min_length"], rule["max_length"])
    if not rule["allow_chinese"]:
        mask &= ~cleaned.str.contains(r"[\u4e00-\u9fff]", regex=True, na=False)
    if rule["allowed_pattern"]:
        mask &= cleaned.str.match(rule["allowed_pattern"], na=False)
    return cleaned, mask


def clean_csv_files(path: str, config: Dict) -> None:
    files_to_process: List[Path] = []
    path_obj = Path(path)
    if path_obj.is_dir():
        files_to_process = list(path_obj.glob("*.csv"))
    elif path_obj.is_file() and path_obj.suffix.lower() == ".csv":
        files_to_process.append(path_obj)
    else:
        print("提供的路径既不是CSV文件也不是包含CSV文件的文件夹。")
        return

    output_mode = config.get("output_mode", "replace")
    fields_cfg = config.get("fields", [])
    rules = [_build_rule(rule) for rule in fields_cfg]

    for file_path in files_to_process:
        df = pd.read_csv(file_path, dtype=str)
        for rule in rules:
            if not rule["name"]:
                continue
            resolved_column = resolve_order_column(df, rule["name"], rule["aliases"])
            if not resolved_column:
                print(f"列不存在: {rule['name']} in {file_path}")
                continue
            cleaned, mask = _build_clean_mask(df[resolved_column], rule)

            if output_mode == "add_column":
                df[f"{rule['name']}_cleaned"] = cleaned
            else:
                df[resolved_column] = cleaned
                df = df.loc[mask].copy()

        cleaned_file_path = file_path.with_name(f"{file_path.stem}_cleaned.csv")
        df.to_csv(cleaned_file_path, index=False)
        print(f"已处理并保存文件：{cleaned_file_path}")


def clean_order_vectorized(
    df: pd.DataFrame,
    column: str,
    min_length: int = 12,
    max_length: int = 18,
    remove_chinese: bool = True,
) -> pd.DataFrame:
    series = _vectorized_clean(df[column])
    length_mask = series.str.len().between(min_length, max_length)
    if remove_chinese:
        chinese_mask = ~series.str.contains(r"[\u4e00-\u9fff]", regex=True, na=False)
    else:
        chinese_mask = pd.Series(True, index=series.index)
    valid_mask = length_mask & chinese_mask
    cleaned_df = df.loc[valid_mask].copy()
    cleaned_df[column] = series[valid_mask]
    return cleaned_df
