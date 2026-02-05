"""
日期清洗处理器
负责识别和清洗各种格式的日期字段
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
from datetime import datetime

from utils.date_cleaner import clean_date_vectorized_v2


class DateFieldConfig:
    """日期字段配置"""

    def __init__(self, config: Dict):
        self.name = config.get("name", "")
        self.aliases = config.get("aliases", [])
        self.has_time = config.get("has_time", True)


class DateCleaningProcessor:
    """日期清洗处理器"""

    def __init__(self, config_path: str = "config/date_formats.yaml"):
        self.config = self._load_config(config_path)
        self.date_cleaning_cfg = self.config.get("date_cleaning", {})
        self.enabled = self.date_cleaning_cfg.get("enabled", True)
        self.parse_formats = self._build_parse_formats()
        self.options = self.date_cleaning_cfg.get("options", {})

    def _load_config(self, config_path: str) -> dict:
        """加载配置文件"""
        import yaml

        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _build_parse_formats(self) -> List[Tuple[str, str]]:
        """构建解析格式列表"""
        formats = []
        for fmt in self.date_cleaning_cfg.get("parse_formats", []):
            if fmt.get("is_excel_serial"):
                # Excel 序列日期特殊处理
                formats.append((None, fmt["regex_pattern"]))
            else:
                formats.append((fmt["strptime_format"], fmt["regex_pattern"]))
        return formats

    def _normalize_column_name(self, name: str) -> str:
        """标准化列名"""
        return str(name).strip().lower().replace("\ufeff", "")

    def _resolve_date_column(
        self, df: pd.DataFrame, field_cfg: DateFieldConfig
    ) -> Optional[str]:
        """解析日期列名"""
        column_lookup = {
            self._normalize_column_name(col): col for col in df.columns
        }
        candidates = [field_cfg.name] + [
            a for a in field_cfg.aliases if a != field_cfg.name
        ]
        for candidate in candidates:
            normalized = self._normalize_column_name(candidate)
            if normalized in column_lookup:
                return column_lookup[normalized]
        return None

    def process_csv_file(self, file_path: Path, output_path: Path) -> None:
        """处理单个 CSV 文件"""
        df = pd.read_csv(file_path, dtype=str)

        # 获取日期字段配置
        date_fields_cfg = self.date_cleaning_cfg.get("date_fields", [])
        field_configs = [DateFieldConfig(cfg) for cfg in date_fields_cfg]

        for field_cfg in field_configs:
            resolved_column = self._resolve_date_column(df, field_cfg)
            if not resolved_column:
                continue

            # 执行日期清洗
            cleaned = clean_date_vectorized_v2(df[resolved_column], self.parse_formats)

            # 应用清洗结果
            output_mode = self.options.get("output_mode", "replace")
            if output_mode == "add_column":
                df[f"{resolved_column}_cleaned"] = cleaned
            else:
                df[resolved_column] = cleaned

            if self.options.get("log_details", False):
                print(f"  清洗列: {resolved_column}")

        # 保存结果
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        if self.options.get("log_details", False):
            print(f"  保存到: {output_path}")

    def process_folder(self, input_folder: str, output_folder: str) -> None:
        """批量处理文件夹中的 CSV 文件"""
        input_path = Path(input_folder)
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        csv_files = list(input_path.glob("*.csv"))
        if not csv_files:
            print(f"文件夹中没有 CSV 文件: {input_folder}")
            return

        for csv_file in csv_files:
            output_file = output_path / csv_file.name
            print(f"处理文件: {csv_file.name}")
            self.process_csv_file(csv_file, output_file)

        print(f"共处理 {len(csv_files)} 个文件")


def clean_date_files(
    input_path: str, output_path: str, config_path: str = "config/date_formats.yaml"
) -> None:
    """
    清洗日期字段的便捷函数

    Args:
        input_path: 输入文件或文件夹路径
        output_path: 输出文件或文件夹路径
        config_path: 配置文件路径
    """
    processor = DateCleaningProcessor(config_path)

    input_p = Path(input_path)
    output_p = Path(output_path)

    if input_p.is_file() and input_p.suffix.lower() == ".csv":
        # 处理单个文件
        output_p.parent.mkdir(parents=True, exist_ok=True)
        processor.process_csv_file(input_p, output_p)
    elif input_p.is_dir():
        # 处理文件夹
        processor.process_folder(str(input_p), str(output_p))
    else:
        print(f"无效的输入路径: {input_path}")
