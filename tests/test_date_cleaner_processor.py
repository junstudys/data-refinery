"""
日期清洗处理器测试
"""

import pytest
import pandas as pd
from pathlib import Path
from processors.date_cleaner_processor import (
    DateCleaningProcessor,
    DateFieldConfig,
    clean_date_files,
)
from utils.date_cleaner import clean_date_vectorized_v2


@pytest.fixture
def sample_config(tmp_path):
    """创建测试配置文件"""
    config_content = """
date_cleaning:
  enabled: true
  output_format: "%Y-%m-%d %H:%M:%S"
  output_format_date_only: "%Y-%m-%d"
  date_fields:
    - name: 创建时间
      aliases:
        - 创建时间
        - time
        - date
      has_time: true
    - name: 订单日期
      aliases:
        - 订单日期
        - order_date
      has_time: false
  parse_formats:
    - name: excel_serial
      strptime_format: null
      regex_pattern: "^\\\\d{1,5}(\\\\.0)?$"
      is_excel_serial: true
      description: "Excel 序列日期"
    - name: standard_slash
      strptime_format: "%Y/%m/%d"
      regex_pattern: "^\\\\d{4}/\\\\d{1,2}/\\\\d{1,2}$"
      is_excel_serial: false
      description: "斜杠分隔"
    - name: compact
      strptime_format: "%Y%m%d"
      regex_pattern: "^\\\\d{8}$"
      is_excel_serial: false
      description: "紧凑格式"
    - name: dot_separated
      strptime_format: "%Y.%m.%d"
      regex_pattern: "^\\\\d{4}\\\\.\\\\d{1,2}\\\\.\\\\d{1,2}$"
      is_excel_serial: false
      description: "点分隔"
    - name: chinese_year_month_day
      strptime_format: "%Y年%m月%d日"
      regex_pattern: "^\\\\d{4}年\\\\d{1,2}月\\\\d{1,2}日$"
      is_excel_serial: false
      description: "中文年月日"
    - name: chinese_year_month_hao
      strptime_format: "%Y年%m月%d号"
      regex_pattern: "^\\\\d{4}年\\\\d{1,2}月\\\\d{1,2}号$"
      is_excel_serial: false
      description: "中文年月号"
    - name: chinese_year_month
      strptime_format: "%Y年%m月"
      regex_pattern: "^\\\\d{4}年\\\\d{1,2}月$"
      is_excel_serial: false
      description: "中文年月"
    - name: iso_datetime
      strptime_format: "%Y-%m-%d %H:%M:%S"
      regex_pattern: "^\\\\d{4}-\\\\d{2}-\\\\d{2} \\\\d{2}:\\\\d{2}:\\\\d{2}$"
      is_excel_serial: false
      description: "ISO 日期时间"
  auto_parse_first: true
  options:
    on_parse_failure: keep_original
    remove_decimal_zero: true
    log_details: false
    output_mode: replace
"""
    config_file = tmp_path / "date_formats.yaml"
    config_file.write_text(config_content, encoding="utf-8")
    return str(config_file)


@pytest.fixture
def sample_csv(tmp_path):
    """创建测试 CSV 文件"""
    csv_file = tmp_path / "test.csv"
    df = pd.DataFrame({
        "创建时间": ["2024年1月2日", "45118", "2020/1/1", "20200123", "2020.1.24"],
        "订单日期": ["2023年2月", "45119.0", "invalid", "2024/3/15", "20251231"],
        "其他列": ["a", "b", "c", "d", "e"]
    })
    df.to_csv(csv_file, index=False)
    return csv_file


class TestDateFieldConfig:
    """测试日期字段配置类"""

    def test_init(self):
        """测试初始化"""
        config = {
            "name": "创建时间",
            "aliases": ["time", "date"],
            "has_time": True,
        }
        field_cfg = DateFieldConfig(config)
        assert field_cfg.name == "创建时间"
        assert field_cfg.aliases == ["time", "date"]
        assert field_cfg.has_time is True


class TestDateCleaningProcessor:
    """测试日期清洗处理器"""

    def test_init(self, sample_config):
        """测试初始化"""
        processor = DateCleaningProcessor(sample_config)
        assert processor.enabled is True
        assert len(processor.parse_formats) > 0

    def test_normalize_column_name(self, sample_config):
        """测试列名标准化"""
        processor = DateCleaningProcessor(sample_config)
        assert processor._normalize_column_name(" 创建时间 ") == "创建时间"
        assert processor._normalize_column_name("\ufeffTime") == "time"

    def test_resolve_date_column(self, sample_config):
        """测试日期列名解析"""
        processor = DateCleaningProcessor(sample_config)
        df = pd.DataFrame({
            "创建时间": ["2024年1月2日", "45118"],
            "其他列": ["a", "b"]
        })

        field_cfg = DateFieldConfig({
            "name": "创建时间",
            "aliases": ["time", "date"],
            "has_time": True,
        })
        resolved = processor._resolve_date_column(df, field_cfg)
        assert resolved == "创建时间"

    def test_resolve_date_column_by_alias(self, sample_config):
        """测试通过别名解析日期列"""
        processor = DateCleaningProcessor(sample_config)
        df = pd.DataFrame({
            "time": ["2024年1月2日", "45118"],
            "其他列": ["a", "b"]
        })

        field_cfg = DateFieldConfig({
            "name": "创建时间",
            "aliases": ["time", "date"],
            "has_time": True,
        })
        resolved = processor._resolve_date_column(df, field_cfg)
        assert resolved == "time"

    def test_process_csv_file(self, sample_config, sample_csv, tmp_path):
        """测试处理 CSV 文件"""
        processor = DateCleaningProcessor(sample_config)
        output_file = tmp_path / "output.csv"

        processor.process_csv_file(sample_csv, output_file)

        # 验证输出文件存在
        assert output_file.exists()

        # 验证清洗结果
        result_df = pd.read_csv(output_file, dtype=str)
        assert "2024-01-02" in result_df["创建时间"].iloc[0]
        assert "2023" in result_df["创建时间"].iloc[1]

    def test_process_folder(self, sample_config, sample_csv, tmp_path):
        """测试处理文件夹"""
        # 创建输入文件夹
        input_folder = tmp_path / "input"
        input_folder.mkdir()
        (input_folder / "test.csv").write_text(sample_csv.read_text())

        output_folder = tmp_path / "output"

        processor = DateCleaningProcessor(sample_config)
        processor.process_folder(str(input_folder), str(output_folder))

        # 验证输出文件夹和文件
        assert output_folder.exists()
        assert (output_folder / "test.csv").exists()


class TestDateCleaningUtility:
    """测试日期清洗工具函数"""

    def test_excel_serial_conversion(self):
        """测试 Excel 序列日期转换"""
        dates = pd.Series(["45118", "45119.0"])
        result = clean_date_vectorized_v2(dates, [(None, r"^\d{1,5}(\.0)?$")])
        # 检查结果不为空且包含年份
        assert pd.notna(result.iloc[0])
        assert "2023" in str(result.iloc[0])
        assert pd.notna(result.iloc[1])
        assert "2023" in str(result.iloc[1])

    def test_chinese_format_conversion(self):
        """测试中文日期格式转换"""
        dates = pd.Series(["2024年1月2日", "2023年2月"])
        formats = [
            ("%Y年%m月%d日", r"^\d{4}年\d{1,2}月\d{1,2}日$"),
            ("%Y年%m月", r"^\d{4}年\d{1,2}月$"),
        ]
        result = clean_date_vectorized_v2(dates, formats)
        assert "2024-01-02" in result.iloc[0]
        assert "2023-02" in result.iloc[1]

    def test_standard_slash_conversion(self):
        """测试斜杠分隔格式转换"""
        dates = pd.Series(["2020/1/1", "2020/12/31"])
        formats = [("%Y/%m/%d", r"^\d{4}/\d{1,2}/\d{1,2}$")]
        result = clean_date_vectorized_v2(dates, formats)
        assert "2020-01-01" in result.iloc[0]
        assert "2020-12-31" in result.iloc[1]

    def test_compact_format_conversion(self):
        """测试紧凑格式转换"""
        dates = pd.Series(["20200123", "20231231"])
        formats = [("%Y%m%d", r"^\d{8}$")]
        result = clean_date_vectorized_v2(dates, formats)
        assert "2020-01-23" in result.iloc[0]
        assert "2023-12-31" in result.iloc[1]

    def test_dot_separated_conversion(self):
        """测试点分隔格式转换"""
        dates = pd.Series(["2020.1.24", "2023.12.31"])
        formats = [("%Y.%m.%d", r"^\d{4}\.\d{1,2}\.\d{1,2}$")]
        result = clean_date_vectorized_v2(dates, formats)
        assert "2020-01-24" in result.iloc[0]
        assert "2023-12-31" in result.iloc[1]

    def test_iso_datetime_conversion(self):
        """测试 ISO 日期时间格式转换"""
        dates = pd.Series(["2023-01-02 23:30:31"])
        formats = [("%Y-%m-%d %H:%M:%S", r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")]
        result = clean_date_vectorized_v2(dates, formats)
        assert "2023-01-02 23:30:31" in result.iloc[0]


class TestCleanDateFiles:
    """测试便捷函数"""

    def test_clean_single_file(self, sample_config, sample_csv, tmp_path):
        """测试清洗单个文件"""
        output_file = tmp_path / "output.csv"
        clean_date_files(str(sample_csv), str(output_file), sample_config)

        assert output_file.exists()
        result_df = pd.read_csv(output_file, dtype=str)
        assert "2024-01-02" in result_df["创建时间"].iloc[0]

    def test_clean_folder(self, sample_config, sample_csv, tmp_path):
        """测试清洗文件夹"""
        input_folder = tmp_path / "input"
        input_folder.mkdir()
        (input_folder / "test.csv").write_text(sample_csv.read_text())

        output_folder = tmp_path / "output"
        clean_date_files(str(input_folder), str(output_folder), sample_config)

        assert (output_folder / "test.csv").exists()
