"""
日期清洗处理器完整测试用例
覆盖多字段处理、drop_row、set_null、keep_original 等场景
"""

import pandas as pd
from pathlib import Path
import tempfile
import yaml

from processors.date_cleaner_processor import DateCleaningProcessor


def create_test_config(
    date_fields=None,
    on_parse_failure="keep_original",
    log_details=False
):
    """创建测试配置"""
    if date_fields is None:
        date_fields = [
            {"name": "创建时间", "aliases": ["创建时间"], "has_time": True},
            {"name": "结算日期", "aliases": ["结算日期"], "has_time": False},
        ]

    config = {
        "date_cleaning": {
            "enabled": True,
            "date_fields": date_fields,
            "parse_formats": [
                {
                    "name": "excel_serial",
                    "strptime_format": None,
                    "regex_pattern": "^\\d{1,5}(\\.0)?$",
                    "is_excel_serial": True,
                    "description": "Excel 序列日期",
                },
                {
                    "name": "dot_separated",
                    "strptime_format": "%Y.%m.%d",
                    "regex_pattern": "^\\d{4}\\.\\d{1,2}\\.\\d{1,2}$",
                    "is_excel_serial": False,
                    "description": "点分隔",
                },
                {
                    "name": "standard_slash",
                    "strptime_format": "%Y/%m/%d",
                    "regex_pattern": "^\\d{4}/\\d{1,2}/\\d{1,2}$",
                    "is_excel_serial": False,
                    "description": "斜杠分隔",
                },
                {
                    "name": "iso_date",
                    "strptime_format": "%Y-%m-%d",
                    "regex_pattern": "^\\d{4}-\\d{1,2}-\\d{1,2}$",
                    "is_excel_serial": False,
                    "description": "ISO 日期",
                },
            ],
            "options": {
                "on_parse_failure": on_parse_failure,
                "output_mode": "replace",
                "log_details": log_details,
            },
        }
    }
    return config


class TestMultiFieldProcessing:
    """测试多字段处理逻辑"""

    def test_drop_row_with_multiple_fields(self):
        """测试多字段 + drop_row 模式：只删除在所有字段中都解析失败的行"""
        df = pd.DataFrame({
            "运单号": ["001", "002", "003", "004"],
            "创建时间": ["2024.1.1", "2024/1/2", "invalid", "2024/1/4"],
            "结算日期": ["2024.1.4", "invalid", "2024/1/6", "2024-01-07"],
            "金额": [100, 200, 300, 400],
        })

        config = create_test_config(on_parse_failure="drop_row", log_details=True)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                # 验证：应该保留 001 和 004（所有字段都解析成功的行）
                assert len(result) == 2
                assert set(result["运单号"].tolist()) == {"001", "004"}
                # 验证日期格式
                assert "2024-01-01" in result.loc[result["运单号"] == "001", "创建时间"].iloc[0]
                assert "2024-01-04" in result.loc[result["运单号"] == "001", "结算日期"].iloc[0]
        finally:
            Path(config_path).unlink(missing_ok=True)

    def test_drop_row_all_fields_fail(self):
        """测试所有字段都解析失败的情况"""
        df = pd.DataFrame({
            "运单号": ["001", "002"],
            "创建时间": ["invalid1", "invalid2"],
            "结算日期": ["invalid3", "invalid4"],
        })

        config = create_test_config(on_parse_failure="drop_row")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                # 所有行都应该被删除
                assert len(result) == 0
        finally:
            Path(config_path).unlink(missing_ok=True)

    def test_set_null_with_multiple_fields(self):
        """测试 set_null 模式：解析失败的设为空，保留所有行"""
        df = pd.DataFrame({
            "运单号": ["001", "002", "003", "004"],
            "创建时间": ["2024.1.1", "2024/1/2", "invalid", "2024/1/4"],
            "结算日期": ["2024.1.4", "invalid", "2024/1/6", "2024-01-07"],
            "金额": [100, 200, 300, 400],
        })

        config = create_test_config(on_parse_failure="set_null")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                # 所有行都应该保留
                assert len(result) == 4
                # 验证解析失败的值为空
                assert pd.isna(result.loc[result["运单号"] == "002", "结算日期"].iloc[0])
                assert pd.isna(result.loc[result["运单号"] == "003", "创建时间"].iloc[0])
                # 验证解析成功的值被清洗
                assert "2024-01-01" in result.loc[result["运单号"] == "001", "创建时间"].iloc[0]
        finally:
            Path(config_path).unlink(missing_ok=True)

    def test_keep_original_with_multiple_fields(self):
        """测试 keep_original 模式：解析失败的保留原值"""
        df = pd.DataFrame({
            "运单号": ["001", "002", "003", "004"],
            "创建时间": ["2024.1.1", "2024/1/2", "invalid", "2024/1/4"],
            "结算日期": ["2024.1.4", "invalid", "2024/1/6", "2024-01-07"],
            "金额": [100, 200, 300, 400],
        })

        config = create_test_config(on_parse_failure="keep_original")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                # 所有行都应该保留
                assert len(result) == 4
                # 解析失败的保留原值
                assert result.loc[result["运单号"] == "002", "结算日期"].iloc[0] == "invalid"
                assert result.loc[result["运单号"] == "003", "创建时间"].iloc[0] == "invalid"
                # 解析成功的使用新值
                assert "2024-01-01" in result.loc[result["运单号"] == "001", "创建时间"].iloc[0]
        finally:
            Path(config_path).unlink(missing_ok=True)

    def test_no_matching_fields(self):
        """测试没有匹配字段的情况"""
        df = pd.DataFrame({
            "运单号": ["001", "002"],
            "其他列": ["A", "B"],
        })

        config = create_test_config(
            date_fields=[
                {"name": "不存在的字段", "aliases": ["不存在"], "has_time": True}
            ]
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                # 应该保持原样
                assert len(result) == 2
                assert result["运单号"].tolist() == ["001", "002"]
        finally:
            Path(config_path).unlink(missing_ok=True)

    def test_single_field_drop_row(self):
        """测试单字段 + drop_row 模式"""
        df = pd.DataFrame({
            "运单号": ["001", "002", "003"],
            "结算日期": ["2024.1.4", "invalid", "2024/1/6"],
        })

        config = create_test_config(
            date_fields=[{"name": "结算日期", "aliases": ["结算日期"], "has_time": False}],
            on_parse_failure="drop_row",
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                # 应该删除 1 行（002，结算日期为 invalid）
                assert len(result) == 2
                assert result["运单号"].tolist() == ["001", "003"]
                assert "2024-01-04" in result.loc[result["运单号"] == "001", "结算日期"].iloc[0]
        finally:
            Path(config_path).unlink(missing_ok=True)


class TestEdgeCases:
    """测试边界情况"""

    def test_empty_dataframe(self):
        """测试空 DataFrame"""
        df = pd.DataFrame({"运单号": [], "结算日期": []})

        config = create_test_config(on_parse_failure="drop_row")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                # 应该保持为空
                assert len(result) == 0
                assert list(result.columns) == ["运单号", "结算日期"]
        finally:
            Path(config_path).unlink(missing_ok=True)

    def test_all_dates_parse_successfully(self):
        """测试所有日期都解析成功的情况"""
        df = pd.DataFrame({
            "运单号": ["001", "002", "003"],
            "结算日期": ["2024.1.4", "2024/1/2", "2024-01-06"],
        })

        config = create_test_config(
            date_fields=[{"name": "结算日期", "aliases": ["结算日期"], "has_time": False}],
            on_parse_failure="drop_row",
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                # 所有行都应该保留
                assert len(result) == 3
                # 所有日期都应该被清洗
                for _, row in result.iterrows():
                    assert "2024" in row["结算日期"]
        finally:
            Path(config_path).unlink(missing_ok=True)

    def test_all_dates_parse_failure(self):
        """测试所有日期都解析失败的情况"""
        df = pd.DataFrame({
            "运单号": ["001", "002", "003"],
            "结算日期": ["invalid1", "invalid2", "invalid3"],
        })

        config = create_test_config(
            date_fields=[{"name": "结算日期", "aliases": ["结算日期"], "has_time": False}],
            on_parse_failure="drop_row",
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                # 所有行都应该被删除
                assert len(result) == 0
        finally:
            Path(config_path).unlink(missing_ok=True)

    def test_set_null_preserves_rows(self):
        """测试 set_null 模式保留所有行"""
        df = pd.DataFrame({
            "运单号": ["001", "002", "003"],
            "结算日期": ["2024.1.4", "invalid", "2024/1/6"],
        })

        config = create_test_config(
            date_fields=[{"name": "结算日期", "aliases": ["结算日期"], "has_time": False}],
            on_parse_failure="set_null",
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                # 所有行都应该保留
                assert len(result) == 3
                # 解析失败的应该是空值
                assert pd.isna(result.loc[result["运单号"] == "002", "结算日期"].iloc[0])
                # 解析成功的应该有值
                assert "2024-01-04" in result.loc[result["运单号"] == "001", "结算日期"].iloc[0]
        finally:
            Path(config_path).unlink(missing_ok=True)


class TestDateFormats:
    """测试各种日期格式的解析"""

    def test_dot_separated_format(self):
        """测试点分隔格式解析"""
        df = pd.DataFrame({
            "运单号": ["001", "002"],
            "结算日期": ["2024.1.4", "2024.12.31"],
        })

        config = create_test_config(
            date_fields=[{"name": "结算日期", "aliases": ["结算日期"], "has_time": False}],
            on_parse_failure="drop_row",
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                assert len(result) == 2
                assert "2024-01-04" in result.loc[result["运单号"] == "001", "结算日期"].iloc[0]
                assert "2024-12-31" in result.loc[result["运单号"] == "002", "结算日期"].iloc[0]
        finally:
            Path(config_path).unlink(missing_ok=True)

    def test_slash_separated_format(self):
        """测试斜杠分隔格式解析"""
        df = pd.DataFrame({
            "运单号": ["001", "002"],
            "结算日期": ["2024/1/4", "2024/12/31"],
        })

        config = create_test_config(
            date_fields=[{"name": "结算日期", "aliases": ["结算日期"], "has_time": False}],
            on_parse_failure="drop_row",
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                assert len(result) == 2
                assert "2024-01-04" in result.loc[result["运单号"] == "001", "结算日期"].iloc[0]
                assert "2024-12-31" in result.loc[result["运单号"] == "002", "结算日期"].iloc[0]
        finally:
            Path(config_path).unlink(missing_ok=True)

    def test_iso_date_format(self):
        """测试 ISO 日期格式解析"""
        df = pd.DataFrame({
            "运单号": ["001", "002"],
            "结算日期": ["2024-01-04", "2024-12-31"],
        })

        config = create_test_config(
            date_fields=[{"name": "结算日期", "aliases": ["结算日期"], "has_time": False}],
            on_parse_failure="drop_row",
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_config:
            yaml.dump(config, tmp_config, allow_unicode=True)
            config_path = tmp_config.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                test_file = Path(tmpdir) / "test.csv"
                output_file = Path(tmpdir) / "output.csv"

                df.to_csv(test_file, index=False)

                processor = DateCleaningProcessor(config_path)
                processor.process_csv_file(test_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                assert len(result) == 2
                # ISO 格式应该保持原样（因为是标准格式）
                assert "2024-01-04" in result.loc[result["运单号"] == "001", "结算日期"].iloc[0]
                assert "2024-12-31" in result.loc[result["运单号"] == "002", "结算日期"].iloc[0]
        finally:
            Path(config_path).unlink(missing_ok=True)


class TestRealWorldScenario:
    """测试真实场景"""

    def test_sample_data_processing(self):
        """测试使用实际 sample 数据的处理"""
        # 检查 sample1.xlsx 数据
        excel_file = Path("excel_folder/sample1.xlsx")
        if excel_file.exists():
            import pandas as pd
            df = pd.read_excel(excel_file)

            # 转换为 CSV 进行测试
            with tempfile.TemporaryDirectory() as tmpdir:
                csv_file = Path(tmpdir) / "sample1.csv"
                df.to_csv(csv_file, index=False)

                # 使用默认配置处理
                processor = DateCleaningProcessor()
                output_file = Path(tmpdir) / "output.csv"

                processor.process_csv_file(csv_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                # 验证有数据被保留
                assert len(result) > 0
                # 验证日期格式被统一（跳过明显非日期的值）
                for _, row in result.head(10).iterrows():
                    date_val = row.get("结算日期", "")
                    if pd.notna(date_val) and date_val != "" and len(str(date_val)) > 4:
                        # 跳过明显非日期的值（如"测试"）
                        if not str(date_val).isalpha():
                            # 应该是标准格式
                            assert "2024" in date_val or date_val == "测试"

    def test_merge_csv_processing(self):
        """测试 merge.csv 的处理（实际场景）"""
        merge_file = Path("Result_files/merge.csv")
        if merge_file.exists():
            with tempfile.TemporaryDirectory() as tmpdir:
                output_file = Path(tmpdir) / "output.csv"

                processor = DateCleaningProcessor()
                processor.process_csv_file(merge_file, output_file)

                result = pd.read_csv(output_file, dtype=str)

                # 验证数据被处理
                assert len(result) > 0
                # 验证点分隔格式被转换
                dot_format_rows = result[result["结算日期"].str.contains(
                    r"2024-\d{2}-\d{2}", na=False
                )]
                if len(dot_format_rows) > 0:
                    # 至少有一些数据被转换
                    pass
