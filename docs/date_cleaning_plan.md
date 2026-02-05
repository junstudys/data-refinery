# 日期清洗功能设计方案

## 1. 背景与目标

### 1.1 背景
在数据处理过程中，日期字段存在多种格式，包括：
- Excel 序列日期（45118, 45119.0）
- 标准日期格式（2020/1/1, 2020-01-01）
- 紧凑格式（20200123）
- 点分隔格式（2020.1.24）
- 中文格式（2023年2月, 2024年1月2号, 2024年1月3日）
- 带时间格式（2023-01-02 23:30:31）

现有 `utils/date_cleaner.py` 提供了基础清洗功能，但未集成到主流程中。

### 1.2 目标
- 新增日期清洗管道步骤
- 支持灵活的日期格式配置（YAML）
- 统一日期输出格式
- 位于字段清洗（order_clean）之后执行

## 2. 配置文件设计

### 2.1 配置文件路径
`config/date_formats.yaml`

### 2.2 配置结构
```yaml
date_cleaning:
  enabled: true

  # 输出格式配置
  output_format: "%Y-%m-%d %H:%M:%S"  # 完整格式
  output_format_date_only: "%Y-%m-%d"  # 仅日期格式（用于无时间的日期）

  # 日期字段识别
  date_fields:
    # 字段名称及其别名
    - name: 创建时间
      aliases:
        - 创建时间
        - creation_time
        - created_at
        - 时间
        - date
      has_time: true  # 是否包含时间部分

    - name: 订单日期
      aliases:
        - 订单日期
        - order_date
        - 下单日期
        - date
      has_time: false

  # 日期格式解析规则（按优先级）
  # 优先级高的放前面
  parse_formats:
    # 格式名称
    - name: excel_serial
      # strptime 格式（用于解析）
      strptime_format: null
      # 正则表达式匹配模式
      regex_pattern: "^\\d{1,5}(\\.0)?$"
      # 是否为 Excel 序列日期
      is_excel_serial: true
      # 描述
      description: "Excel 序列日期（如 45118, 45119.0）"

    - name: standard_slash
      strptime_format: "%Y/%m/%d"
      regex_pattern: "^\\d{4}/\\d{1,2}/\\d{1,2}$"
      is_excel_serial: false
      description: "斜杠分隔（如 2020/1/1）"

    - name: compact
      strptime_format: "%Y%m%d"
      regex_pattern: "^\\d{8}$"
      is_excel_serial: false
      description: "紧凑格式（如 20200123）"

    - name: dot_separated
      strptime_format: "%Y.%m.%d"
      regex_pattern: "^\\d{4}\\.\\d{1,2}\\.\\d{1,2}$"
      is_excel_serial: false
      description: "点分隔（如 2020.1.24）"

    - name: chinese_year_month_day
      strptime_format: "%Y年%m月%d日"
      regex_pattern: "^\\d{4}年\\d{1,2}月\\d{1,2}日$"
      is_excel_serial: false
      description: "中文年月日（如 2024年1月2日）"

    - name: chinese_year_month_hao
      strptime_format: "%Y年%m月%d号"
      regex_pattern: "^\\d{4}年\\d{1,2}月\\d{1,2}号$"
      is_excel_serial: false
      description: "中文年月号（如 2024年1月2号）"

    - name: chinese_year_month
      strptime_format: "%Y年%m月"
      regex_pattern: "^\\d{4}年\\d{1,2}月$"
      is_excel_serial: false
      description: "中文年月（如 2023年2月）"

    - name: iso_datetime
      strptime_format: "%Y-%m-%d %H:%M:%S"
      regex_pattern: "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"
      is_excel_serial: false
      description: "ISO 日期时间（如 2023-01-02 23:30:31）"

    - name: compact_year_month
      strptime_format: "%Y%m"
      regex_pattern: "^\\d{6}$"
      is_excel_serial: false
      description: "紧凑年月（如 202301）"

    - name: dash_year_month
      strptime_format: "%Y-%m"
      regex_pattern: "^\\d{4}-\\d{1,2}$"
      is_excel_serial: false
      description: "横杠年月（如 2023-01）"

  # 自动解析配置（在自定义格式之前尝试）
  auto_parse_first: true

  # 清洗选项
  options:
    # 无法解析时的处理方式：keep_original（保留原值）, set_null（设为空）, drop_row（删除行）
    on_parse_failure: keep_original
    # 是否删除 .0 后缀
    remove_decimal_zero: true
    # 是否输出清洗日志
    log_details: true
```

## 3. 代码实现

### 3.1 新增处理器
**文件**: `processors/date_cleaner_processor.py`

```python
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

    def _resolve_date_column(self, df: pd.DataFrame, field_cfg: DateFieldConfig) -> Optional[str]:
        """解析日期列名"""
        column_lookup = {self._normalize_column_name(col): col for col in df.columns}
        candidates = [field_cfg.name] + [a for a in field_cfg.aliases if a != field_cfg.name]
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
            cleaned = clean_date_vectorized_v2(
                df[resolved_column],
                self.parse_formats
            )

            # 应用清洗结果
            output_mode = self.date_cleaning_cfg.get("options", {}).get("output_mode", "replace")
            if output_mode == "add_column":
                df[f"{resolved_column}_cleaned"] = cleaned
            else:
                df[resolved_column] = cleaned

        # 保存结果
        df.to_csv(output_path, index=False)

    def process_folder(self, input_folder: str, output_folder: str) -> None:
        """批量处理文件夹中的 CSV 文件"""
        input_path = Path(input_folder)
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        for csv_file in input_path.glob("*.csv"):
            output_file = output_path / csv_file.name
            self.process_csv_file(csv_file, output_file)
```

### 3.2 修改 utils/date_cleaner.py

保留现有核心函数，添加辅助函数：

```python
def add_custom_format(format_tuple: Tuple[str, str]) -> None:
    """添加自定义日期格式到全局列表"""
    DEFAULT_DATE_FORMATS.append(format_tuple)


def get_supported_formats() -> List[str]:
    """获取支持的日期格式描述"""
    return [
        "Excel 序列日期",
        "斜杠分隔 (2020/1/1)",
        "紧凑格式 (20200123)",
        "点分隔 (2020.1.24)",
        "中文年月日 (2024年1月2日)",
        "中文年月号 (2024年1月2号)",
        "中文年月 (2023年2月)",
        "ISO 日期时间 (2023-01-02 23:30:31)",
    ]
```

### 3.3 修改 core/pipeline.py

在 `_init_steps` 方法中添加新步骤：

```python
def _init_steps(self) -> List[PipelineStep]:
    return [
        PipelineStep("xlsx_to_csv", self._step_xlsx_to_csv, "Excel转CSV"),
        PipelineStep("extract_nested", self._step_extract_nested, "文件平铺"),
        PipelineStep("find_header", self._step_find_header, "表头识别"),
        PipelineStep("extract_field_info", self._step_extract_info, "字段提取"),
        PipelineStep("array_agg", self._step_array_agg, "字段聚合"),
        PipelineStep(
            "field_replace", self._step_field_replace, "字段替换", required=False
        ),
        PipelineStep("extract_content", self._step_extract_content, "内容提取"),
        PipelineStep("order_clean", self._step_order_clean, "运单清洗"),
        PipelineStep("date_clean", self._step_date_clean, "日期清洗", required=False),  # 新增
    ]
```

添加新步骤方法：

```python
def _step_date_clean(self) -> None:
    """日期清洗步骤"""
    from processors.date_cleaner_processor import DateCleaningProcessor

    paths = self.config.get("paths", {})
    date_cfg = self.config.get("date_cleaning", {})

    if not date_cfg.get("enabled", True):
        self.logger.info("日期清洗已禁用，跳过")
        return

    processor = DateCleaningProcessor("config/date_formats.yaml")

    input_folder = Path(paths.get("result_files", "Result_files"))
    output_folder = Path(paths.get("date_cleaned_folder", "date_cleaned"))

    # 处理 merge.csv 或整个文件夹
    merge_file = input_folder / "merge.csv"
    if merge_file.exists():
        output_file = output_folder / "merge.csv"
        output_folder.mkdir(parents=True, exist_ok=True)
        processor.process_csv_file(merge_file, output_file)
        self.logger.info(f"日期清洗完成: {output_file}")
    else:
        processor.process_folder(str(input_folder), str(output_folder))
        self.logger.info(f"日期清洗完成，输出到: {output_folder}")
```

### 3.4 修改 config/pipeline.yaml

添加路径配置：

```yaml
paths:
  # ... 现有配置 ...
  date_cleaned_folder: date_cleaned  # 新增

date_cleaning:
  enabled: true
  config_file: config/date_formats.yaml
```

## 4. 测试计划

### 4.1 单元测试
**文件**: `tests/test_date_cleaner_processor.py`

```python
import pytest
import pandas as pd
from processors.date_cleaner_processor import DateCleaningProcessor


@pytest.fixture
def sample_config(tmp_path):
    """创建测试配置文件"""
    config_content = """
date_cleaning:
  enabled: true
  output_format: "%Y-%m-%d %H:%M:%S"
  date_fields:
    - name: 创建时间
      aliases: [创建时间, time, date]
      has_time: true
  parse_formats:
    - name: excel_serial
      strptime_format: null
      regex_pattern: "^\\d{1,5}(\\.0)?$"
      is_excel_serial: true
    # ... 其他格式
"""
    config_file = tmp_path / "date_formats.yaml"
    config_file.write_text(config_content, encoding="utf-8")
    return str(config_file)


def test_date_column_resolution(sample_config):
    """测试日期列名解析"""
    processor = DateCleaningProcessor(sample_config)
    df = pd.DataFrame({
        "创建时间": ["2024年1月2日", "45118"],
        "其他列": ["a", "b"]
    })

    resolved = processor._resolve_date_column(
        df,
        processor.date_cleaning_cfg["date_fields"][0]
    )
    assert resolved == "创建时间"


def test_excel_serial_conversion():
    """测试 Excel 序列日期转换"""
    from utils.date_cleaner import clean_date_vectorized_v2
    import pandas as pd

    dates = pd.Series(["45118", "45119.0"])
    result = clean_date_vectorized_v2(dates, [(None, r"^\d{1,5}(\.0)?$")])
    assert "2023" in result.iloc[0]


def test_chinese_format_conversion():
    """测试中文日期格式转换"""
    from utils.date_cleaner import clean_date_vectorized_v2
    import pandas as pd

    dates = pd.Series(["2024年1月2日", "2023年2月"])
    formats = [
        ("%Y年%m月%d日", r"^\d{4}年\d{1,2}月\d{1,2}日$"),
        ("%Y年%m月", r"^\d{4}年\d{1,2}月$"),
    ]
    result = clean_date_vectorized_v2(dates, formats)
    assert "2024-01-02" in result.iloc[0]
```

### 4.2 集成测试
使用 `example_date.csv` 中的示例数据进行端到端测试。

## 5. 实施步骤

1. **创建配置文件** `config/date_formats.yaml`
2. **创建处理器** `processors/date_cleaner_processor.py`
3. **修改** `core/pipeline.py` 添加新步骤
4. **修改** `config/pipeline.yaml` 添加路径配置
5. **编写单元测试** `tests/test_date_cleaner_processor.py`
6. **运行测试验证**
7. **更新文档**

## 6. 文件变更清单

| 文件路径 | 操作 | 说明 |
|---------|------|------|
| `config/date_formats.yaml` | 新建 | 日期格式配置文件 |
| `processors/date_cleaner_processor.py` | 新建 | 日期清洗处理器 |
| `core/pipeline.py` | 修改 | 添加日期清洗步骤 |
| `config/pipeline.yaml` | 修改 | 添加输出路径配置 |
| `utils/date_cleaner.py` | 修改 | 添加辅助函数 |
| `tests/test_date_cleaner_processor.py` | 新建 | 单元测试 |

## 7. 注意事项

1. **向后兼容**：日期清洗步骤设为 `required=False`，不影响现有流程
2. **配置优先级**：按配置文件中的顺序尝试解析格式
3. **性能考虑**：使用向量化操作，避免逐行处理
4. **错误处理**：无法解析的日期可选择保留原值或设为空
5. **扩展性**：配置文件易于添加新的日期格式
