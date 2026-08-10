# DataRefinery

## 产品说明
DataRefinery 用于批量处理结构不一致、质量参差的 Excel/CSV 数据文件：
1. Excel → CSV 转换（支持失败重跑与详细日志）
2. 表头识别与标准化
3. 字段提取与聚合统计
4. 字段替换、内容抽取、字段清洗
5. 日期格式识别与统一清洗（支持多种日期格式）
6. 对异常值/公式文本进行预处理与审计

适用场景：异构表格清洗、批量字段抽取、跨来源数据标准化。

---

## 使用方法

### 1. 进入仓库
```bash
git clone https://github.com/junstudys/data-refinery
cd data-refinery
```

### 2. 环境准备（uv）
本项目使用 [uv](https://docs.astral.sh/uv/) 管理依赖与运行脚本，依赖锁定在 `uv.lock`。

```bash
# 安装 uv（macOS / Linux）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 安装 uv（Windows，PowerShell）
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# 同步依赖（首次运行 uv run 时也会自动同步）
uv sync
```

### 3. 一键运行全流程
```bash
uv run cli.py pipeline
```

### 4. 单步执行（可按需组合）
```bash
# Excel 转 CSV
uv run cli.py xlsx-to-csv

# 仅重跑失败文件
uv run cli.py xlsx-to-csv --mode failed

# 启用 Excel 预处理（公式文本/错误审计）
uv run cli.py xlsx-to-csv --preprocess

# 平铺文件
uv run cli.py flatten

# 表头识别
uv run cli.py find-header

# 字段提取与聚合
uv run cli.py extract-fields
uv run cli.py array-agg

# 字段替换
uv run cli.py field-replace

# 内容提取
uv run cli.py extract-content --columns "单号,客户名称" --merge

# 字段清洗
uv run cli.py field-clean

# 日期清洗
uv run cli.py date-clean

# 日期清洗（指定字段，覆盖配置）
uv run cli.py date-clean --columns "创建时间,结算日期"
```

---

## 设置方法（核心配置）
配置文件：`config/pipeline.yaml`

### 1. 路径配置
```yaml
workspace_root: .

paths:
  excel_folder: excel_folder
  excel_preprocess_folder: excel_preprocessed
  csv_results_folder: csv_results_folder
  mid_files: mid_files
  result_files: Result_files
  tmp_find_header_row: mid_files/tmp_find_header_row
  tmp_field_replace: mid_files/tmp_field_replace
```

所有由流水线创建、写入或清理的目录都必须是 `workspace_root` 的严格子目录。程序会拒绝工作空间根目录本身、父目录、工作空间外路径、普通文件及包含符号链接的路径，避免配置错误造成递归误删。`dir_policies` 中只有显式设为 `true` 的目录才会在运行前清理，未配置的策略默认不清理。

> 输入与输出不要配置成同一路径。需要使用其他位置时，应把 `workspace_root` 设置为一个专用工作目录，再将各输出目录配置在其下。

### 2. 失败补救与重跑
```yaml
xlsx_to_csv:
  retry_mode: all
  failed_list_path: mid_files/failed_xlsx.csv
  allow_flatten_on_success_only: true
  failure_markers:
    - "#N/A"
    - "#NAME?"
    - "#VALUE!"
    - "#REF!"
    - "#DIV/0!"
  failure_scan_columns: []
```
说明：如果 CSV 中出现 `failure_markers`，该文件将被视为失败并进入补救流程。

### 3. 失败补救（预处理 + 重跑）
```yaml
excel_preprocess:
  enabled: false
  fallback_on_failed: true
  in_place: false
  audit_log_path: logs/excel_error_audit.csv
  error_handling:
    mode: keep
  formula_text:
    preserve: true
    preserve_if_contains_chinese: true
    preserve_if_no_parentheses: false
```
说明：只对失败文件进行预处理，避免全量耗时。

### 4. 失败文件输出清理
```yaml
cleanup_on_failed:
  enabled: true
  remove_failed_output_folders: true
```

### 5. 字段清洗（多字段规则）
```yaml
order_clean:
  output_mode: replace
  fields:
    - name: 运单号
      aliases: [运单号, 单号]
      min_length: 6
      max_length: 32
      allow_chinese: false
      allowed_pattern: "^[A-Za-z0-9]+$"
```

### 6. 长数字与标识字段保真

字段替换、字段统计和内容合并阶段会将业务数据按文本读取，避免 pandas 把整列推断为浮点数。运单号、订单号、银行卡号、客户号等长标识中的前导零和完整数字将按 CSV 原始文本保留，不会由本项目转换为科学计数法。

注意 Excel 本身的限制：

- 精确长标识应在源 Excel 中设置为“文本”并以文本方式录入；
- 如果单元格按“常规/数值”保存，Excel 对超长数字通常只有约 15 位有效数字精度；
- Excel 已经舍入的末位无法由本项目恢复；本项目只能保证不再进行额外的 float 转换；
- 若 CSV 原始文本是完整数字，但双击后 Excel 显示为科学计数法，应通过“数据 → 从文本/CSV”导入并将该列指定为文本。

默认运单字段长度上限为 32，可在 `order_clean.fields` 中按具体业务字段覆盖。

### 7. 日期清洗（多种格式支持）
配置文件：`config/date_formats.yaml`

#### 使用方式
- 默认清洗：
  - `uv run cli.py date-clean`
  - 优先处理 `Result_files/merge_cleaned.csv`，不存在时使用 `Result_files/merge.csv` 生成 `merge_cleaned.csv`
- 指定字段（覆盖配置）：
  - `uv run cli.py date-clean --columns "创建时间,结算日期"`
  - 仅清洗传入的列名（严格匹配 CSV 列名，不使用别名）

支持的日期格式：
- Excel 序列日期（45118, 45119.0）
- 点分隔格式（2024.1.4）
- 斜杠分隔（2024/1/4）
- ISO 日期（2024-01-04）
- 紧凑格式（20240104）
- 中文格式（2024年1月4日、2024年1月4号、2024年1月）

```yaml
date_cleaning:
  enabled: true

  # 日期字段识别
  date_fields:
    - name: 创建时间
      aliases: [创建时间, creation_time, 时间, date]
      has_time: true

    - name: 结算日期
      aliases: [结算日期, 结算时间]
      has_time: false

  # 清洗选项
  options:
    # 无法解析时的处理方式：keep_original（保留原值）, set_null（设为空）, drop_row（删除行）
    on_parse_failure: keep_original
    log_details: true
```

### 8. 人工确认继续
```yaml
pipeline:
  manual_continue_after_repair: true
```
说明：补救成功后是否继续执行后续步骤，由用户确认。

---

## 运行输出
- 日志文件：`logs/pipeline.log`
- 失败清单：`mid_files/failed_xlsx.csv`
- 预处理审计：`logs/excel_error_audit.csv`
- 日期清洗结果：`Result_files/merge_cleaned.csv`

---

## 文档归档
详细历史文档在 `docs/` 目录。
