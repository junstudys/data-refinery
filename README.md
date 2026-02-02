# DataRefinery

## 产品说明
DataRefinery 用于批量处理结构不一致、质量参差的 Excel/CSV 数据文件：
1. Excel → CSV 转换（支持失败重跑与详细日志）
2. 表头识别与标准化
3. 字段提取与聚合统计
4. 字段替换、内容抽取、字段清洗
5. 对异常值/公式文本进行预处理与审计

适用场景：异构表格清洗、批量字段抽取、跨来源数据标准化。

---

## 使用方法

### 1. 进入仓库
```bash
git clone <your-repo-url>
cd <your-repo>
```

### 2. 一键运行全流程
```bash
python cli.py pipeline
```

### 3. 单步执行（可按需组合）
```bash
# Excel 转 CSV
python cli.py xlsx-to-csv

# 仅重跑失败文件
python cli.py xlsx-to-csv --mode failed

# 启用 Excel 预处理（公式文本/错误审计）
python cli.py xlsx-to-csv --preprocess

# 平铺文件
python cli.py flatten

# 表头识别
python cli.py find-header

# 字段提取与聚合
python cli.py extract-fields
python cli.py array-agg

# 字段替换
python cli.py field-replace

# 内容提取
python cli.py extract-content --columns "单号,客户名称" --merge

# 字段清洗
python cli.py field-clean
```

---

## 设置方法（核心配置）
配置文件：`config/pipeline.yaml`

### 1. 路径配置
```yaml
paths:
  excel_folder: excel_folder
  excel_preprocess_folder: excel_preprocessed
  csv_results_folder: csv_results_folder
  mid_files: mid_files
  result_files: Result_files
  tmp_find_header_row: mid_files/tmp_find_header_row
  tmp_field_replace: mid_files/tmp_field_replace
```

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
      min_length: 12
      max_length: 18
      allow_chinese: false
      allowed_pattern: "^[A-Za-z0-9]+$"
```

### 6. 人工确认继续
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

---

## 文档归档
详细历史文档在 `docs/` 目录。
