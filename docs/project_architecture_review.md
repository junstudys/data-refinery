# DataRefinery 项目审查与架构优化建议

> 审查日期：2026-08-07
> 适用仓库：DataRefinery
> 文档目的：记录当前项目的主要风险、工程与业务架构问题、目标架构及渐进式优化路线，作为后续重构和需求规划依据。
>
> 实施状态：本报告中的“危险目录清理”和“pandas 长文本类型推断”两项 P0 风险已完成第一轮修复与回归测试，详情见 [`implementation_status.md`](implementation_status.md)。其余条目仍是后续优化建议。

## 1. 执行摘要

DataRefinery 已具备可用的 Excel/CSV 批处理能力，包括 Excel 转 CSV、异常补救、表头检测、字段统计与映射、内容合并、运单字段清洗和日期标准化。当前流水线顺序清楚，配置化方向合理，日期清洗也有较充分的自动化测试。

项目目前更接近“功能完整的数据处理脚本”，尚未形成安全、可审计、可恢复的数据流水线产品。主要问题不是算法，而是数据和工件生命周期：

1. 配置错误可能触发危险的递归目录删除。
2. pandas 类型推断可能破坏运单号、订单号等标识字段。
3. 清洗、校验和删除混在一起，数据被丢弃后缺少审计。
4. CLI 单步命令和完整流水线重复实现业务逻辑，行为可能逐渐漂移。
5. 配置缺少统一模型和启动时校验，部分配置实际未生效。
6. 没有明确的运行批次、工件、质量门和数据血缘。
7. 多个阶段全量读取文件，数据规模增大后容易出现内存和性能问题。

建议渐进重构，不推倒重写，也暂不引入微服务、外部工作流平台或分布式计算框架。优先目标应是：

> 安全的数据生命周期、统一的阶段契约、可审计的业务规则。

---

## 2. 当前架构概览

### 2.1 当前处理流程

`core.pipeline.DataPipeline` 按以下顺序执行：

1. Excel 转 CSV。
2. 平铺工作簿输出目录。
3. 检测并规范表头。
4. 提取字段信息到 `mid_files/field_info.csv`。
5. 聚合字段出现情况到 `mid_files/agg.csv`。
6. 使用 `config/dict_zh.xlsx` 替换字段名。
7. 提取指定字段并合并为 `Result_files/merge.csv`。
8. 清洗运单等业务字段，生成 `merge_cleaned.csv`。
9. 清洗日期字段，优先覆盖 `merge_cleaned.csv`。

### 2.2 当前模块职责

- `cli.py`：解析 CLI 参数，也直接解释配置并调用处理器。
- `core/pipeline.py`：定义完整流水线、重试补救、目录清理、人工确认和停止策略。
- `processors/`：每个模块负责一种转换，但普遍同时承担文件遍历、读取、业务转换、写入和输出提示。
- `utils/`：日期解析、日志和目录管理。
- `config/pipeline.yaml`：路径、目录清理、字段检测、重试、预处理、业务清洗和日志配置。
- `config/date_formats.yaml`：日期字段、别名、解析格式和失败策略。
- `config/dict_zh.xlsx`：人工维护的字段映射字典。

### 2.3 当前优点

以下方向值得保留：

- 流水线顺序显式、易理解，见 `core/pipeline.py:56`。
- YAML 配置化适合当前项目规模。
- Excel 转换失败清单、预处理和重试流程具有实际业务价值。
- CSV 编码回退适合中文异构数据，见 `processors/header_detector.py:150`。
- Header 检测已经具有 metadata/status 结果。
- 日期清洗的测试覆盖相对完整。
- `dict_zh.xlsx` 是实用的人工业务控制点。
- 本地 CLI 与文件系统架构目前仍然适用，不必过早平台化。

---

## 3. 优先风险与改进建议

## 3.1 P0：目录清理可能误删任意数据

### 证据

- `utils/path_manager.py:6-13`：`ensure_dir(clear=True)` 使用 `shutil.rmtree()` 删除整个目录。
- `utils/path_manager.py:17-23`：目录策略应用于所有配置路径。
- `core/pipeline.py:93-94`：完整流水线启动时执行目录策略。
- `config/pipeline.yaml:17-24`：多个中间和结果目录默认开启清理。

### 失败场景

如果配置被误写为：

```yaml
paths:
  result_files: .
```

或指向仓库根目录、用户主目录、父目录、共享数据目录，执行流水线可能递归删除无关数据。

### 根因

破坏性清理直接信任裸配置路径，没有工作空间边界、危险路径拒绝、符号链接保护、预演或备份机制。

### 建议

所有生成目录必须位于项目拥有的工作空间下：

```yaml
workspace:
  root: workspace

paths:
  converted: converted
  intermediate: intermediate
  results: results
  logs: logs
```

删除前必须：

- 解析为规范绝对路径；
- 拒绝 `.`, `..`, `/`, 仓库根目录、用户主目录；
- 拒绝工作空间外路径；
- 拒绝符号链接或链接逃逸；
- 输出将被清理的路径；
- 支持 `--dry-run`；
- 最好只清理当前 `run_id` 的临时目录，而不是共享目录。

---

## 3.2 P0：标识字段可能被 pandas 类型推断破坏

### 证据

以下位置未统一使用字符串类型读取：

- `processors/field_replacer.py:35`
- `processors/field_replacer.py:55`
- `processors/content_extractor.py:48`
- `processors/content_extractor.py:72`

内容提取还会尝试将整数样式的浮点列转换为整数：

- `processors/content_extractor.py:112-115`

### 失败场景

以下业务值可能发生不可逆变化：

```text
001234567890
123456789012345678
```

可能出现：

- 前导零丢失；
- 超过 Excel 精度的长数字被舍入；
- 科学计数法输出；
- 空值和字符串 `nan` 混淆；
- 标识字段被数值化后再次序列化。

后续步骤即使使用 `dtype=str`，也无法恢复已经丢失的原始信息。

### 建议

标识类数据默认按文本保真：

```python
pd.read_csv(
    file_path,
    dtype=str,
    keep_default_na=False,
)
```

同时：

- 删除 `_normalize_int_columns()`，或让它按字段显式启用；
- 在字段字典中增加 `data_type` 或 `semantic_type`；
- 对 `identifier` 类型禁止数值转换；
- 增加前导零、18 位数字、科学计数法、空字符串和混合单元格类型测试。

---

## 3.3 P1：正式输出不是原子发布，失败会丢失旧产物

### 证据

`processors/content_extractor.py:30-39` 在完成新一轮处理之前先删除结果目录中的旧文件，之后才读取、转换和写入新结果。

### 已验证场景

1. 结果目录有旧的可用文件；
2. 新一轮提取先删除旧文件；
3. 后续 DataFrame 处理发生异常；
4. 新结果未生成，旧结果也已丢失。

### 建议

采用 staging 和原子发布：

```text
Result_files/
├── current/
└── .staging/<run_id>/
```

执行顺序：

1. 所有新产物写入 `.staging/<run_id>`；
2. 验证文件存在、行数和 Schema；
3. 执行质量门；
4. 成功后原子替换 `current`；
5. 失败则保留旧版本，并保留本次失败记录。

单个文件也应先写入 `*.tmp`，成功后 `rename()` 为正式文件。

---

## 3.4 P1：字段清洗会静默删除业务行

### 证据

- `config/pipeline.yaml:43-61`：默认 `order_clean.output_mode: replace`。
- `processors/field_cleaner.py:72-81`：标准化字段后直接使用校验 mask 过滤 DataFrame。

### 风险

运单号为空、过短、过长、包含中文或不匹配正则的记录会从结果中消失，但没有：

- rejected 文件；
- 删除原因；
- 清洗前后行数；
- 规则命中统计；
- 删除率质量阈值。

### 建议

将三个概念拆开：

1. **标准化**：改变表现形式，如 `"123.0" → "123"`。
2. **校验**：生成 `valid_flag` 和 `validation_reason`。
3. **发布策略**：决定保留、隔离、删除或终止。

建议配置：

```yaml
validation:
  invalid_row_policy: quarantine
  max_invalid_rate: 0.05
```

可支持：

- `keep`：保留并标记；
- `quarantine`：写入拒绝文件；
- `drop`：显式授权删除；
- `fail`：超过阈值则失败。

至少输出：

```text
merge_standardized.csv
invalid_rows.csv
validation_summary.csv
```

---

## 3.5 P1：转换失败产物可能进入后续流程

### 证据

- `processors/xlsx_converter.py:49-59`：检测到错误标记后将工作簿标记为失败。
- `processors/xlsx_converter.py:144-145`：写失败清单，但转换器本身不清理失败产物。
- `cli.py:111-122`：单步 `xlsx-to-csv` 可直接调用转换器。
- 清理只存在于完整流水线的 `core/pipeline.py:150-153` 和 `core/pipeline.py:191-194`。

### 失败场景

用户单独运行转换后，再手工执行 `flatten` 或其他阶段，失败工作簿的 CSV 仍可能参与后续处理。

### 建议

转换器必须自行保证：

> 只有验证成功的文件才会出现在成功产物目录中。

采用每文件临时目录：

```text
.staging/<run_id>/<workbook>/
```

检查错误标记后：

- 成功：整体移动到正式 converted 目录；
- 失败：移动到 quarantine 或删除临时产物，并写审计记录。

---

## 3.6 P1：重跑会与旧 CSV 冲突

### 证据

- `processors/xlsx_converter.py:115-116`：复用已有工作簿输出目录。
- `processors/xlsx_converter.py:44-47`：遍历目录内全部 CSV 并重命名。

### 失败场景

上次转换中断后留下的 CSV 会在本次重跑时被再次重命名，可能导致：

- 重复前缀；
- 文件名冲突；
- 旧文件被误认为本次产物；
- 不同转换批次混在一起。

### 建议

每次转换使用独立临时目录，只移动本次实际生成的文件，不在持久化目录中边写边改名。

---

## 4. 已确认的正确性与可靠性问题

## 4.1 不规则 CSV 行会让 Header 批处理崩溃

### 证据

- `processors/header_detector.py:150-159`：完整读取 CSV 行。
- `processors/header_detector.py:205-227`：直接使用表头列数构建 DataFrame。
- `processors/header_detector.py:230-258`：批处理没有为单文件构建异常提供隔离。

以下两种数据均可触发 `ValueError`：

```csv
运单号,客户
123456
```

```csv
运单号,客户
123456,张三,额外字段
```

### 建议

- 短行补空值；
- 长行按配置截断、放入 `_extra_columns`，或隔离该行；
- 输出 `ragged_row_count`、行号、预期列数和实际列数；
- 单个文件失败默认不应终止整个批次。

---

## 4.2 日期输出格式配置未生效

### 证据

配置定义：

- `config/date_formats.yaml:4-6`：`output_format` 和 `output_format_date_only`。
- `config/date_formats.yaml:20` 等：字段级 `has_time`。

实现始终使用：

- `utils/date_cleaner.py:57`：`%Y-%m-%d %H:%M:%S`。

因此日期字段即使设置 `has_time: false`，也可能输出 `00:00:00`。

### 建议

日期清洗函数接收字段级输出格式：

```python
clean_date_series(
    series,
    parse_formats,
    output_format="%Y-%m-%d",
)
```

测试必须精确断言最终字符串，而不是只检查是否包含年份。

---

## 4.3 日期别名存在语义冲突

`date` 同时是 `创建时间` 和 `订单日期` 的别名。当前解析器会让两个配置匹配到同一列。

### 建议

配置加载阶段检测：

- canonical name 重复；
- 别名重复；
- 大小写、空格和 BOM 标准化后的重复；
- 一个旧字段映射到多个新字段。

存在歧义时应启动失败，而不是按配置顺序猜测。

---

## 4.4 `auto_parse_first` 配置与实现相反

`config/date_formats.yaml` 中的说明表示自动解析优先，但 `utils/date_cleaner.py:28-50` 实际先使用自定义格式，再执行自动解析。

### 建议

二选一：

- 真正实现 `auto_parse_first`；
- 删除该无效配置并修正文档。

对存在歧义的日期值增加解析优先级测试。

---

## 4.5 数字日期可能被误认为 Excel 序列日期

配置将较宽泛的纯数字模式视为 Excel 序列日期。类似 `2024` 的值如果实际表示年份，可能被转换成 1905 年附近的 Excel 日期。

### 建议

- 将 Excel 序列号限制在业务上合理的范围；
- 明确是否支持年份值；
- 对歧义数字输出警告或隔离；
- 支持按字段关闭 Excel 序列解析。

---

## 4.6 `StateManager` 存在 JSON 键类型漂移

### 证据

- `core/state_manager.py:34-39` 使用整数 step 作为字典键。
- JSON 对象键保存后会变成字符串。

已验证连续执行：

```python
mark_completed(1, "a.xlsx")
mark_completed(1, "b.xlsx")
```

重新载入后可能只剩：

```python
{"1": ["b.xlsx"]}
```

当前 `StateManager` 未接入主流水线，因此属于有缺陷的未使用代码。

### 建议

- 暂不支持恢复：删除该模块；或
- 正式支持恢复：使用字符串 stage ID，定义版本化状态 Schema，并测试保存、读取、追加和损坏恢复。

---

## 4.7 `DataQualityChecker` 未接入且有边界错误

`core/quality_checker.py` 没有被流水线使用。对“有行但没有列”的 DataFrame，`core/quality_checker.py:36-43` 的完整度计算会除零。

### 建议

不要保留“看起来已有质量门”的原型代码。应当：

- 删除，待真正需要时重建；或
- 作为正式 `validate` stage 接入，并补齐边界测试和业务规则配置。

---

## 4.8 大写扩展名和部分文件类型被静默忽略

多个位置使用大小写敏感的 `glob("*.xlsx")` 或 `endswith(".csv")`。在大小写敏感环境中，`BOOK.XLSX`、`DATA.CSV` 会被忽略。

### 建议

统一使用：

```python
path.suffix.lower() in {".xlsx", ".xls"}
```

并排除 Excel 临时文件 `~$*.xlsx`。

---

## 4.9 CSV 公式注入风险

源数据中的文本若以 `=`, `+`, `-`, `@` 开头，输出 CSV 被 Excel 打开时可能被当作公式执行。相关输出位置包括：

- `processors/field_replacer.py:43`
- `processors/content_extractor.py:65-66`
- `processors/content_extractor.py:109`
- `processors/date_cleaner_processor.py:162`

### 建议

增加可配置的 CSV 公式中和策略：

```yaml
export:
  neutralize_spreadsheet_formulas: true
```

对危险前缀添加单引号或采用业务认可的转义方式。该行为会改变原始文本，必须明确记录并允许关闭。

---

## 5. 工程质量问题

## 5.1 CLI 与 Pipeline 重复实现业务逻辑

`cli.py` 对各子命令直接解释配置和调用 processor；`core/pipeline.py` 又实现一次相同阶段。转换、预处理和日期清洗等行为存在两条执行路径。

### 风险

- 修复只落在其中一条路径；
- 单步执行和完整执行语义不同；
- 默认值逐渐漂移；
- 测试难以覆盖所有组合。

### 建议

CLI 只负责：

1. 参数解析；
2. 加载配置；
3. 调用统一 Stage；
4. 展示 StageResult。

完整流水线和单步命令必须调用同一套 Stage 实现。

---

## 5.2 裸字典配置缺少统一校验

配置分别在以下位置独立加载：

- `cli.py:22-24`
- `core/pipeline.py:52-54`
- `processors/date_cleaner_processor.py:32-37`

默认值散落在 YAML、CLI、Pipeline 和 Processor 中。

### 后果

- 拼错配置键时静默回退；
- 类型错误运行到中途才暴露；
- 配置写了但不一定生效；
- 无法输出最终生效配置；
- 无法集中做危险路径和别名冲突检查。

### 建议

增加统一配置层：

```python
@dataclass(frozen=True)
class AppConfig:
    paths: PathConfig
    conversion: ConversionConfig
    header: HeaderConfig
    mapping: MappingConfig
    cleaning: CleaningConfig
    validation: ValidationConfig
```

启动时完成：

- 默认值合并；
- 类型校验；
- 路径解析；
- 危险路径检查；
- 字段和别名冲突检查；
- 配置快照输出。

当前规模可使用 dataclass；若需要更强错误信息，可选择 Pydantic。

---

## 5.3 Processor 混合 IO、业务转换和展示

例如 `processors/content_extractor.py` 同时负责：

- 文件遍历；
- 删除输出；
- CSV/XLSX 读取；
- 列名解析；
- 缺失列填充；
- 合并；
- 数据类型转换；
- 文件写入；
- 错误输出。

### 建议

只拆分高价值边界，不做过度抽象：

```python
def project_columns(df, columns) -> ProjectionResult: ...
def apply_field_mapping(df, mapping) -> MappingResult: ...
def clean_order_fields(df, rules) -> CleaningResult: ...
def normalize_date_fields(df, rules) -> DateResult: ...
```

文件读取、写入和遍历放在 adapter/stage 层。

---

## 5.4 错误和日志处理不统一

当前混用：

- `print()`；
- root logging；
- Pipeline logger；
- 返回 `False`；
- 返回空列表；
- 吞异常；
- 抛异常。

结果是流水线可能记录“步骤完成”，但没有汇总：

- Header 识别失败文件数；
- 被删除的行数；
- 日期解析失败数；
- 未映射字段数；
- 空产物和跳过产物数。

### 建议

所有阶段返回统一的 `StageResult`，Pipeline 负责展示和决策，不让 processor 自己决定日志格式。

---

## 5.5 测试、打包和 CI 不完整

当前验证结果：

```text
uv run python -m pytest
62 passed, 3 warnings
```

但：

- `uv run pytest` 在当前环境可能因顶层模块导入路径而失败；
- 没有 CI；
- 没有 lint/formatter/type-check；
- 没有 coverage 插件；
- `pytest` 位于生产依赖；
- pandas 只有最低版本，没有兼容上限；
- 部分测试依赖本地可选数据，干净 checkout 下可能没有实际执行核心断言。

### 建议

最小工程化配置：

```toml
[dependency-groups]
dev = [
  "pytest",
  "pytest-cov",
  "ruff",
  "mypy",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
```

CI 至少执行：

```bash
uv lock --check
uv run ruff check .
uv run ruff format --check .
uv run python -m pytest
```

后续可迁移为标准包：

```text
src/datarefinery/
```

并注册：

```toml
[project.scripts]
datarefinery = "datarefinery.cli:run"
```

---

## 6. 性能问题与优化顺序

## 6.1 当前瓶颈

- `processors/header_detector.py:150-159`：为了检测前几行表头，读取整个 CSV。
- `processors/field_extractor.py:21`：为了提取列名，读取整个 CSV。
- `processors/content_extractor.py:41`：将所有 DataFrame 保存在 `dfs`。
- `processors/content_extractor.py:107`：一次性 `pd.concat()`。
- `processors/field_cleaner.py:64`：逐文件全量读入。
- `config/pipeline.yaml` 中的 `chunk_size` 当前没有实际用于主要处理阶段。

## 6.2 第一阶段：低成本优化

- Header 检测只读取 `max_rows_to_check` 所需行数；
- 字段提取使用 `pd.read_csv(..., nrows=0)`；
- Excel 字段提取只读取表头；
- 合并结果增量写入；
- 使用迭代器遍历文件，不构建无必要的全量列表。

## 6.3 第二阶段：按块处理

- 内容提取使用 `chunksize`；
- 运单清洗按块处理；
- 日期清洗按块处理；
- rejected rows 和统计结果跨 chunk 正确合并。

## 6.4 暂不采用

当前不建议直接引入 Spark 或分布式执行。若单文件达到数 GB，再评估 DuckDB 或 Polars；在此之前应优先修复正确性和工件生命周期。

---

## 7. 推荐的软件项目架构

## 7.1 目标结构

```text
datarefinery/
├── cli.py
│   └── 参数解析和展示
│
├── app/
│   ├── commands.py
│   └── interactions.py
│
├── core/
│   ├── config.py
│   ├── contracts.py
│   ├── pipeline.py
│   ├── artifacts.py
│   └── validation.py
│
├── stages/
│   ├── intake.py
│   ├── preprocess.py
│   ├── convert.py
│   ├── header.py
│   ├── profile.py
│   ├── mapping.py
│   ├── extract.py
│   ├── clean.py
│   ├── validate.py
│   └── publish.py
│
├── processors/
│   ├── header_detection.py
│   ├── field_mapping.py
│   ├── order_cleaning.py
│   └── date_cleaning.py
│
└── adapters/
    ├── csv_io.py
    ├── excel_io.py
    ├── dictionary_io.py
    └── filesystem.py
```

这仍然是本地、单进程、顺序执行的 CLI 批处理应用，不是服务化架构。

## 7.2 核心契约

### RunContext

```python
@dataclass
class RunContext:
    run_id: str
    config: AppConfig
    artifacts: ArtifactStore
    logger: logging.Logger
    interaction: InteractionPolicy
```

### Artifact

```python
@dataclass
class Artifact:
    kind: str
    path: Path
    stage: str
    source_artifacts: list[str]
    row_count: int | None
    checksum: str | None
    metadata: dict
```

### StageResult

```python
@dataclass
class StageResult:
    stage: str
    status: StageStatus
    inputs: list[Artifact]
    outputs: list[Artifact]
    metrics: dict[str, int | float]
    warnings: list[str]
    errors: list[str]
```

核心变化是：

> Stage 不再只是产生副作用并打印信息，而是显式返回状态、指标和工件。

## 7.3 Stage 注册

不需要动态插件系统。简单显式列表即可：

```python
PIPELINE_STAGES = [
    IntakeStage(),
    PreprocessStage(),
    ConvertStage(),
    HeaderStage(),
    ProfileStage(),
    MappingStage(),
    ExtractStage(),
    CleanStage(),
    ValidateStage(),
    PublishStage(),
]
```

CLI 单步执行和完整流水线都从这份注册表中调用同一个 Stage。

---

## 8. 推荐的业务架构

建议将业务流程明确为：

```text
1. 入场 Intake
2. 预处理 Preprocess
3. 转换 Convert
4. 结构标准化 Header Normalize
5. 数据探查 Profile
6. 字段映射 Mapping
7. 投影与合并 Project/Merge
8. 值标准化 Standardize
9. 业务校验 Validate
10. 发布 Publish
11. 审计与回放 Audit/Replay
```

## 8.1 入场 Intake

输入：待处理 Excel 文件。

输出：

```text
input_inventory.json
```

至少记录：

- 文件名和相对路径；
- 文件大小和修改时间；
- SHA-256；
- sheet 列表；
- run_id；
- 是否为临时文件；
- 配置版本。

目的：明确回答“本次到底处理了哪些输入”。

## 8.2 预处理 Preprocess

输入：原始或转换失败的 Excel 工件。

输出：

```text
preprocessed Excel
excel_error_audit.csv
repair_manifest.json
```

必须记录所有发生变化的单元格。

## 8.3 转换 Convert

输入：Excel 工件。

输出：

```text
converted CSV
failed_xlsx.csv
conversion_manifest.json
```

转换成功与失败产物必须物理隔离。

## 8.4 Header 标准化

输入：转换成功的 CSV。

输出：

```text
header-normalized CSV
header_detection_report.csv
```

报告应包含未找到表头、空文件、编码、表头行号和 ragged rows 等信息。

## 8.5 数据探查 Profile

当前字段统计建议扩展为：

```text
source_file
column_name
row_count
non_null_count
null_rate
unique_count
sample_values
inferred_type
```

输出：

```text
field_profile.csv
field_summary.csv
```

## 8.6 字段映射 Mapping

继续保留 Excel 字典作为人工控制点，但增加：

```text
mapping_report.csv
unmapped_fields.csv
mapping_conflicts.csv
dictionary_snapshot.xlsx
```

字典路径、sheet 和列定义应移入配置，不再硬编码。

## 8.7 投影与合并 Project/Merge

输入：字段已标准化的文件。

输出：

```text
merge_raw.csv
merge_manifest.json
```

每行保留 source lineage。迁移期可同时写出兼容的 `merge.csv`。

## 8.8 值标准化 Standardize

负责运单号、日期等值的格式规范，不负责静默删除数据。

输出：

```text
merge_standardized.csv
changed_values.csv
```

## 8.9 业务校验 Validate

负责：

- 必填字段；
- 运单号规则；
- 日期解析状态；
- 重复记录；
- 字段完整率；
- 行级有效性。

输出：

```text
merge_validated.csv
invalid_rows.csv
validation_report.csv
quality_summary.csv
```

## 8.10 发布 Publish

输入：通过质量门的数据。

输出：

```text
merge_final.csv
schema.json
run_summary.json
```

只有该阶段可以替换正式产物。

## 8.11 审计与回放 Audit/Replay

每次运行建议保存：

```text
runs/<run_id>/
├── run_manifest.json
├── config_snapshot.yaml
├── dictionary_snapshot.xlsx
├── stages/
│   ├── intake.json
│   ├── convert.json
│   ├── mapping.json
│   └── validate.json
└── audit/
    ├── failed_files.csv
    ├── rejected_rows.csv
    └── changed_values.csv
```

初期无需数据库，JSON/CSV 足以支持审计和重放。

---

## 9. 渐进式实施路线

## 9.1 第一阶段：安全止血

目标：不改变主要 CLI 使用方式，先消除数据损坏和数据丢失风险。

1. 为目录清理增加工作空间边界和危险路径拒绝。
2. 标识字段全程按字符串读取和写入。
3. 输出改为 staging + 原子发布。
4. invalid rows 写入 rejected 文件。
5. 转换失败产物不进入成功目录。
6. 修复 ragged CSV 行处理。
7. 修复日期输出格式和别名冲突。
8. 补充上述场景的回归测试。

## 9.2 第二阶段：统一执行模型

目标：消除 CLI 和 Pipeline 行为漂移。

1. 增加统一 `AppConfig`。
2. 将每个业务阶段封装为共享 Stage。
3. 引入 `StageResult`、`Artifact` 和 `RunContext`。
4. CLI 与完整流水线调用同一 Stage。
5. 统一日志、异常和指标汇总。
6. 删除或正式接入 `StateManager` 和 `DataQualityChecker`。

## 9.3 第三阶段：质量与审计体系

1. 增加输入清单和文件哈希。
2. 增加字段画像。
3. 增加映射冲突和未映射报告。
4. 增加行级拒绝原因。
5. 增加质量阈值和发布门。
6. 增加 run/stage manifest。
7. 保存配置和字典快照。

## 9.4 第四阶段：性能与工程化

1. Header 只读取必要行。
2. 字段提取只读取表头。
3. 内容合并和清洗支持 chunksize。
4. 迁移为标准 Python 包结构。
5. 增加 CI、Ruff、类型检查和覆盖率。
6. 建立 Python 3.10–3.13 兼容矩阵。

---

## 10. 暂时不建议引入

当前阶段不建议引入：

- 微服务；
- REST API；
- Airflow、Dagster、Prefect；
- 数据库状态管理；
- 动态插件系统；
- Spark；
- Kubernetes；
- 复杂 DDD；
- LLM 自动字段映射。

这些技术不能解决当前最紧迫的路径安全、标识字段保真、产物原子性和审计问题，反而会增加维护成本。

如果以后出现以下明确需求，再重新评估：

- 多用户远程提交任务；
- 跨机器执行；
- 定时调度和复杂依赖；
- 单文件数 GB 以上；
- 多租户和权限管理；
- 集中式运行历史查询。

---

## 11. 优先级清单

如果第一轮只实施五项，建议按以下顺序：

1. **安全路径清理边界**
2. **运单号等标识字段全程字符串保真**
3. **staging + 原子输出发布**
4. **清洗与校验分离，并输出 rejected rows**
5. **统一 Config + StageResult，使 CLI 和 Pipeline 共用实现**

完成以上五项后，项目将从“能运行的数据脚本”提升为“可安全重复运行、可追溯的数据流水线工具”。

---

## 12. 验证基线

审查时的测试基线：

```text
uv run python -m pytest
62 passed, 3 warnings
```

三条 warning 来自 pandas 无法自动推断部分日期格式时退回逐项解析。后续应通过明确格式优先级和歧义处理减少警告，但它们目前不影响测试通过。

审查期间还以最小示例确认了以下问题：

- Header 检测对缺列/多列数据行抛出 `ValueError`；
- 内容提取在后续失败前已删除旧输出；
- `StateManager` 重载后整数键转为字符串并导致追加记录丢失；
- 大写 `.XLSX` 文件被静默忽略；
- 日期配置中的日期专用输出格式没有应用。
