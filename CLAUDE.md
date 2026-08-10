# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development commands

The project requires Python 3.10+ and uses `uv`; dependencies are locked in `uv.lock`.

```bash
# Install/sync dependencies
uv sync

# Run the full test suite
uv run python -m pytest

# Run one test file
uv run python -m pytest tests/test_cli_date_clean.py

# Run one test
uv run python -m pytest tests/test_cli_date_clean.py::test_cli_columns_override_passed

# Run the full data pipeline
uv run cli.py pipeline

# Use an alternate pipeline configuration (the global option precedes the subcommand)
uv run cli.py --config path/to/pipeline.yaml pipeline
```

There is no configured build, lint, formatting, or static type-check command in `pyproject.toml`.

Useful individual pipeline commands:

```bash
uv run cli.py xlsx-to-csv                   # Convert all configured workbooks
uv run cli.py xlsx-to-csv --mode failed     # Retry files in the failed-file list
uv run cli.py xlsx-to-csv --preprocess      # Preprocess formulas/errors first
uv run cli.py flatten
uv run cli.py find-header
uv run cli.py extract-fields
uv run cli.py array-agg
uv run cli.py field-replace
uv run cli.py extract-content --columns "单号,客户名称" --merge
uv run cli.py field-clean
uv run cli.py date-clean
uv run cli.py date-clean --columns "创建时间,结算日期"
```

## Architecture

DataRefinery is a config-driven batch processor for heterogeneous Excel/CSV files. `cli.py` is the primary entry point: it either invokes one processor directly or constructs `core.pipeline.DataPipeline`. `main.py` is a minimal secondary entry point that only runs the full pipeline.

`DataPipeline` owns the ordered workflow:

1. Convert Excel workbooks to CSV.
2. Flatten per-workbook output directories.
3. Detect and normalize header rows.
4. Extract field metadata to `mid_files/field_info.csv`.
5. Aggregate field occurrences to `mid_files/agg.csv`.
6. Rename source fields through `config/dict_zh.xlsx` (sheet `dict`, columns `new_field`, `priority`, and `old_field`).
7. Select requested content and optionally merge it into `Result_files/merge.csv`.
8. Clean configured order/tracking fields into `Result_files/merge_cleaned.csv`.
9. Normalize configured date fields, preferring and updating `merge_cleaned.csv`; if it does not exist, read `merge.csv` and create it.

The implementation is split by responsibility:

- `core/pipeline.py`: sequencing, retry/fallback behavior, optional-step handling, logging, and interactive continuation gates.
- `processors/`: one module per transformation. Processors accept paths/configuration and perform the actual pandas/openpyxl/xlsx2csv work.
- `utils/`: shared logging, date parsing, and directory-policy helpers.
- `config/pipeline.yaml`: source/output paths, directory reset policies, header/order rules, retry/preprocessing behavior, performance settings, and logging.
- `config/date_formats.yaml`: date aliases, accepted formats, normalized output formats, and parse-failure policy.
- `config/dict_zh.xlsx`: editable field-name mapping used between field aggregation and content extraction.

## Pipeline behavior and data safety

- Input workbooks belong in the configured `paths.excel_folder` (normally `excel_folder/`). Generated data flows through `csv_results_folder/` and `mid_files/` to `Result_files/`; logs go to `logs/`.
- A full pipeline run applies `dir_policies` before processing. For every path whose policy is `true`, `utils.path_manager.ensure_dir` recursively deletes and recreates the directory. The checked-in configuration preserves `excel_folder` but clears generated/intermediate/result directories. Inspect the active config before running the pipeline against valuable data.
- In `retry_mode: failed`, the pipeline disables those directory clears so prior conversion outputs remain available.
- Conversion scans generated CSV values for configured `failure_markers`. Failed workbooks are recorded in `mid_files/failed_xlsx.csv`; the pipeline can preprocess only those workbooks, write an audit log, retry them, and stop downstream work if failures remain.
- The default config enables interactive gates after conversion/repair and after field aggregation so a user can inspect results or edit `dict_zh.xlsx`. It also leaves `content_extraction.columns` empty, causing the full pipeline to prompt for fields. Full-pipeline runs are therefore not unattended by default.
- Required pipeline steps re-raise errors. Field replacement and date cleaning are optional steps: failures are logged and the pipeline continues.
- Most generated output directories are gitignored. Tests should use pytest's `tmp_path`/monkeypatch patterns rather than repository data directories.
