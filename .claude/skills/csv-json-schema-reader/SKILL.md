---
name: csv-json-schema-reader
description: Use when the user wants a Python script that infers and reads the schema of CSV and JSON data files in the us-healthcare-de project, driven by config/datasets.yaml. Generates the script once and reuses it.
---

# CSV + JSON Schema Reader (config-driven)

This skill produces ONE reusable Python script that scans raw CSV and JSON files,
applies per-dataset settings from `config/datasets.yaml`, infers each file's schema,
saves it, reads the file using that schema, and compares CSV vs JSON versions of
the same dataset. Datasets are processed in dependency order.

The script is generated ONCE. Every later use of this skill reuses the existing script.
Adding a new dataset means adding an entry to `config/datasets.yaml`, not changing the script.

## Step 0 - Check first (mandatory)

1. Look for `src/common/schema_reader.py`.
2. If it EXISTS:
   - Do NOT create, overwrite or regenerate it.
   - Do NOT create copies with new names (no `schema_reader_v2.py`, no timestamped copies).
   - Tell the user it already exists and give the run command:
     `python src/common/schema_reader.py`
   - Only edit it if the user explicitly says "regenerate", "update the script",
     "change the script" or "sync the script with the skill". Then:
     - Read this SKILL.md and the existing script, and list every rule the script does not yet follow
       (check the Change log at the end of this file first).
     - Edit the existing file in place. Never create a new file or a copy.
     - Keep the engine choice already recorded in the script's header docstring. Do not ask again.
     - Summarise what changed, and give the run command so the user can re-run it.
3. If it does NOT exist, continue to Step 1.
4. Never edit `config/datasets.yaml` unless the user asks. It is the user's file.
   If it is missing, tell the user; do not invent dataset entries.

## Step 1 - Ask one question (only when generating from scratch)

Ask once: "Should the script use pandas (simple, runs anywhere) or PySpark (matches
AWS Glue, needs Java + PySpark locally)?" Default to pandas if there's no preference.
Record the choice in the script's header docstring.

## Step 2 - Use the existing project structure

Never delete, rename or move existing folders or files.

```
us-healthcare-de/
  config/
    datasets.yaml    <- user-maintained dataset settings (read-only for the script)
    logs/            <- existing; script writes run logs here
    schemas/         <- inferred schema and comparison files go here
  data/
    raw/
      csv/           <- user drops .csv files here
      json/          <- user drops .json files here
    test_out/        <- existing; old trial files may be parked here (never scanned)
  src/
    common/
      db_config.py   <- existing; do not touch
      schema_reader.py  <- the script this skill generates
```

Create only missing folders, and tell the user which were created and which already existed.

## Step 3 - The script `src/common/schema_reader.py`

### Dependencies
- pandas (or PySpark if chosen) and PyYAML (`pip install pyyaml`). Use `yaml.safe_load` only.

### Inputs
- Scan `data/raw/csv/` for `*.csv` and `data/raw/json/` for `*.json`. Never scan `data/test_out/`.
- Command-line options: override folders, `--file <path>` for a single file,
  `--dataset <name>` to process one configured dataset, `--config <path>`
  (default `config/datasets.yaml`), `--preview`, `--preview-columns`.
- Build every path from the project root with `pathlib`. No hardcoded absolute paths.
- Open every file with encoding `utf-8-sig` (UTF-8 that also strips the invisible BOM Excel adds
  when saving "CSV UTF-8"; otherwise the first header becomes `﻿Account ID` and won't match).
  Values and headers contain accents and en dashes (–); they must survive.
- If a file is not valid UTF-8 (e.g. saved by Excel as ANSI), do NOT guess another encoding.
  Log a clear error naming the file and suggesting: re-save it as "CSV UTF-8", then continue with other files.
- Read CSVs with a real CSV parser so quoted values containing commas (`"$250,000.00"`,
  `"Jumbo Group (1,000+)"`) stay in one column.

### Loading `config/datasets.yaml`
- Load once at start. If the file is missing, log a WARNING and run with built-in generic rules
  (the same defaults shown in the sample config). If it is malformed, stop with a clear error
  naming the problem. Never silently ignore a broken config.
- Validate each dataset entry and log a clear error for: missing `file_stem`, duplicate `file_stem`,
  duplicate `load_order`, `depends_on` naming an unknown dataset, an unknown type in `column_types`,
  or a rule (column_types, sensitive_columns, preview_columns) naming a column that is not a
  `header_map` value. Skip only the invalid dataset; continue with the rest.
- No dataset-specific column lists may be hardcoded in the script. All of them come from the config.

### Matching files to datasets
- A file belongs to a dataset when its file stem equals the dataset's `file_stem`
  (`raw_account.csv` and `raw_account.json` -> dataset `account`).
- Files that match no dataset are still processed with the `defaults` rules only, after all
  configured datasets, and logged as `unconfigured`.

### Processing order and dependency check
- Process configured datasets in ascending `load_order`, then unconfigured files.
- For each dataset, check that every dataset in `depends_on` has at least one file (CSV or JSON)
  in this run. If not, log a WARNING such as
  `member depends on subscription, but no subscription file was found` and still process it.
- The `relationships` section is documentation only. Do NOT check key values across files;
  that belongs to the Silver layer.

### Header standardization (before anything else)
- Compare raw headers to `header_map` keys after trimming spaces, ignoring case, and treating
  en dash (–), em dash (—) and hyphen (-) as the same character.
- Mapped header -> rename to the standardized name.
- Unmapped header in a configured dataset -> convert to UPPER_SNAKE automatically
  (letters/digits only, other characters become `_`, no repeated or trailing `_`) and log a WARNING.
- `header_map` key not found in the file -> log a WARNING (possible missing or renamed column).
- Two headers ending up with the same name -> stop processing that file with a clear error.
- Unconfigured files: convert every header to UPPER_SNAKE the same way.

### JSON layout detection
Detect and log the layout before reading:
- JSON Lines (one object per line) - the default for this project
- JSON array `[ {...}, {...} ]`
- Single JSON object - treat as one record

### Type rules (apply in this order, using standardized names)
1. Empty CSV strings and JSON `null` both count as null.
2. A type declared in the dataset's `column_types` always wins. Supported types:
   - `string`, `date` (`YYYY-MM-DD`), `integer`, `decimal`
   - `percent`: strip `%`, divide by 100 -> decimal (`80%` -> `0.80`; `0%` -> `0.00`)
   - `currency`: strip `$` and thousands commas -> decimal (`$250,000.00` -> `250000.00`)
3. A column that's null in every row -> `string`, flag `all_null: true`.
4. Columns in `defaults.force_string_columns`, or ending in a `defaults.force_string_suffixes`
   suffix -> `string`. Leading zeros must never be lost.
5. All non-null values matching `YYYY-MM-DD` -> `date`. `9999-12-31` is a valid open-ended high date.
   Future dates (e.g. a pending account starting next year) are valid.
6. Only `Y`/`N` values -> keep as `string`, flag `boolean_candidate: true`. Do not convert.
7. Whole numbers without leading zeros -> `integer`; decimals -> `decimal`.
8. Everything else -> `string`.
9. Mixed types in one column -> `string`, flag `mixed_types: true`.

Spark type names for each column: `StringType`, `DateType`, `IntegerType`,
`DecimalType(18,2)` for decimal and currency, `DecimalType(5,4)` for percent.

### Paths written to outputs (schema files, comparison files, logs, console)
- Always project-relative with forward slashes, e.g. `data/raw/csv/raw_account.csv`.
- Use `relative_to(PROJECT_ROOT)` then `as_posix()`.
- Never write backslashes or absolute paths into any output. If a `--file` is outside the project,
  use `as_posix()` of the absolute path and log a warning.

### Schema output
For each input file, write `config/schemas/<file_stem>_<csv|json>_schema.json` with:
- dataset name (or `unconfigured`), config version, source file (relative, forward slashes),
  format, JSON layout (if JSON), row count, column count, run timestamp
- per column: standardized `name`, original `source_header`, position, inferred_type, spark_type,
  `type_source` (`config` or `inferred`), nullable, null_count, flags, `sample_values` and `raw_sample_values`
- `sample_values`: up to 3 values AFTER type conversion, as text (`250000.00`, `0.8000`, `2019-01-01`).
- `raw_sample_values`: the same rows' values exactly as they appeared in the file (`$250,000.00`, `80%`).
  Take both from the same rows so they line up. Both lists are masked for sensitive columns.

Masking: columns in `defaults.sensitive_columns` plus the dataset's own `sensitive_columns`
show only the last `mask_visible_chars` characters, e.g. `*********67`. Apply this to schema samples,
the preview and every log message.

Re-running the script overwrites these schema files (they're outputs). It never creates new scripts.

### Read the file using the schema
- Read every column as string first, then cast to the final types.
- Log rows that fail a cast (column, row number, value masked if sensitive), e.g. a percent column
  containing `eighty`. Count failures per column in the schema file as `cast_failures`.
- Print the row count, then the preview.

### Console preview (must stay readable for wide files)
Never print all columns side by side. `--preview` modes:
1. `vertical` (default): the FIRST record, one column per line, names left-aligned to a fixed width,
   inferred type in brackets:
   ```
   --- raw_account.csv (account) : record 1 of 15 ---
   ACCOUNT_HCC_ID           : ACC100245          (string)
   EMPLOYER_CONTRIB_PCT_EE  : <null>             (percent)
   ```
2. `keys`: the first 5 rows as a table using only key columns, taken from `--preview-columns` if given,
   else the dataset's `preview_columns`, else the first 5 columns. Ignore and warn about missing names.
3. `none`: no preview.
Show nulls as `<null>`. Mask sensitive columns. Do not change pandas' global display settings.
Keep columns aligned: in `vertical` mode, values longer than 30 characters are cut to 27 characters
plus `...`, so the `(type)` column always lines up; in `keys` mode, cut values longer than 25 characters
the same way. Shortening applies to the console only, never to schema files or data.

### CSV vs JSON comparison
When a CSV and a JSON file share a file stem, compare them using standardized column names and report:
columns present in only one file, columns whose inferred type differs, and row count difference.
Write `config/schemas/<file_stem>_comparison.json` and print a short summary.

### Privacy check (runs last, after all files)
Purpose: produce evidence that no sensitive value leaked into any output.
- Sensitive columns = `defaults.sensitive_columns` plus each dataset's own `sensitive_columns`.
- While reading each file, collect the distinct non-null raw values of its sensitive columns in memory only.
  Ignore values shorter than 4 characters (too short to check meaningfully). Never write this collection anywhere.
- After all files are processed and the log handlers are flushed, scan every output written in this run:
  all schema files, all comparison files, and this run's log file. Count any exact occurrence of a
  collected raw value.
- Write `config/schemas/privacy_check.json` (overwritten each run) containing:
  - run timestamp, config version, overall `status`: `PASS` (0 leaks) or `FAIL` (1 or more)
  - `files_scanned`: list of output paths (project-relative, forward slashes)
  - `sensitive_columns_checked`: per dataset, the column names and how many distinct values were checked
  - `leaks`: for each leak, the output file and the column name. NEVER the value itself.
- This file must not contain any sensitive value, masked or unmasked.
- Add the result to the final summary line, e.g. `privacy check=PASS`. On FAIL, log an ERROR naming the
  file and column and exit with a non-zero exit code after writing all outputs.

### Code standards
- Small functions with docstrings, at least: `load_config`, `validate_config`, `match_dataset`,
  `standardize_headers`, `detect_json_layout`, `infer_schema`, `apply_types`, `write_schema`,
  `read_with_schema`, `print_preview`, `compare_schemas`, `check_dependencies`, `run_privacy_check`, `main`.
- Use `logging` (console + `config/logs/schema_reader_<date>.log`), not bare prints for status.
- `if __name__ == "__main__":` entry point.
- If one file fails, log it and carry on with the others.
- End with a summary: datasets processed in order, files processed, files failed, unconfigured files,
  dependency warnings, schemas written, privacy check result.

## Step 4 - Hand over

- Do NOT run the script. The user runs it in VS Code.
- Tell the user:
  1. where the script is,
  2. that `config/datasets.yaml` must exist (and that adding a dataset = adding an entry there),
  3. to put files in `data/raw/csv/` and `data/raw/json/`, named `<file_stem>.csv` / `.json`,
  4. the run command: `python src/common/schema_reader.py` (project root, .venv active), plus examples:
     `--dataset account`, `--preview keys`,
  5. packages to install: `pip install pandas pyyaml` (or PySpark if chosen).

## Never
- Never regenerate or duplicate the script when it already exists.
- Never run the script unless the user asks.
- Never modify or move files in `data/raw/`, and never edit `config/datasets.yaml` unless asked.
- Never hardcode dataset-specific column lists in the script.
- Never print or write unmasked sensitive values.
- Never write backslash or absolute paths into output files.
- Never check key values across datasets (that's Silver-layer validation).

## Change log
Use this to see which rules an existing script may be missing when asked to update or sync it.

- v1 (2026-11-20): first version - schema inference, CSV vs JSON comparison, masking, logging.
- v2 (2026-11-22):
  - Paths in all outputs are project-relative with forward slashes (`relative_to` + `as_posix`).
  - The console preview uses `--preview` modes `vertical` (default), `keys` and `none`, with
    `--preview-columns`; nulls are shown as `<null>`.
- v3 (2026-11-22):
  - Config-driven via `config/datasets.yaml` (PyYAML, `safe_load`); config validation.
  - Moved hardcoded sensitive and force-string lists out of the script into the config `defaults`.
  - Header standardization (`header_map`, dash/case-insensitive matching, UPPER_SNAKE fallback).
  - Declared `column_types`, including new `percent` and `currency` types.
  - Dataset matching by `file_stem`; processing in `load_order`; `depends_on` warnings.
  - New CLI options `--dataset` and `--config`; `data/test_out/` is never scanned.
  - Schema files gain `dataset`, `source_header`, `type_source` and `cast_failures`.
  - Files are read as `utf-8-sig` (BOM-safe); non-UTF-8 files fail with a clear "re-save as CSV UTF-8" error.
- v3.1 (2026-11-22):
  - Privacy check after every run: scans all outputs for unmasked sensitive values and writes
    `config/schemas/privacy_check.json` (PASS/FAIL, never the values); result in the summary line.
  - Schema files: `sample_values` now hold converted values; new `raw_sample_values` hold the original text.
  - Preview: long values are shortened with `...` so the type column stays aligned (console only).
