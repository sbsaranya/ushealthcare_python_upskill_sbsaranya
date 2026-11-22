"""CSV + JSON schema reader for the us-healthcare-de project (config-driven).

Engine: pandas (chosen by user).
Skill version: v3.1 (see .claude/skills/csv-json-schema-reader/SKILL.md, Change log).

Scans data/raw/csv/*.csv and data/raw/json/*.json, applies per-dataset settings from
config/datasets.yaml (header mapping, declared types, sensitive columns, load order),
infers each file's schema, writes it to config/schemas/, reads the file using that
schema, and compares CSV vs JSON versions of the same dataset (same file stem).

Usage (from project root):
    python src/common/schema_reader.py
    python src/common/schema_reader.py --dataset account
    python src/common/schema_reader.py --file data/raw/csv/raw_account.csv
    python src/common/schema_reader.py --config config/datasets.yaml
    python src/common/schema_reader.py --csv-dir other/csv --json-dir other/json
    python src/common/schema_reader.py --preview keys --preview-columns ACCOUNT_HCC_ID,ACCOUNT_NAME
    python src/common/schema_reader.py --preview none

Preview modes: vertical (default, first record one column per line), keys (first 5 rows,
key columns only), none. Long values are shortened with '...' in the console only.

After all files are processed a privacy check scans every output of the run for unmasked
sensitive values and writes config/schemas/privacy_check.json (PASS/FAIL, never the values).

Requires: pip install pandas pyyaml
"""

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV_DIR = PROJECT_ROOT / "data" / "raw" / "csv"
DEFAULT_JSON_DIR = PROJECT_ROOT / "data" / "raw" / "json"
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "datasets.yaml"
SCHEMA_DIR = PROJECT_ROOT / "config" / "schemas"
LOG_DIR = PROJECT_ROOT / "config" / "logs"

ENCODING = "utf-8-sig"  # UTF-8 that also strips the BOM Excel adds to "CSV UTF-8"

VALID_TYPES = {"string", "date", "integer", "decimal", "percent", "currency"}

SPARK_TYPES = {
    "string": "StringType",
    "date": "DateType",
    "integer": "IntegerType",
    "decimal": "DecimalType(18,2)",
    "currency": "DecimalType(18,2)",
    "percent": "DecimalType(5,4)",
}

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
INT_RE = re.compile(r"^-?(0|[1-9]\d*)$")
DEC_RE = re.compile(r"^-?\d+\.\d+$")
CURRENCY_RE = re.compile(r"^-?\$?-?\d{1,3}(,\d{3})*(\.\d+)?$|^-?\$?-?\d+(\.\d+)?$")
PERCENT_RE = re.compile(r"^-?\d+(\.\d+)?%$")

# Used only when config/datasets.yaml is missing (same as the sample config defaults).
BUILTIN_DEFAULTS = {
    "force_string_suffixes": ["_ID", "_CODE", "_HCC_ID"],
    "force_string_columns": ["SSN", "ZIP_CODE", "PHONE_NUMBER"],
    "sensitive_columns": [
        "SSN", "MEDICARE_BENEFICIARY_ID", "MEDICAID_ID",
        "EMAIL_ADDRESS", "PHONE_NUMBER", "DATE_OF_BIRTH", "TAX_ID_EIN",
    ],
    "mask_visible_chars": 2,
}

MIN_PRIVACY_LEN = 4        # sensitive values shorter than this are not checked
VERTICAL_MAX = 30          # console shortening limits (console only)
KEYS_MAX = 25

log = logging.getLogger("schema_reader")
LOG_FILE = LOG_DIR / f"schema_reader_{datetime.now():%Y-%m-%d}.log"


@dataclass
class Rules:
    """Effective rules for one file: defaults merged with its dataset's settings."""

    dataset: str = "unconfigured"
    config_version: object = None
    header_map: dict = field(default_factory=dict)       # raw header -> standardized name
    column_types: dict = field(default_factory=dict)     # standardized name -> declared type
    sensitive: set = field(default_factory=set)
    force_columns: set = field(default_factory=set)
    force_suffixes: tuple = ()
    visible: int = 2
    preview_columns: list = field(default_factory=list)
    configured: bool = False


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def setup_logging() -> None:
    """Log to console and to config/logs/schema_reader_<date>.log."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    for handler in (
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ):
        handler.setFormatter(fmt)
        log.addHandler(handler)


def rel_path(path: Path) -> str:
    """Project-relative path with forward slashes (absolute posix path if outside the project)."""
    path = Path(path).resolve()
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        log.warning("Path is outside the project root: %s", path.name)
        return path.as_posix()


def mask(value, visible: int) -> str:
    """Mask all but the last `visible` characters of a value."""
    text = str(value)
    if visible <= 0 or len(text) <= visible:
        return "*" * len(text)
    return "*" * (len(text) - visible) + text[-visible:]


def display(column: str, value, rules: Rules):
    """Return a value safe for printing/logging (masked if the column is sensitive)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return value
    return mask(value, rules.visible) if column in rules.sensitive else value


def is_forced_string(column: str, rules: Rules) -> bool:
    """ID/code columns always stay string so leading zeros survive."""
    return column in rules.force_columns or column.endswith(rules.force_suffixes)


def to_text(value):
    """Normalise a raw value to a stripped string, or None for null."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, bool):
        return str(value).lower()
    text = str(value).strip()
    return text if text != "" else None


def norm_header(header: str) -> str:
    """Normalise a header for matching: trim, lower-case, en/em dash -> hyphen."""
    return str(header).strip().lower().replace("–", "-").replace("—", "-")


def upper_snake(header: str) -> str:
    """Convert a header to UPPER_SNAKE (letters/digits only, no repeated or trailing '_')."""
    return re.sub(r"[^A-Za-z0-9]+", "_", str(header)).strip("_").upper()


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
def load_config(path: Path) -> dict:
    """Load config/datasets.yaml with safe_load.

    Missing file -> WARNING and built-in defaults. Malformed file -> SystemExit.
    """
    if not path.exists():
        log.warning("Config not found (%s); running with built-in generic rules.", rel_path(path))
        return {"version": None, "defaults": dict(BUILTIN_DEFAULTS), "datasets": {}}
    try:
        with open(path, "r", encoding=ENCODING) as fh:
            cfg = yaml.safe_load(fh)
    except (yaml.YAMLError, UnicodeDecodeError) as exc:
        raise SystemExit(f"ERROR: config {rel_path(path)} is malformed: {exc}")
    if cfg is None:
        cfg = {}
    if not isinstance(cfg, dict):
        raise SystemExit(f"ERROR: config {rel_path(path)} must be a mapping at the top level.")
    if not isinstance(cfg.get("datasets") or {}, dict):
        raise SystemExit(f"ERROR: config {rel_path(path)}: 'datasets' must be a mapping.")
    if not isinstance(cfg.get("defaults") or {}, dict):
        raise SystemExit(f"ERROR: config {rel_path(path)}: 'defaults' must be a mapping.")
    cfg["defaults"] = {**BUILTIN_DEFAULTS, **(cfg.get("defaults") or {})}
    cfg["datasets"] = cfg.get("datasets") or {}
    log.info("Config loaded: %s (version %s, %d datasets)",
             rel_path(path), cfg.get("version"), len(cfg["datasets"]))
    return cfg


def validate_config(cfg: dict) -> dict:
    """Validate each dataset entry; log errors and return only the valid datasets by name."""
    all_names = set(cfg["datasets"])
    valid, seen_stems, seen_orders = {}, {}, {}
    for name, ds in cfg["datasets"].items():
        problems = []
        if not isinstance(ds, dict):
            log.error("Dataset '%s' skipped: entry must be a mapping.", name)
            continue
        stem = ds.get("file_stem")
        if not stem:
            problems.append("missing file_stem")
        elif stem in seen_stems:
            problems.append(f"duplicate file_stem '{stem}' (already used by '{seen_stems[stem]}')")
        order = ds.get("load_order")
        if order is not None and order in seen_orders:
            problems.append(f"duplicate load_order {order} (already used by '{seen_orders[order]}')")
        for dep in ds.get("depends_on") or []:
            if dep not in all_names:
                problems.append(f"depends_on names unknown dataset '{dep}'")

        header_map = ds.get("header_map") or {}
        standard_names = set(header_map.values())
        for col, typ in (ds.get("column_types") or {}).items():
            if typ not in VALID_TYPES:
                problems.append(f"column_types: unknown type '{typ}' for {col}")
        if header_map:
            rule_columns = {
                "column_types": list((ds.get("column_types") or {}).keys()),
                "sensitive_columns": list(ds.get("sensitive_columns") or []),
                "preview_columns": list(ds.get("preview_columns") or []),
            }
            for rule, cols in rule_columns.items():
                for col in cols:
                    if col not in standard_names:
                        problems.append(f"{rule}: '{col}' is not a header_map value")

        if problems:
            for problem in problems:
                log.error("Dataset '%s' skipped: %s", name, problem)
            continue
        seen_stems[stem] = name
        if order is not None:
            seen_orders[order] = name
        valid[name] = ds
    return valid


def build_rules(cfg: dict, name: str = None, ds: dict = None) -> Rules:
    """Merge config defaults with a dataset's settings (ds=None -> defaults only)."""
    d = cfg["defaults"]
    sensitive = set(d.get("sensitive_columns") or [])
    rules = Rules(
        config_version=cfg.get("version"),
        sensitive=sensitive,
        force_columns=set(d.get("force_string_columns") or []),
        force_suffixes=tuple(d.get("force_string_suffixes") or []),
        visible=int(d.get("mask_visible_chars", 2)),
    )
    if ds is not None:
        rules.dataset = name
        rules.configured = True
        rules.header_map = dict(ds.get("header_map") or {})
        rules.column_types = dict(ds.get("column_types") or {})
        rules.sensitive |= set(ds.get("sensitive_columns") or [])
        rules.preview_columns = list(ds.get("preview_columns") or [])
    return rules


def match_dataset(stem: str, datasets: dict):
    """Return (name, entry) of the dataset whose file_stem equals `stem`, else (None, None)."""
    for name, ds in datasets.items():
        if ds.get("file_stem") == stem:
            return name, ds
    return None, None


def check_dependencies(name: str, ds: dict, stems_found: set, stem_of: dict) -> int:
    """Warn for each depends_on dataset with no file in this run. Returns warning count."""
    warnings = 0
    for dep in ds.get("depends_on") or []:
        if stem_of.get(dep) not in stems_found:
            log.warning("%s depends on %s, but no %s file was found", name, dep, dep)
            warnings += 1
    return warnings


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def detect_json_layout(path: Path) -> str:
    """Detect 'json_lines', 'json_array' or 'single_object'."""
    with open(path, "r", encoding=ENCODING) as fh:
        content = fh.read().strip()
    if not content:
        raise ValueError("JSON file is empty")
    if content.startswith("["):
        return "json_array"
    if content.startswith("{"):
        lines = [ln for ln in content.splitlines() if ln.strip()]
        if len(lines) > 1:
            try:
                json.loads(lines[0])
                json.loads(lines[-1])
                return "json_lines"
            except json.JSONDecodeError:
                pass
        try:
            json.loads(content)
            return "single_object"
        except json.JSONDecodeError:
            return "json_lines"
    raise ValueError("Unrecognised JSON layout")


def load_raw(path: Path, fmt: str):
    """Load a file as an all-string DataFrame. Returns (df, json_layout).

    A file that is not valid UTF-8 is reported with a re-save hint; no other
    encoding is guessed.
    """
    try:
        if fmt == "csv":
            df = pd.read_csv(path, dtype=str, keep_default_na=False,
                             encoding=ENCODING, encoding_errors="strict")
            df = df.map(lambda v: None if v is None or str(v).strip() == "" else v)
            return df, None

        layout = detect_json_layout(path)
        with open(path, "r", encoding=ENCODING) as fh:
            if layout == "json_lines":
                records = [json.loads(ln) for ln in fh if ln.strip()]
            elif layout == "json_array":
                records = json.load(fh)
            else:
                records = [json.load(fh)]
        log.info("%s: detected JSON layout = %s", path.name, layout)
        df = pd.DataFrame(records)
        df = df.astype(object).map(to_text)
        return df, layout
    except UnicodeDecodeError:
        raise ValueError(
            f"{path.name} is not valid UTF-8 (probably saved by Excel as ANSI). "
            'Re-save it as "CSV UTF-8" and run again.'
        )


def standardize_headers(df: pd.DataFrame, rules: Rules, file_name: str):
    """Rename raw headers to standardized names. Returns (df, {standard_name: source_header}).

    Mapped -> header_map value. Unmapped -> UPPER_SNAKE (+ WARNING when the dataset is
    configured). A header_map key missing from the file -> WARNING. Duplicate results -> ValueError.
    """
    lookup = {norm_header(k): v for k, v in rules.header_map.items()}
    standard_values = set(rules.header_map.values())
    new_names, found_keys = [], set()
    for header in df.columns:
        key = norm_header(header)
        if key in lookup:
            new_names.append(lookup[key])
            found_keys.add(key)
        elif header in standard_values:  # already standardized (e.g. JSON export)
            new_names.append(header)
            found_keys.add(norm_header(next(k for k, v in rules.header_map.items() if v == header)))
        else:
            snake = upper_snake(header)
            new_names.append(snake)
            if rules.configured:
                log.warning("%s: header '%s' is not in header_map; using %s", file_name, header, snake)

    for raw_key in rules.header_map:
        if norm_header(raw_key) not in found_keys:
            log.warning("%s: header_map column '%s' not found in file (missing or renamed?)",
                        file_name, raw_key)

    dupes = sorted({n for n in new_names if new_names.count(n) > 1})
    if dupes:
        raise ValueError(f"duplicate column names after standardizing headers: {', '.join(dupes)}")

    source = {new: str(old) for new, old in zip(new_names, df.columns)}
    out = df.copy()
    out.columns = new_names
    return out, source


# --------------------------------------------------------------------------- #
# Inference
# --------------------------------------------------------------------------- #
def infer_column_type(column: str, values: list, rules: Rules) -> tuple:
    """Infer (type, flags, type_source) for one column from its non-null string values."""
    flags = {}
    if not values:
        flags["all_null"] = True
    declared = rules.column_types.get(column)
    if declared:
        return declared, flags, "config"
    if not values:
        return "string", flags, "inferred"
    if is_forced_string(column, rules):
        return "string", flags, "inferred"
    if all(DATE_RE.match(v) for v in values):
        return "date", flags, "inferred"
    if set(values) <= {"Y", "N"}:
        return "string", {"boolean_candidate": True}, "inferred"
    if all(INT_RE.match(v) for v in values):
        return "integer", flags, "inferred"
    if all(INT_RE.match(v) or DEC_RE.match(v) for v in values):
        return "decimal", flags, "inferred"

    kinds = set()
    for v in values:
        if DATE_RE.match(v):
            kinds.add("date")
        elif INT_RE.match(v):
            kinds.add("integer")
        elif DEC_RE.match(v):
            kinds.add("decimal")
        else:
            kinds.add("string")
    if len(kinds) > 1:
        flags["mixed_types"] = True
    return "string", flags, "inferred"


def infer_schema(df: pd.DataFrame, path: Path, fmt: str, layout, rules: Rules,
                 source_headers: dict) -> dict:
    """Infer the schema dict for a DataFrame of string values (standardized column names)."""
    columns = []
    for pos, col in enumerate(df.columns, start=1):
        series = df[col]
        values = [v for v in series if v is not None and not (isinstance(v, float) and pd.isna(v))]
        inferred, flags, type_source = infer_column_type(col, values, rules)
        null_count = len(series) - len(values)
        columns.append({
            "name": col,
            "source_header": source_headers.get(col, col),
            "position": pos,
            "inferred_type": inferred,
            "spark_type": SPARK_TYPES[inferred],
            "type_source": type_source,
            "nullable": null_count > 0,
            "null_count": null_count,
            "cast_failures": 0,
            "flags": flags,
            "sample_values": [],       # filled by add_samples() after casting
            "raw_sample_values": [],
        })
    return {
        "dataset": rules.dataset,
        "config_version": rules.config_version,
        "source_file": rel_path(path),
        "format": fmt,
        "json_layout": layout,
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        "columns": columns,
    }


def write_schema(schema: dict, stem: str, fmt: str) -> Path:
    """Write the schema JSON to config/schemas/<stem>_<fmt>_schema.json."""
    SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
    out = SCHEMA_DIR / f"{stem}_{fmt}_schema.json"
    out.write_text(json.dumps(schema, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Schema written: %s", rel_path(out))
    return out


# --------------------------------------------------------------------------- #
# Reading with schema
# --------------------------------------------------------------------------- #
def cast_value(text: str, typ: str):
    """Cast one non-null string to `typ`. Raises ValueError on failure."""
    if typ == "string":
        return text
    if typ == "date":
        return date.fromisoformat(text) if DATE_RE.match(text) else _bad(text)
    if typ == "integer":
        return int(text) if INT_RE.match(text) else _bad(text)
    try:
        if typ == "decimal":
            return Decimal(text).quantize(Decimal("0.01"))
        if typ == "currency":
            if not CURRENCY_RE.match(text):
                _bad(text)
            return Decimal(text.replace("$", "").replace(",", "")).quantize(Decimal("0.01"))
        if typ == "percent":
            if not PERCENT_RE.match(text):
                _bad(text)
            return (Decimal(text[:-1]) / 100).quantize(Decimal("0.0001"))
    except InvalidOperation:
        _bad(text)
    raise ValueError(f"unknown type {typ}")


def _bad(text):
    """Raise a ValueError for a value that cannot be cast."""
    raise ValueError(text)


def apply_types(df: pd.DataFrame, schema: dict, rules: Rules) -> pd.DataFrame:
    """Cast the all-string DataFrame to the schema types, logging and counting failures."""
    typed = df.copy()
    for col in schema["columns"]:
        name, typ = col["name"], col["inferred_type"]
        if typ == "string":
            continue
        out, failures = [], 0
        for idx, raw in enumerate(df[name], start=1):
            if raw is None or (isinstance(raw, float) and pd.isna(raw)):
                out.append(None)
                continue
            try:
                out.append(cast_value(raw, typ))
            except ValueError:
                failures += 1
                out.append(None)
                log.warning("Cast failure: column=%s type=%s row=%d value=%s",
                            name, typ, idx, display(name, raw, rules))
        typed[name] = pd.Series(out, index=df.index, dtype="Int64" if typ == "integer" else object)
        col["cast_failures"] = failures
    return typed


def read_with_schema(df: pd.DataFrame, schema: dict, rules: Rules) -> pd.DataFrame:
    """Read the file using the schema: all columns start as string, then are cast."""
    return apply_types(df, schema, rules)


def add_samples(schema: dict, raw: pd.DataFrame, typed: pd.DataFrame, rules: Rules) -> None:
    """Fill sample_values (converted, as text) and raw_sample_values (as in the file).

    Both come from the same rows (first 3 distinct non-null raw values) and are masked
    for sensitive columns.
    """
    for col in schema["columns"]:
        name = col["name"]
        seen, converted, originals = set(), [], []
        for raw_val, typed_val in zip(raw[name], typed[name]):
            if raw_val is None or (isinstance(raw_val, float) and pd.isna(raw_val)):
                continue
            if raw_val in seen:
                continue
            seen.add(raw_val)
            if typed_val is None or typed_val is pd.NA:
                text = None
            elif isinstance(typed_val, date):
                text = typed_val.isoformat()
            else:
                text = str(typed_val)
            converted.append(display(name, text, rules) if text is not None else None)
            originals.append(display(name, raw_val, rules))
            if len(originals) == 3:
                break
        col["sample_values"] = converted
        col["raw_sample_values"] = originals


def collect_sensitive(raw: pd.DataFrame, rules: Rules) -> dict:
    """Distinct raw values of sensitive columns (in memory only, never written anywhere)."""
    found = {}
    for col in raw.columns:
        if col in rules.sensitive:
            vals = {v for v in raw[col] if isinstance(v, str) and len(v) >= MIN_PRIVACY_LEN}
            if vals:
                found[col] = vals
    return found


def preview_value(column: str, value, rules: Rules) -> str:
    """Format one value for the preview: masked if sensitive, '<null>' if missing."""
    if value is None or (not isinstance(value, (str, date, Decimal)) and pd.isna(value)):
        return "<null>"
    if isinstance(value, date):
        value = value.isoformat()
    return str(display(column, value, rules))


def shorten(text: str, limit: int) -> str:
    """Console only: cut text longer than `limit` to limit-3 characters plus '...'."""
    return text if len(text) <= limit else text[:limit - 3] + "..."


def print_preview(df: pd.DataFrame, name: str, schema: dict, rules: Rules,
                  mode: str = "vertical", preview_columns=None) -> None:
    """Print the row count, then a preview according to mode (vertical, keys, none)."""
    log.info("%s: %d rows", name, len(df))
    if mode == "none" or df.empty:
        return

    if mode == "keys":
        wanted = list(preview_columns or rules.preview_columns or [])
        cols = [c for c in wanted if c in df.columns]
        for missing in (c for c in wanted if c not in df.columns):
            log.warning("%s: preview column not found, ignored: %s", name, missing)
        if not cols:
            cols = list(df.columns[:5])
        rows = df.head(5)
        table = pd.DataFrame({c: [shorten(preview_value(c, v, rules), KEYS_MAX) for v in rows[c]]
                              for c in cols})
        log.info("%s: first %d rows (columns: %s)\n%s", name, len(rows), ", ".join(cols),
                 table.to_string(index=False))
        return

    types = {c["name"]: c["inferred_type"] for c in schema["columns"]}
    width = max(len(c) for c in df.columns)
    lines = [f"--- {name} ({schema['dataset']}) : record 1 of {len(df)} ---"]
    for col in df.columns:
        value = preview_value(col, df[col].iloc[0], rules)
        value = shorten(value, VERTICAL_MAX)
        lines.append(f"{col:<{width}} : {value:<{VERTICAL_MAX}} ({types[col]})")
    log.info("\n%s", "\n".join(lines))


# --------------------------------------------------------------------------- #
# Comparison
# --------------------------------------------------------------------------- #
def compare_schemas(csv_schema: dict, json_schema: dict, stem: str) -> dict:
    """Compare CSV and JSON schemas of the same dataset and write the result."""
    c = {col["name"]: col["inferred_type"] for col in csv_schema["columns"]}
    j = {col["name"]: col["inferred_type"] for col in json_schema["columns"]}
    result = {
        "dataset": csv_schema.get("dataset", stem),
        "file_stem": stem,
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        "columns_only_in_csv": sorted(set(c) - set(j)),
        "columns_only_in_json": sorted(set(j) - set(c)),
        "type_differences": {
            k: {"csv": c[k], "json": j[k]} for k in sorted(set(c) & set(j)) if c[k] != j[k]
        },
        "csv_row_count": csv_schema["row_count"],
        "json_row_count": json_schema["row_count"],
        "row_count_difference": csv_schema["row_count"] - json_schema["row_count"],
    }
    out = SCHEMA_DIR / f"{stem}_comparison.json"
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(
        "Comparison %s: only_csv=%d only_json=%d type_diffs=%d row_diff=%d -> %s",
        stem, len(result["columns_only_in_csv"]), len(result["columns_only_in_json"]),
        len(result["type_differences"]), result["row_count_difference"],
        rel_path(out),
    )
    return result


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def process_file(path: Path, fmt: str, rules: Rules, preview: str = "vertical",
                 preview_columns=None) -> tuple:
    """Standardize headers, infer, read, write and preview one file.

    Returns (schema, written_path, sensitive_values) where sensitive_values holds the raw
    values of sensitive columns for the privacy check (memory only).
    """
    log.info("Processing %s [%s]", rel_path(path), rules.dataset)
    df, layout = load_raw(path, fmt)
    df, source_headers = standardize_headers(df, rules, path.name)
    schema = infer_schema(df, path, fmt, layout, rules, source_headers)
    typed = read_with_schema(df, schema, rules)  # fills cast_failures
    add_samples(schema, df, typed, rules)
    out = write_schema(schema, path.stem, fmt)
    print_preview(typed, path.name, schema, rules, preview, preview_columns)
    return schema, out, collect_sensitive(df, rules)


def run_privacy_check(outputs: list, sensitive: dict, config_version, log_start: int) -> dict:
    """Scan this run's outputs for raw sensitive values; write config/schemas/privacy_check.json.

    `sensitive` is {dataset: {column: set(values)}}. Leaks record the file and column only,
    never the value. The log file is scanned from byte offset `log_start` (this run only).
    """
    for handler in log.handlers:
        handler.flush()
    targets = list(outputs)
    if LOG_FILE.exists():
        targets.append(LOG_FILE)

    leaks, scanned = [], []
    for path in targets:
        try:
            data = Path(path).read_bytes()
            if Path(path) == LOG_FILE:
                data = data[log_start:]
            text = data.decode("utf-8", errors="replace")
        except OSError as exc:
            log.error("Privacy check could not read %s: %s", rel_path(path), exc)
            continue
        scanned.append(rel_path(path))
        for dataset, columns in sensitive.items():
            for column, values in columns.items():
                if any(v in text for v in values):
                    leaks.append({"file": rel_path(path), "column": column})

    result = {
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        "config_version": config_version,
        "status": "FAIL" if leaks else "PASS",
        "files_scanned": scanned,
        "sensitive_columns_checked": {
            ds: {col: len(vals) for col, vals in cols.items()} for ds, cols in sensitive.items()
        },
        "leaks": leaks,
    }
    out = SCHEMA_DIR / "privacy_check.json"
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Privacy check %s: %d files scanned, %d leaks -> %s",
             result["status"], len(scanned), len(leaks), rel_path(out))
    for leak in leaks:
        log.error("Privacy leak: column %s found unmasked in %s", leak["column"], leak["file"])
    return result


def discover_files(args) -> list:
    """Return [(path, fmt)] for the requested scan (never touches data/test_out/)."""
    if args.file:
        f = args.file if args.file.is_absolute() else PROJECT_ROOT / args.file
        fmt = f.suffix.lower().lstrip(".")
        if fmt not in ("csv", "json"):
            log.error("Unsupported file type: %s", rel_path(f))
            return []
        return [(f, fmt)]
    targets = [(p, "csv") for p in sorted(args.csv_dir.glob("*.csv"))]
    targets += [(p, "json") for p in sorted(args.json_dir.glob("*.json"))]
    return targets


def main() -> int:
    """Entry point: load config, process files in load order, compare CSV vs JSON."""
    parser = argparse.ArgumentParser(description="Infer and read CSV/JSON schemas (config-driven).")
    parser.add_argument("--csv-dir", type=Path, default=DEFAULT_CSV_DIR)
    parser.add_argument("--json-dir", type=Path, default=DEFAULT_JSON_DIR)
    parser.add_argument("--file", type=Path, help="Process a single .csv or .json file")
    parser.add_argument("--dataset", help="Process only this configured dataset")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG,
                        help="Dataset config (default: config/datasets.yaml)")
    parser.add_argument("--preview", choices=("vertical", "keys", "none"), default="vertical",
                        help="Console preview mode (default: vertical)")
    parser.add_argument("--preview-columns",
                        type=lambda s: [c.strip() for c in s.split(",") if c.strip()],
                        help="Comma-separated key columns for --preview keys")
    args = parser.parse_args()

    log_start = LOG_FILE.stat().st_size if LOG_FILE.exists() else 0
    setup_logging()
    SCHEMA_DIR.mkdir(parents=True, exist_ok=True)

    config_path = args.config if args.config.is_absolute() else PROJECT_ROOT / args.config
    cfg = load_config(config_path)
    datasets = validate_config(cfg)
    stem_of = {name: ds["file_stem"] for name, ds in datasets.items()}

    if args.dataset and args.dataset not in datasets:
        log.error("Dataset '%s' is not a valid configured dataset (known: %s)",
                  args.dataset, ", ".join(datasets) or "none")
        return 1

    # Assign each file to a dataset; configured ones first in load_order, then unconfigured.
    scanned = discover_files(args)
    stems_found = {p.stem for p, _ in scanned}
    items = []
    for path, fmt in scanned:
        name, ds = match_dataset(path.stem, datasets)
        order = ds.get("load_order", 9999) if ds else float("inf")
        items.append((order, name, ds, path, fmt))
    items.sort(key=lambda t: (t[0], t[3].stem, t[4]))
    if args.dataset:
        items = [t for t in items if t[1] == args.dataset]

    if not items:
        log.warning("No input files found.")

    dep_warnings = 0
    for name, ds in datasets.items():
        if any(t[1] == name for t in items):
            dep_warnings += check_dependencies(name, ds, stems_found, stem_of)

    schemas = {}
    order_seen, unconfigured, failed_files = [], [], []
    written = 0
    outputs, sensitive = [], {}
    for _, name, ds, path, fmt in items:
        rules = build_rules(cfg, name, ds)
        if name is None:
            log.info("%s: unconfigured (default rules only)", rel_path(path))
            unconfigured.append(path.name)
        elif name not in order_seen:
            order_seen.append(name)
        try:
            schema, out, found = process_file(path, fmt, rules, args.preview,
                                              args.preview_columns)
            schemas[(path.stem, fmt)] = schema
            outputs.append(out)
            written += 1
            bucket = sensitive.setdefault(rules.dataset, {})
            for col, vals in found.items():
                bucket.setdefault(col, set()).update(vals)
        except Exception as exc:  # keep going with other files
            failed_files.append(path.name)
            log.error("Failed %s: %s", rel_path(path), exc)

    for stem in sorted({s for s, _ in schemas}):
        if (stem, "csv") in schemas and (stem, "json") in schemas:
            try:
                compare_schemas(schemas[(stem, "csv")], schemas[(stem, "json")], stem)
                outputs.append(SCHEMA_DIR / f"{stem}_comparison.json")
            except Exception as exc:
                log.error("Comparison failed for %s: %s", stem, exc)

    privacy = run_privacy_check(outputs, sensitive, cfg.get("version"), log_start)

    log.info(
        "SUMMARY: datasets in order=%s | files processed=%d | files failed=%d%s | "
        "unconfigured files=%d%s | dependency warnings=%d | schemas written=%d | "
        "privacy check=%s",
        " -> ".join(order_seen) or "none", len(schemas), len(failed_files),
        f" ({', '.join(failed_files)})" if failed_files else "",
        len(unconfigured), f" ({', '.join(unconfigured)})" if unconfigured else "",
        dep_warnings, written, privacy["status"],
    )
    return 1 if failed_files or privacy["status"] == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
