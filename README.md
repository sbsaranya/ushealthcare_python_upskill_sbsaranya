# US Healthcare Payer Data Engineering – Python Upskill

A hands-on data engineering project built around a **US healthcare payer data model**
(accounts, products, plans, subscriptions, members and coverage), using **synthetic data only**.

The goal is to practise modern data engineering on a domain I know well from 15+ years of
SQL development in US healthcare: config-driven Python pipelines, schema management,
a Bronze / Silver / Gold layout, and AI-assisted development with Claude Code.

> **Data notice:** all data in this project is synthetic. No real member, employer or
> health information is used. Data files are not published in this repository.

---

## Tech stack

- **Python** (pandas, PyYAML) – current schema reader
- **SQL Server Express** – source tables (planned)
- **PySpark / AWS Glue, S3** – later stages (planned)
- **Claude Code** in VS Code – AI-assisted development driven by project skills (`SKILL.md`)

## Project structure

```
.claude/skills/            Claude Code skills (instructions that generate / update scripts)
config/
  datasets.yaml            Per-dataset settings: header mapping, types, masking, load order
  schemas/                 Generated schema, comparison and privacy-check files (masked)
data/                      Bronze / Silver / Gold and raw folders (files not published)
sql/                       DDL and stored procedures
src/
  common/
    schema_reader.py       Config-driven CSV + JSON schema reader
    db_config.py           SQL Server connection (settings come from .env)
```

## Payer data model – load order

| # | Dataset | Depends on | Status |
|---|---------|------------|--------|
| 1 | Account (employer groups + child classes/locations) | – | ✅ Done |
| 2 | Product | – | Next |
| 3 | Plan | Product | Planned |
| 4 | Account–Plan offering | Account, Plan | Planned |
| 5 | Subscription | Account, Plan offering | Planned |
| 6 | Member | Subscription, Account | Planned |
| 7 | Member coverage | Member, Plan | Planned |

## Schema reader (`src/common/schema_reader.py`)

A reusable script that reads raw CSV and JSON files and documents their structure.
Adding a new dataset means adding an entry to `config/datasets.yaml`, with no code change.

- Maps business-style source headers to standard names (`Min Hours / Week` → `MIN_HOURS_PER_WEEK`)
- Infers or applies column types, including percent (`80%` → 0.80) and currency (`$250,000.00` → 250000.00)
- Keeps IDs and codes as text so leading zeros are never lost
- Processes datasets in dependency order and warns when a parent dataset is missing
- Compares CSV and JSON versions of the same dataset
- Handles UTF-8 safely (accents, en dashes, Excel BOM)

### Run it

```
pip install -r requirements.txt
python src/common/schema_reader.py
python src/common/schema_reader.py --dataset account --preview keys
```

Input files go in `data/raw/csv/` and `data/raw/json/`, named after the dataset's `file_stem`
(for example `raw_account.csv`). Database settings are read from a local `.env` file, which is not published.

## Data protection

Built with synthetic data only, following HIPAA-aligned practices. This is not a compliance certification.

- Sensitive columns are classified in `config/datasets.yaml`
- Sensitive values are masked in every output: schema files, console preview and logs
- An automated **privacy check** scans all outputs after every run and writes
  `config/schemas/privacy_check.json` (PASS / FAIL, never the values themselves)
- Secrets, data files and logs are excluded from source control via `.gitignore`

## Roadmap

- [x] Config-driven schema reader with privacy check
- [x] Account dataset
- [ ] Product, plan and account–plan offering datasets
- [ ] Subscription, member and member coverage datasets
- [ ] Silver layer: validation (duplicates, invalid dates, nulls, key checks)
- [ ] Nested 834-style member JSON → flattened Silver tables with PySpark
- [ ] AWS: S3 landing + Glue jobs

## Author

**Saranya** – SQL developer moving into data engineering, with a US healthcare payer background.