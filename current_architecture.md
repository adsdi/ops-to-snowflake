# Current Architecture — ops-to-snowflake

**Version:** 26.4.2  
**As-of date:** 2026-04-06

---

## Overview

A single-file Azure Function (Python v2 programming model) that incrementally synchronises rows from an Azure Table Storage table into a Snowflake table on a 15-minute schedule.

---

## Trigger

| Property        | Value |
|-----------------|-------|
| Type            | Timer |
| Schedule (CRON) | `0 */15 * * * *` — fires at :00, :15, :30, :45 of every hour |
| `run_on_startup` | `False` |
| `use_monitor`   | `True` — distributed singleton lock via Azure Storage; prevents duplicate execution across scale-out instances |

---

## Data Flow

```
Azure Table Storage
  └─ query_entities (incremental filter OR full scan)
       │
       ▼
  pandas DataFrame
  (column rename, type coercion, timestamp normalisation)
       │
       ▼
  Snowflake Temp Staging Table  (<TARGET>_STG)
  (write_pandas — bulk PUT + COPY INTO)
       │
       ▼
  Snowflake MERGE INTO <TARGET>
  (upsert on PARTITION_KEY + ROW_KEY)
       │
       ▼
  Email notification (SMTP)
  (sent via SMTP to recipients stored in Azure Table "FunctionConfig")
```

### Watermark / Incremental Strategy

1. On each run, queries `SELECT MAX(CREATED_TIMESTAMP) FROM <TARGET_TABLE>` in Snowflake.
2. If a max timestamp exists, uses `Timestamp gt datetime'<watermark>'` as an OData filter on the Azure Table query — incremental load.
3. If no rows exist in Snowflake, performs a full historical load (no filter).
4. The watermark is derived from the Snowflake target table; there is no separate state/checkpoint store.

### Chunked Processing

- Azure Table results are iterated page-by-page (SDK pagination).
- Each page is written to a fresh `TEMPORARY TABLE` (`<TARGET>_STG`) and then merged into the target.
- A 9-minute soft timeout guards against the Azure Functions 10-minute execution limit; processing stops cleanly and resumes on the next trigger.

---

## Column Mapping

| Azure Table Field       | Snowflake Column          | Type in Snowflake |
|-------------------------|---------------------------|-------------------|
| `PartitionKey`          | `PARTITION_KEY`           | VARCHAR           |
| `RowKey`                | `ROW_KEY`                 | VARCHAR           |
| `e.metadata['timestamp']` | `CREATED_TIMESTAMP`     | TIMESTAMP_NTZ     |
| `FunctionId`            | `FUNCTION_ID`             | VARCHAR           |
| `Status`                | `STATUS`                  | VARCHAR           |
| `Code`                  | `CODE`                    | VARCHAR           |
| `RunStarted`            | `RUN_STARTED`             | TIMESTAMP_NTZ     |
| `RunEnded`              | `RUN_ENDED`               | TIMESTAMP_NTZ     |
| `Duration`              | `DURATION`                | NUMBER (ms, int)  |
| `DateSent`              | `DATE_SENT`               | TIMESTAMP_NTZ     |
| `IsDryRun`              | `IS_DRY_RUN`              | BOOLEAN           |
| `IsProd`                | `IS_PROD`                 | BOOLEAN           |
| `BlobStorageFileName`   | `BLOB_STORAGE_FILE_NAME`  | VARCHAR           |

Timestamps are normalised to UTC, stripped of timezone info, and formatted as `YYYY-MM-DD HH24:MI:SS.FF6` strings before staging; `TRY_TO_TIMESTAMP_NTZ` converts them during the MERGE.  
`DURATION` is converted from a timedelta to an integer number of milliseconds.

---

## Notification System

- SMTP configuration and recipient list are read from the Azure Table `FunctionConfig` (partition key `function-global-smtp`) at send time.
- Key config values: `SmtpServer`, `SmtpPort`, `SmtpUserID`, `SmtpPassword`, `EmailFromAddress`, `EmailFromName`, `EmailReplyToAddress`, `EmailReplyToName`.
- Recipients are stored as indexed rows: `Recipient[N]EmailAddress`, `Recipient[N]Name`, `Recipient[N]IsActive`, `Recipient[N]ErrorsOnly`.
- Email is sent if `_org_AlwaysSendEmail=True` (default), or if rows were loaded, or if the run status is ERROR.
- Send failures are logged as warnings but do not affect the function's exit status.

---

## Required Environment Variables / Secrets

| Variable                          | Description                                               | Secret? |
|-----------------------------------|-----------------------------------------------------------|---------|
| `SNOWFLAKE_USER`                  | Snowflake username for key-pair auth                      | No      |
| `SNOWFLAKE_ACCOUNT`               | Snowflake account identifier                              | No      |
| `SNOWFLAKE_WAREHOUSE`             | Snowflake warehouse name                                  | No      |
| `SNOWFLAKE_DATABASE`              | Snowflake database name                                   | No      |
| `SNOWFLAKE_SCHEMA`                | Snowflake schema name                                     | No      |
| `SNOWFLAKE_TARGET_TABLE`          | Target table name (uppercased at runtime)                 | No      |
| `SNOWFLAKE_PRIVATE_KEY_PEM`       | RSA private key — PEM string or base64-encoded DER        | **Yes** |
| `SNOWFLAKE_PRIVATE_KEY_PATH`      | Alternative: path to `.p8` key file (fallback)            | **Yes** |
| `AZURE_TABLE_CONNECTION_STRING`   | Connection string for Azure Table Storage (source + config) | **Yes** |
| `AZURE_TABLE_NAME`                | Source Azure Table name                                   | No      |
| `_org_FunctionInstance`           | Human-readable instance label used in notification emails | No      |
| `_org_AlwaysSendEmail`            | `True`/`False` — send notification even when 0 rows synced | No     |
| `DEBUGPY_ENABLE`                  | Non-empty to enable debugpy listener on port 9091 (dev only) | No   |

---

## Key Files

| File                    | Purpose |
|-------------------------|---------|
| [function_app.py](function_app.py) | All application logic (single file) |
| [requirements.txt](requirements.txt) | Runtime dependencies |
| [.funcignore](.funcignore) | Files excluded from Azure Functions deployment package |
| [.gitignore](.gitignore) | Files excluded from version control (keys, local settings, Azurite state) |
| [.vscode/launch.json](.vscode/launch.json) | VS Code debug attach config (debugpy on port 9091) |

---

## Dependencies

```
azure-functions
azure-data-tables
pandas
snowflake-connector-python[pandas]
```

`cryptography` is used directly (`serialization.load_pem_private_key`) but is not pinned in `requirements.txt` — it is an implicit transitive dependency of `snowflake-connector-python`.

---

## Deviations from CLAUDE.md Standards

### 1. Missing type hints (HIGH)
**Standard:** All functions must have complete PEP 484 type hints.  
**Current state:** None of the helper functions have parameter or return type annotations.

```python
# Current — no type hints
def _pad_field(label, value, width):
def _format_duration(ms):
def _format_email_body(status, status_code, ...):
def _send_notification(conn_str, status, ...):
```

### 2. Connection string used instead of DefaultAzureCredential (HIGH)
**Standard:** Use `azure-identity` and `DefaultAzureCredential`. No hardcoded connection strings or keys.  
**Current state:** Azure Table Storage is accessed via `AZURE_TABLE_CONNECTION_STRING` (a shared-key connection string stored as an environment variable) rather than `DefaultAzureCredential` + an endpoint URL. `azure-identity` is not in `requirements.txt`.

### 3. Overly broad exception handling (MEDIUM)
**Standard:** Use specific exception handling; avoid bare `except:`.  
**Current state:** All `except` clauses catch `Exception` broadly. More specific types are available and would improve diagnostics:
- `azure.core.exceptions.AzureError` for Table Storage operations
- `snowflake.connector.errors.DatabaseError` / `ProgrammingError` for Snowflake
- `smtplib.SMTPException` for email failures

### 4. `cryptography` is an undeclared dependency (LOW)
**Current state:** `from cryptography.hazmat.primitives import serialization` is imported directly but `cryptography` is not listed in `requirements.txt`. The package is available transitively today, but a future version of `snowflake-connector-python` could drop it without warning.
