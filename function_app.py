import os
import re
import time
import base64
import logging
import smtplib
import pandas as pd
import azure.functions as func
from azure.data.tables import TableServiceClient
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization

if os.environ.get("DEBUGPY_ENABLE"):
    import debugpy
    debugpy.listen(("localhost", 9091))

VERSION = "26.4.2"

app = func.FunctionApp()


# ---------------------------------------------------------------------------
# Email helpers
# ---------------------------------------------------------------------------

def _pad_field(label, value, width):
    """Format a label+value line with dot-padding: label fills 'width' chars."""
    dots = '.' * max(1, width - len(label))
    return f"{label}{dots} {value}"


def _format_duration(ms):
    if ms < 1000:
        return f"{ms}ms"
    seconds = ms / 1000
    if seconds < 60:
        return f"{seconds:.3f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m {secs:.3f}s"


def _format_email_body(status, status_code, function_instance,
                        run_started_str, run_ended_str, duration_ms,
                        watermark_used, total_rows, chunks_processed,
                        error_message=None):
    H = 24   # header section column width
    M = 40   # message section column width

    lines = [
        "FxResult Report",
        "-" * 50,
        _pad_field("Function",    "OpsMetricsToSnowflake", H),
        _pad_field("Instance",    function_instance,          H),
        _pad_field("Version",     VERSION,                    H),
        _pad_field("Status",      status,                     H),
        _pad_field("Status Code", status_code,                H),
        _pad_field("Run Started", run_started_str,            H),
        _pad_field("Run Ended",   run_ended_str,              H),
        _pad_field("Duration",    _format_duration(duration_ms), H),
        "",
        "MESSAGE",
        "-" * 50,
        _pad_field("Watermark Used",    watermark_used or "None (full load)", M),
        _pad_field("Rows Synced",       str(total_rows),                      M),
        _pad_field("Chunks Processed",  str(chunks_processed),                M),
    ]

    if error_message:
        lines += [
            "",
            "ERROR DETAIL",
            "-" * 50,
            error_message,
        ]

    return "\n".join(lines)


def _send_notification(conn_str, status, status_code, function_instance,
                        run_started_str, run_ended_str, duration_ms,
                        watermark_used, total_rows, chunks_processed,
                        error_message=None):
    try:
        table_service = TableServiceClient.from_connection_string(conn_str=conn_str)
        config_table = table_service.get_table_client(table_name="FunctionConfig")
        entities = list(config_table.query_entities(
            query_filter="PartitionKey eq 'function-global-smtp'"
        ))

        cfg = {e['RowKey']: e.get('Value', '') for e in entities}

        # --- Parse recipients ---
        pattern = re.compile(r'^Recipient\[(\d+)\](.+)$')
        recipients_raw = {}
        for key, value in cfg.items():
            m = pattern.match(key)
            if m:
                idx = int(m.group(1))
                attr = m.group(2)
                if idx not in recipients_raw:
                    recipients_raw[idx] = {}
                recipients_raw[idx][attr] = value
        recipients = [recipients_raw[i] for i in sorted(recipients_raw)]

        # --- Build email ---
        smtp_server  = cfg.get('SmtpServer', '')
        smtp_port    = int(cfg.get('SmtpPort', 587))
        smtp_user    = cfg.get('SmtpUserID', '')
        smtp_pass    = cfg.get('SmtpPassword', '')
        from_addr    = cfg.get('EmailFromAddress', '')
        from_name    = cfg.get('EmailFromName', '')
        reply_to_addr = cfg.get('EmailReplyToAddress', '')
        reply_to_name = cfg.get('EmailReplyToName', '')

        subject = (
            f"{status}-({status_code}) - AzFn - OpsMetricsToSnowflake "
            f"({function_instance}) Completed"
        )
        body = _format_email_body(
            status, status_code, function_instance,
            run_started_str, run_ended_str, duration_ms,
            watermark_used, total_rows, chunks_processed,
            error_message
        )

        from_header  = f"{from_name} <{from_addr}>" if from_name else from_addr
        reply_header = f"{reply_to_name} <{reply_to_addr}>" if reply_to_name else reply_to_addr

        smtp_class = smtplib.SMTP_SSL if smtp_port == 465 else smtplib.SMTP
        with smtp_class(smtp_server, smtp_port, timeout=30) as server:
            if smtp_port != 465:
                server.starttls()
            server.login(smtp_user, smtp_pass)

            for recipient in recipients:
                is_active   = str(recipient.get('IsActive',   'true')).lower()  == 'true'
                errors_only = str(recipient.get('ErrorsOnly', 'false')).lower() == 'true'
                to_addr     = recipient.get('EmailAddress', '')
                to_name     = recipient.get('Name', '')

                if not is_active or not to_addr:
                    continue
                if errors_only and status == 'SUCCESS':
                    continue

                msg = MIMEMultipart()
                msg['Subject']  = subject
                msg['From']     = from_header
                msg['To']       = f"{to_name} <{to_addr}>" if to_name else to_addr
                if reply_to_addr:
                    msg['Reply-To'] = reply_header
                msg.attach(MIMEText(body, 'plain'))

                server.sendmail(from_addr, to_addr, msg.as_string())
                logging.info(f"Notification email sent to {to_addr}.")

    except Exception as e:
        logging.error(f"Failed to send notification email: {str(e)}")


# ---------------------------------------------------------------------------
# Timer trigger
# ---------------------------------------------------------------------------

@app.timer_trigger(schedule="0 */15 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True)
def table_to_snowflake(myTimer: func.TimerRequest) -> None:
    logging.info('Starting incremental OpsMetrics sync.')

    ctx             = None
    status          = "ERROR"
    status_code     = "00001"
    total_rows_loaded = 0
    chunks_processed  = 0
    watermark_str   = None
    error_message   = None
    run_started     = datetime.now(timezone.utc)
    function_instance = os.environ.get("_org_FunctionInstance", "")

    try:
        key_b64 = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PEM")
        if key_b64:
            if key_b64.strip().startswith("-----"):
                # PEM format — load and convert to DER
                private_key = serialization.load_pem_private_key(key_b64.encode(), password=None)
                private_key_bytes = private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
            else:
                # Base64-encoded DER format
                key_b64 = "".join(key_b64.split()).rstrip("=")
                key_b64 += "=" * (-len(key_b64) % 4)
                private_key_bytes = base64.b64decode(key_b64)
        else:
            with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as f:
                private_key = serialization.load_pem_private_key(f.read(), password=None)
            private_key_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

        ctx = snowflake.connector.connect(
            user=os.environ["SNOWFLAKE_USER"],
            private_key=private_key_bytes,
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_SCHEMA"]
        )

        target_table = os.environ["SNOWFLAKE_TARGET_TABLE"].upper()
        cursor = ctx.cursor()

        cursor.execute(f"SELECT MAX(CREATED_TIMESTAMP) FROM {target_table}")
        max_ts = cursor.fetchone()[0]

        filter_query = None
        if max_ts:
            watermark_str = max_ts.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            filter_query = f"Timestamp gt datetime'{watermark_str}'"
            logging.info(f"High watermark found: {watermark_str}")
        else:
            logging.info("No watermark found. Executing full historical load.")

        conn_str = os.environ["AZURE_TABLE_CONNECTION_STRING"]
        table_service_client = TableServiceClient.from_connection_string(conn_str=conn_str)
        table_client = table_service_client.get_table_client(table_name=os.environ["AZURE_TABLE_NAME"])

        if filter_query:
            pages = table_client.query_entities(query_filter=filter_query).by_page()
        else:
            pages = table_client.list_entities().by_page()

        chunk_number = 1
        start_time = time.monotonic()
        is_azure = "WEBSITE_INSTANCE_ID" in os.environ
        timeout_seconds = 9 * 60

        for page in pages:
            if is_azure and (time.monotonic() - start_time) >= timeout_seconds:
                logging.warning(f"Approaching 10-minute timeout — stopping after {chunk_number - 1} chunks. Will resume on next execution.")
                break
            entities = list(page)
            if not entities:
                continue

            df = pd.DataFrame(entities)
            df['Timestamp'] = [e.metadata.get('timestamp') for e in entities]

            rename_map = {
                'PartitionKey': 'PARTITION_KEY',
                'RowKey': 'ROW_KEY',
                'Timestamp': 'CREATED_TIMESTAMP',
                'FunctionId': 'FUNCTION_ID',
                'Status': 'STATUS',
                'Code': 'CODE',
                'RunStarted': 'RUN_STARTED',
                'RunEnded': 'RUN_ENDED',
                'Duration': 'DURATION',
                'DateSent': 'DATE_SENT',
                'IsDryRun': 'IS_DRY_RUN',
                'IsProd': 'IS_PROD',
                'BlobStorageFileName': 'BLOB_STORAGE_FILE_NAME'
            }
            df = df.rename(columns=rename_map)

            columns_to_keep = list(rename_map.values())
            df = df[[col for col in df.columns if col in columns_to_keep]]

            if 'DURATION' in df.columns:
                df['DURATION'] = pd.to_timedelta(df['DURATION'])
                df['DURATION'] = (df['DURATION'].dt.total_seconds() * 1000).fillna(0).astype(int)

            timestamp_cols = ['CREATED_TIMESTAMP', 'RUN_STARTED', 'RUN_ENDED', 'DATE_SENT']
            for col in timestamp_cols:
                if col in df.columns:
                    parsed = pd.to_datetime(df[col], utc=True).dt.tz_convert(None)
                    df[col] = parsed.dt.strftime('%Y-%m-%d %H:%M:%S.%f')

            staging_table = f"{target_table}_STG"
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
            cursor.execute(f"""
                CREATE TEMPORARY TABLE {staging_table} (
                    PARTITION_KEY       VARCHAR,
                    ROW_KEY             VARCHAR,
                    CREATED_TIMESTAMP   VARCHAR,
                    FUNCTION_ID         VARCHAR,
                    STATUS              VARCHAR,
                    CODE                VARCHAR,
                    RUN_STARTED         VARCHAR,
                    RUN_ENDED           VARCHAR,
                    DURATION            NUMBER,
                    DATE_SENT           VARCHAR,
                    IS_DRY_RUN          BOOLEAN,
                    IS_PROD             BOOLEAN,
                    BLOB_STORAGE_FILE_NAME VARCHAR
                )
            """)
            logging.info(f"Writing Chunk {chunk_number} ({len(df)} rows) to Staging Table...")

            success, _, nrows, _ = write_pandas(
                conn=ctx,
                df=df,
                table_name=staging_table,
                quote_identifiers=False
            )

            if success:
                logging.info(f"Merging Chunk {chunk_number} into {target_table}...")

                merge_sql = f"""
                MERGE INTO {target_table} AS target
                USING {staging_table} AS source
                ON target.PARTITION_KEY = source.PARTITION_KEY
                   AND target.ROW_KEY = source.ROW_KEY

                WHEN MATCHED THEN
                    UPDATE SET
                        target.CREATED_TIMESTAMP = TRY_TO_TIMESTAMP_NTZ(source.CREATED_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF6'),
                        target.FUNCTION_ID = source.FUNCTION_ID,
                        target.STATUS = source.STATUS,
                        target.CODE = source.CODE,
                        target.RUN_STARTED = TRY_TO_TIMESTAMP_NTZ(source.RUN_STARTED, 'YYYY-MM-DD HH24:MI:SS.FF6'),
                        target.RUN_ENDED = TRY_TO_TIMESTAMP_NTZ(source.RUN_ENDED, 'YYYY-MM-DD HH24:MI:SS.FF6'),
                        target.DURATION = source.DURATION,
                        target.DATE_SENT = TRY_TO_TIMESTAMP_NTZ(source.DATE_SENT, 'YYYY-MM-DD HH24:MI:SS.FF6'),
                        target.IS_DRY_RUN = source.IS_DRY_RUN,
                        target.IS_PROD = source.IS_PROD,
                        target.BLOB_STORAGE_FILE_NAME = source.BLOB_STORAGE_FILE_NAME

                WHEN NOT MATCHED THEN
                    INSERT (
                        PARTITION_KEY, ROW_KEY, CREATED_TIMESTAMP, FUNCTION_ID,
                        STATUS, CODE, RUN_STARTED, RUN_ENDED, DURATION,
                        DATE_SENT, IS_DRY_RUN, IS_PROD, BLOB_STORAGE_FILE_NAME
                    )
                    VALUES (
                        source.PARTITION_KEY, source.ROW_KEY, TRY_TO_TIMESTAMP_NTZ(source.CREATED_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF6'), source.FUNCTION_ID,
                        source.STATUS, source.CODE, TRY_TO_TIMESTAMP_NTZ(source.RUN_STARTED, 'YYYY-MM-DD HH24:MI:SS.FF6'), TRY_TO_TIMESTAMP_NTZ(source.RUN_ENDED, 'YYYY-MM-DD HH24:MI:SS.FF6'), source.DURATION,
                        TRY_TO_TIMESTAMP_NTZ(source.DATE_SENT, 'YYYY-MM-DD HH24:MI:SS.FF6'), source.IS_DRY_RUN, source.IS_PROD, source.BLOB_STORAGE_FILE_NAME
                    );
                """
                cursor.execute(merge_sql)
                total_rows_loaded += nrows
                chunks_processed += 1
                chunk_number += 1
            else:
                logging.error(f"Failed to write chunk {chunk_number} to staging table.")

        status      = "SUCCESS"
        status_code = "00000"
        logging.info(f"Sync complete. Successfully processed {total_rows_loaded} rows.")

    except Exception as e:
        error_message = str(e)
        logging.error(f"Critical error during sync pipeline: {error_message}")

    finally:
        try:
            if ctx and not ctx.is_closed():
                ctx.close()
        except Exception as e:
            logging.warning(f"Error closing Snowflake connection: {str(e)}")

        run_ended    = datetime.now(timezone.utc)
        duration_ms  = int((run_ended - run_started).total_seconds() * 1000)
        ts_fmt       = '%Y-%m-%d %H:%M:%S UTC'

        always_send  = os.environ.get("_org_AlwaysSendEmail", "True").lower() == "true"
        should_send  = always_send or total_rows_loaded > 0 or status == "ERROR"

        if should_send:
            conn_str = os.environ.get("AZURE_TABLE_CONNECTION_STRING")
            if conn_str:
                _send_notification(
                    conn_str, status, status_code, function_instance,
                    run_started.strftime(ts_fmt),
                    run_ended.strftime(ts_fmt),
                    duration_ms,
                    watermark_str, total_rows_loaded, chunks_processed,
                    error_message
                )
