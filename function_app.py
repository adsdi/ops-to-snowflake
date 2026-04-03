import os
import time
import base64
import logging
import pandas as pd
import azure.functions as func
from azure.data.tables import TableServiceClient
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization

if os.environ.get("DEBUGPY_ENABLE"):
    import debugpy
    debugpy.listen(("localhost", 9091))

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */15 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def table_to_snowflake(myTimer: func.TimerRequest) -> None:
    logging.info('Starting incremental OpsMetrics sync.')
    ctx = None

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
        
        # --- MODIFICATION 1: Update Watermark Column ---
        # Query Snowflake for the max CREATED_TIMESTAMP
        cursor.execute(f"SELECT MAX(CREATED_TIMESTAMP) FROM {target_table}")
        max_ts = cursor.fetchone()[0]
        
        filter_query = None
        if max_ts:
            watermark_str = max_ts.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            filter_query = f"Timestamp gt datetime'{watermark_str}'"
            logging.info(f"High watermark found: {watermark_str}")
        else:
            logging.info("No watermark found. Executing full historical load.")

        table_service_client = TableServiceClient.from_connection_string(conn_str=os.environ["AZURE_TABLE_CONNECTION_STRING"])
        table_client = table_service_client.get_table_client(table_name=os.environ["AZURE_TABLE_NAME"])

        if filter_query:
            pages = table_client.query_entities(query_filter=filter_query).by_page()
        else:
            pages = table_client.list_entities().by_page()

        total_rows_loaded = 0
        chunk_number = 1
        start_time = time.monotonic()
        is_azure = "WEBSITE_INSTANCE_ID" in os.environ
        timeout_seconds = 9 * 60  # stop at 9 minutes to allow graceful cleanup

        for page in pages:
            if is_azure and (time.monotonic() - start_time) >= timeout_seconds:
                logging.warning(f"Approaching 10-minute timeout — stopping after {chunk_number - 1} chunks. Will resume on next execution.")
                break
            entities = list(page)
            if not entities:
                continue

            df = pd.DataFrame(entities)
            df['Timestamp'] = [e.metadata.get('timestamp') for e in entities]
            
            # --- MODIFICATION 2: Explicit Column Mapping ---
            # Map Azure PascalCase columns to Snowflake SNAKE_CASE columns
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
            # .rename() safely ignores columns if they happen to be missing in a specific chunk
            df = df.rename(columns=rename_map)

            # Drop any internal Azure columns that weren't in our rename map (like 'etag')
            columns_to_keep = list(rename_map.values())
            df = df[[col for col in df.columns if col in columns_to_keep]]
            
            # --- Format Datatypes ---
            # 1. Parse the ISO 8601 Duration string into integer milliseconds
            if 'DURATION' in df.columns:
                df['DURATION'] = pd.to_timedelta(df['DURATION'])
                # Convert to milliseconds. If a duration is completely missing (NaN), default to 0 to safely cast to integer.
                df['DURATION'] = (df['DURATION'].dt.total_seconds() * 1000).fillna(0).astype(int)

            # 2. Convert timestamp columns to ISO strings for reliable Snowflake ingestion
            timestamp_cols = ['CREATED_TIMESTAMP', 'RUN_STARTED', 'RUN_ENDED', 'DATE_SENT']
            for col in timestamp_cols:
                if col in df.columns:
                    parsed = pd.to_datetime(df[col], utc=True).dt.tz_convert(None)
                    df[col] = parsed.dt.strftime('%Y-%m-%d %H:%M:%S.%f')

            # --- Staging & Upsert ---
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
                # --- MODIFICATION 3: Updated MERGE Statement ---
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
                chunk_number += 1
            else:
                logging.error(f"Failed to write chunk {chunk_number} to staging table.")

        logging.info(f"Sync complete. Successfully processed {total_rows_loaded} rows.")

    except Exception as e:
        logging.error(f"Critical error during sync pipeline: {str(e)}")
    
    finally:
        if ctx and not ctx.is_closed():
            ctx.close()