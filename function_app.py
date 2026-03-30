import os
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

        for page in pages:
            entities = list(page)
            if not entities:
                continue

            df = pd.DataFrame(entities)
            
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

            # 2. Strip timezone metadata from all timestamp columns for TIMESTAMP_NTZ
            timestamp_cols = ['CREATED_TIMESTAMP', 'RUN_STARTED', 'RUN_ENDED', 'DATE_SENT']
            for col in timestamp_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)

            # --- Staging & Upsert ---
            staging_table = f"{target_table}_STG"
            logging.info(f"Writing Chunk {chunk_number} ({len(df)} rows) to Staging Table...")
            
            success, _, nrows, _ = write_pandas(
                conn=ctx,
                df=df,
                table_name=staging_table,
                table_type="temp",
                auto_create_table=True,
                overwrite=True,
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
                        target.CREATED_TIMESTAMP = source.CREATED_TIMESTAMP,
                        target.FUNCTION_ID = source.FUNCTION_ID,
                        target.STATUS = source.STATUS,
                        target.CODE = source.CODE,
                        target.RUN_STARTED = source.RUN_STARTED,
                        target.RUN_ENDED = source.RUN_ENDED,
                        target.DURATION = source.DURATION,
                        target.DATE_SENT = source.DATE_SENT,
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
                        source.PARTITION_KEY, source.ROW_KEY, source.CREATED_TIMESTAMP, source.FUNCTION_ID, 
                        source.STATUS, source.CODE, source.RUN_STARTED, source.RUN_ENDED, source.DURATION, 
                        source.DATE_SENT, source.IS_DRY_RUN, source.IS_PROD, source.BLOB_STORAGE_FILE_NAME
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