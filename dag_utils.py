















# ---------------------------------------------------------------------
# Imports: Standard Library
# ---------------------------------------------------------------------
import json
import logging
import time

# -------------------------
# Airflow imports
# -------------------------
from airflow.exceptions import AirflowSkipException

# ---------------------------------------------------------------------
# Imports: Common / Shared Libraries
# ---------------------------------------------------------------------
from dataset_generator import generate_customers, generate_products, generate_sales
from minio_client import MinioClient


# =========================
#  Load config
# =========================
def load_config():
    with open("/opt/airflow/config/config.json") as f:
        return json.load(f)["datasets"]
    
# =========================
#  Logging
# =========================
logger = logging.getLogger(__name__)
    

# -----------------------------
# GENERATE DATASETS
# -----------------------------
def generate_and_upload_all(
    bucket: str,
    base_path: str,
    timestamp: str,
    **context
):
    """
    Generate datasets and upload to MinIO.
    Fully parameterized (no hardcoded config).
    """

    minio = MinioClient(conn_id="minio_conn")

    start_time = time.time()

    # --------------------------------------------------
    # Generate datasets

    t0 = time.time()
    customers_df = generate_customers()
    logger.info(
        f"✅ Customers generated | rows={len(customers_df)} | duration={time.time() - t0:.2f}s"
    )

    t0 = time.time()
    products_df = generate_products()
    logger.info(
        f"✅ Products generated | rows={len(products_df)} | duration={time.time() - t0:.2f}s"
    )

    t0 = time.time()
    sales_df = generate_sales(customers_df, products_df)
    logger.info(
        f"✅ Sales generated | rows={len(sales_df)} | duration={time.time() - t0:.2f}s"
    )

    datasets = {
        "customers": customers_df,
        "products": products_df,
        "sales": sales_df,
    }
    # --------------------------------------------------
    # Upload each dataset

    for name, df in datasets.items():
        key = f"{base_path}/{name}/{name}_{timestamp}.csv.gz"

        logger.info(
            f"⬆️ Uploading {name} | rows={len(df)} | destination=s3://{bucket}/{key}"
        )

        t0 = time.time()

        try:
            minio.upload_dataframe_stream(
                bucket=bucket,
                key=key,
                df=df
            )

            logger.info(
                f"✅ Upload successful: {name} | duration={time.time() - t0:.2f}s"
            )

        except Exception as e:
            logger.error(
                f"❌ Upload failed: {name} | error={str(e)}",
                exc_info=True
            )
            raise

    logger.info(
        f"🎉 Pipeline completed | total_duration={time.time() - start_time:.2f}s"
    )

# -----------------------------
# LOAD DATA FROM MINIO INTO POSTGRES
# -----------------------------
def load_all_from_minio(minio_client, postgres_client, bucket: str):
    """
    Auto-detect and load all datasets from MinIO → Postgres
    (incremental + multi-dataset with UPSERT for dimensions)
    """


    objects = minio_client.list_objects(bucket=bucket, prefix="landing/")

    files = [
        obj["Key"]
        for obj in objects
        if obj["Key"].endswith(".csv.gz")
    ]

    logger.info(f"Files from MinIO: {len(files)}")
    # =========================
    # 2. Filter new files
    # =========================
    postgres_client.execute_file(file_path='/opt/airflow/sql/tracker_ddl.sql')
    
    new_files = [
        file for file in files
        if not postgres_client.is_file_loaded(file)
    ]

    if not new_files:
        raise AirflowSkipException("No new files found")

    # =========================
    # 3. Load config
    # =========================
    config = load_config()

    # =========================
    # 4. Process each file
    # =========================
    table_file_count = {} 

    for object_name in new_files:

        try:
            dataset = object_name.split("/")[1]
        except IndexError:
            logger.warning(f"Invalid path: {object_name}")
            continue

        if dataset not in config:
            logger.warning(f"Skipping unknown dataset: {object_name}")
            continue

        dataset_config = config[dataset]

        table = dataset_config["table"]
        columns = dataset_config["columns"]
        sql_path = dataset_config["sql"]
        conflict_cols = dataset_config.get("conflict_cols", [])

        # Count files per table
        table_file_count[table] = table_file_count.get(table, 0) + 1

        # =========================
        # Ensure table exists
        # =========================
        postgres_client.execute_file(sql_path)

        # =========================
        # Get file from MinIO
        # =========================
        response = minio_client.client.get_object(Bucket=bucket, Key=object_name)

        try:
            # =========================
            # Load strategy
            # =========================
            if dataset_config["load_type"] == "upsert":
                # Dimension → UPSERT
                data = postgres_client.read_gzip_csv(
                    gzip_stream=response["Body"],   # ✅ THIS is the stream
                    columns=columns
                )

                postgres_client.upsert(
                    table=table,
                    data=data,
                    columns=columns,
                    conflict_cols=conflict_cols
                )


            elif dataset_config["load_type"] == "insert":
                # 👉 Fact → INSERT (append)
                postgres_client.load_gzip_csv(
                    table=table,
                    gzip_stream=response["Body"],
                    columns=columns
                )
                
            else:
                logger.warning(f"No strategy defined for dataset: {dataset}")
                continue

            # =========================
            # Track ingestion
            # =========================
            postgres_client.track_loaded_file(object_name, dataset)

        except Exception as e:
            logger.error(f"Failed loading {object_name}: {str(e)}")
            raise

    # =========================
    # 5. Log summary per table
    # =========================
    for table, count in table_file_count.items():
        logger.info(f"Processed {count} new file(s) for table {table}")

    logger.info("All files processed successfully")