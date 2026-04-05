from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow import DAG
from datetime import datetime
import logging


from minio_client import MinioClient
from postgres_client import PostgresClient
from dag_utils import generate_and_upload_all, load_all_from_minio




logger = logging.getLogger(__name__)


def load_task(**context):
    """
    Airflow task wrapper for loading all datasets
    """

    # =========================
    # DAG-level parameters
    # =========================
    MINIO_CONN_ID = "minio_conn"
    POSTGRES_CONN_ID = "postgres_conn"
    BUCKET = "retailio"

    # =========================
    # Initialize clients
    # =========================
    minio_client = MinioClient(conn_id=MINIO_CONN_ID)
    postgres_client = PostgresClient(conn_id=POSTGRES_CONN_ID)

    # =========================
    # Call your function
    # =========================
    load_all_from_minio(
        minio_client=minio_client,
        postgres_client=postgres_client,
        bucket=BUCKET
    )



with DAG(
    dag_id="retail_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    upload_to_minio = PythonOperator(
        task_id="generate_and_upload_data",
        python_callable=generate_and_upload_all,
        op_kwargs={
            "bucket": "retailio",
            "base_path": "landing",
            "timestamp": "{{ ts_nodash }}"
        }
    )

    load_all = PythonOperator(
        task_id="load_all_from_minio",
        python_callable=load_task
    )

    upload_to_minio >> load_all