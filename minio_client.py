import boto3
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from botocore.config import Config
from datetime import datetime
import logging
import os
from io import BytesIO
import gzip


class MinioClient(BaseHook):
    """
    Reusable MinIO/S3 client for Airflow.
    Uses Airflow Connection only (no Variables).
    """

    def __init__(self, conn_id: str):
        super().__init__()
        self.conn = self.get_connection(conn_id)

        self._parse_connection()
        self._init_boto_client()

    # --------------------------------------------------------------
    def _parse_connection(self):
        """Extract credentials and endpoint from Airflow connection."""

        extras = self.conn.extra_dejson

        self.endpoint = extras.get("endpoint")
        if not self.endpoint:
            raise ValueError(
                "Missing 'endpoint' in connection extras (e.g. http://minio:9000)"
            )

        self.access_key = self.conn.login
        self.secret_key = self.conn.password

        if not self.access_key or not self.secret_key:
            raise ValueError("Missing access_key or secret_key in connection.")

        self.verify_ssl = extras.get("verify_ssl", False)

    # --------------------------------------------------------------
    def _init_boto_client(self):
        """Initialize boto3 S3 client."""

        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            verify=self.verify_ssl,
            config=Config(
                retries={"max_attempts": 3, "mode": "standard"},
                s3={"addressing_style": "path"},
            ),
        )

    # --------------------------------------------------------------
    # ✅ GENERIC METHODS (Reusable anywhere)

    def list_buckets(self):
        return self.client.list_buckets()

    def list_objects_1(self, bucket: str, prefix: str = ""):
        return self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    def list_objects_2(self, bucket: str, prefix: str = "", recursive: bool = True):
        return self.client.list_objects(
            bucket_name=bucket,
            prefix=prefix,
            recursive=recursive
        )
    
    def list_objects(self, bucket: str, prefix: str = ""):
        response = self.client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        return response.get("Contents", [])

    def upload_file(self, bucket: str, key: str, file_path: str):
        self.client.upload_file(file_path, bucket, key)

    def download_file(self, bucket: str, key: str, file_path: str):
        self.client.download_file(bucket, key, file_path)

    def delete_object(self, bucket: str, key: str):
        self.client.delete_object(Bucket=bucket, Key=key)

    def object_exists(self, bucket: str, key: str) -> bool:
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False

    def _generate_filename(self, dataset: str, extension: str = "csv.gz"):
        """
        Generate dynamic filename using timestamp
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"{dataset}_{timestamp}.{extension}"

    def _generate_object_path(self, dataset: str, filename: str):
        """
        Generate MinIO object path dynamically
        """
        if dataset not in self.dataset_paths:
            raise ValueError(f"Invalid dataset: {dataset}")

        return f"{self.base_path}{self.dataset_paths[dataset]}{filename}"

    def upload_dataset(
        self,
        dataset: str,
        local_file_path: str,
        compress: bool = True
    ):
        """
        Upload dataset (customers/products/sales) dynamically

        :param dataset: customers | products | sales
        :param local_file_path: path to generated file
        :param compress: expect gzip file for large datasets
        """

        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"{local_file_path} not found")

        extension = "csv.gz" if compress else "csv"
        filename = self._generate_filename(dataset, extension)
        object_path = self._generate_object_path(dataset, filename)

        try:
            logging.info(f"Uploading {dataset} → {object_path}")

            self.client.upload_file(
                Filename=local_file_path,
                Bucket=self.bucket,
                Key=object_path
            )

            logging.info(f"{dataset} upload successful")

        except Exception as e:
            logging.error(f"{dataset} upload failed: {str(e)}")
            raise

    # def upload_dataframe_stream(
    #     self,
    #     dataset: str,
    #     df,
    #     file_name: str,
    #     compress: bool = True,
    #     chunksize: int = 100_000
    # ):
    #     """
    #     Stream DataFrame directly to MinIO without saving locally.
    #     Handles large datasets using chunking.
    #     """

    #     if dataset not in self.dataset_paths:
    #         raise ValueError(f"Invalid dataset: {dataset}")

    #     object_path = (
    #         f"{self.base_path}"
    #         f"{self.dataset_paths[dataset]}"
    #         f"{file_name}"
    #     )

    #     logging.info(f"Streaming upload to s3://{self.bucket}/{object_path}")

    #     buffer = BytesIO()

    #     if compress:
    #         with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
    #             for i, chunk in enumerate(
    #                 range(0, len(df), chunksize)
    #             ):
    #                 df.iloc[chunk:chunk + chunksize].to_csv(
    #                     gz,
    #                     index=False,
    #                     header=(i == 0)
    #                 )
    #     else:
    #         for i, chunk in enumerate(range(0, len(df), chunksize)):
    #             df.iloc[chunk:chunk + chunksize].to_csv(
    #                 buffer,
    #                 index=False,
    #                 header=(i == 0)
    #             )

    #     buffer.seek(0)

    #     self.client.upload_fileobj(
    #         buffer,
    #         Bucket=self.bucket,
    #         Key=object_path
    #     )

    #     logging.info("Streaming upload complete")


    import logging
    from io import BytesIO
    import gzip


    def upload_dataframe_stream(
        self,
        bucket: str,
        key: str,
        df,
        compress: bool = True,
        chunksize: int = 100_000
    ):
        """
        Stream a pandas DataFrame directly to MinIO/S3.

        Parameters:
        - bucket: target bucket name
        - key: full object key (path + filename)
        - df: pandas DataFrame
        - compress: whether to gzip the file
        - chunksize: number of rows per chunk
        """

        logging.info(f"Streaming upload to s3://{bucket}/{key}")

        buffer = BytesIO()

        if compress:
            with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
                for i, start in enumerate(range(0, len(df), chunksize)):
                    df.iloc[start:start + chunksize].to_csv(
                        gz,
                        index=False,
                        header=(i == 0)
                    )
        else:
            for i, start in enumerate(range(0, len(df), chunksize)):
                df.iloc[start:start + chunksize].to_csv(
                    buffer,
                    index=False,
                    header=(i == 0)
                )

        buffer.seek(0)

        self.client.upload_fileobj(
            buffer,
            Bucket=bucket,
            Key=key
        )

        logging.info("Streaming upload complete")




# # MINIO_BUCKET = ecommerce
# # MINIO_CUSTOMERS_PATH = landing/customers/
# # MINIO_PRODUCTS_PATH = landing/products/
# # MINIO_SALES_PATH = landing/sales/
# # MINIO_ENDPOINT = http://minio:9000

# MINIO_BUCKET = ecommerce
# MINIO_ENDPOINT = http://minio:9000

# MINIO_BASE_PATH = landing/

# MINIO_DATASETS = {
#   "customers": "customers/",
#   "products": "products/",
#   "sales": "sales/"
# }

# LOCAL_DATA_DIR = /opt/airflow/data/