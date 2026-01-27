"""S3 staging layer: upload extracted/transformed data to S3 for processing."""

import io
import time
from typing import Any, Optional

import pandas as pd
import structlog

logger = structlog.get_logger(__name__)


class S3Stager:
    """Manages S3 staging bucket for extracted and transformed data.

    Data flow:
        DB2 Extract → S3 staging (Parquet) → Glue Transform → S3 output (Parquet) → FAST Load

    Uses Parquet format for:
    - Columnar storage (efficient for Spark/Glue)
    - Schema preservation (types survive round-trip)
    - Compression (~10x smaller than CSV)
    """

    def __init__(
        self,
        bucket: str,
        region: str = "us-east-1",
        prefix: str = "migrations",
    ):
        self.bucket = bucket
        self.region = region
        self.prefix = prefix
        self._client = None

    def _get_client(self) -> Any:
        if self._client is None:
            try:
                import boto3
                self._client = boto3.client("s3", region_name=self.region)
            except Exception:
                self._client = "mock"
        return self._client

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        job_id: str,
        entity_type: str,
        batch_number: int,
        stage: str = "extract",
    ) -> str:
        """Upload a DataFrame to S3 as Parquet. Returns the S3 path."""
        s3_key = f"{self.prefix}/{job_id}/{entity_type}/{stage}/batch_{batch_number:04d}.parquet"
        start = time.monotonic()

        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
        buffer.seek(0)
        size_mb = buffer.getbuffer().nbytes / (1024 * 1024)

        client = self._get_client()
        if client != "mock":
            client.put_object(Bucket=self.bucket, Key=s3_key, Body=buffer.getvalue())

        duration_ms = int((time.monotonic() - start) * 1000)
        s3_path = f"s3://{self.bucket}/{s3_key}"

        logger.info(
            "s3_upload_complete",
            path=s3_path,
            records=len(df),
            size_mb=f"{size_mb:.2f}",
            duration_ms=duration_ms,
        )
        return s3_path

    def download_dataframe(self, s3_path: str) -> pd.DataFrame:
        """Download a Parquet file from S3 and return as DataFrame."""
        client = self._get_client()
        if client == "mock":
            logger.warning("mock_s3_download", path=s3_path)
            return pd.DataFrame()

        # Parse s3://bucket/key
        parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1]

        response = client.get_object(Bucket=bucket, Key=key)
        buffer = io.BytesIO(response["Body"].read())
        return pd.read_parquet(buffer)

    def list_batches(self, job_id: str, entity_type: str, stage: str = "extract") -> list[str]:
        """List all batch files for a job/entity/stage."""
        prefix = f"{self.prefix}/{job_id}/{entity_type}/{stage}/"
        client = self._get_client()

        if client == "mock":
            return []

        response = client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return [
            f"s3://{self.bucket}/{obj['Key']}"
            for obj in response.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]

    def delete_job_data(self, job_id: str) -> int:
        """Delete all S3 data for a job (used during rollback)."""
        prefix = f"{self.prefix}/{job_id}/"
        client = self._get_client()

        if client == "mock":
            logger.info("mock_s3_delete", prefix=prefix)
            return 0

        response = client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        objects = response.get("Contents", [])
        if not objects:
            return 0

        delete_keys = [{"Key": obj["Key"]} for obj in objects]
        client.delete_objects(Bucket=self.bucket, Delete={"Objects": delete_keys})

        logger.info("s3_job_data_deleted", job_id=job_id, files_deleted=len(delete_keys))
        return len(delete_keys)
