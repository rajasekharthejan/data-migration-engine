"""AWS Glue job manager for large-scale data transformations.

Manages Glue ETL jobs that run Apache Spark transformations on S3-staged data.
Each Glue job applies entity-specific business rules and produces Parquet output.
"""

import time
from typing import Any, Optional

import structlog

logger = structlog.get_logger(__name__)


class GlueTransformer:
    """Orchestrate AWS Glue ETL jobs for data transformation.

    Pipeline:
        S3 (extracted CSV/Parquet) → Glue Job (Spark transform) → S3 (transformed Parquet)

    Each Glue job:
    1. Reads source data from S3 staging bucket
    2. Applies entity-specific field transformations
    3. Runs business rule validation
    4. Writes validated output to S3 transform bucket
    5. Reports metrics (records processed, validation failures, duration)
    """

    def __init__(
        self,
        region: str = "us-east-1",
        glue_database: str = "migration_transforms",
        iam_role: str = "",
        staging_bucket: str = "migration-staging",
    ):
        self.region = region
        self.glue_database = glue_database
        self.iam_role = iam_role
        self.staging_bucket = staging_bucket
        self._client = None

    def _get_client(self) -> Any:
        """Get or create boto3 Glue client."""
        if self._client is None:
            try:
                import boto3

                self._client = boto3.client("glue", region_name=self.region)
            except Exception as e:
                logger.warning("glue_client_mock", error=str(e))
                self._client = "mock"
        return self._client

    def create_job(
        self,
        job_name: str,
        script_location: str,
        entity_type: str,
        extra_args: Optional[dict[str, str]] = None,
    ) -> str:
        """Create or update a Glue ETL job."""
        client = self._get_client()

        default_args = {
            "--job-language": "python",
            "--enable-metrics": "",
            "--enable-continuous-cloudwatch-log": "true",
            "--entity_type": entity_type,
            "--staging_bucket": self.staging_bucket,
            "--glue_database": self.glue_database,
        }
        if extra_args:
            default_args.update(extra_args)

        if client == "mock":
            logger.info("mock_glue_job_created", job_name=job_name, entity_type=entity_type)
            return job_name

        try:
            client.create_job(
                Name=job_name,
                Role=self.iam_role,
                Command={
                    "Name": "glueetl",
                    "ScriptLocation": script_location,
                    "PythonVersion": "3",
                },
                DefaultArguments=default_args,
                GlueVersion="4.0",
                NumberOfWorkers=10,
                WorkerType="G.1X",
                Timeout=120,  # minutes
            )
        except client.exceptions.AlreadyExistsException:
            client.update_job(
                JobName=job_name,
                JobUpdate={
                    "Role": self.iam_role,
                    "Command": {
                        "Name": "glueetl",
                        "ScriptLocation": script_location,
                        "PythonVersion": "3",
                    },
                    "DefaultArguments": default_args,
                },
            )

        logger.info("glue_job_created", job_name=job_name, entity_type=entity_type)
        return job_name

    def run_job(
        self,
        job_name: str,
        source_path: str,
        output_path: str,
        arguments: Optional[dict[str, str]] = None,
    ) -> str:
        """Start a Glue job run and return the run ID."""
        client = self._get_client()

        run_args = {
            "--source_path": source_path,
            "--output_path": output_path,
        }
        if arguments:
            run_args.update(arguments)

        if client == "mock":
            run_id = f"jr_mock_{int(time.time())}"
            logger.info("mock_glue_job_started", job_name=job_name, run_id=run_id)
            return run_id

        response = client.start_job_run(
            JobName=job_name,
            Arguments=run_args,
        )
        run_id = response["JobRunId"]
        logger.info("glue_job_started", job_name=job_name, run_id=run_id)
        return run_id

    def wait_for_completion(
        self,
        job_name: str,
        run_id: str,
        poll_interval: int = 30,
        timeout: int = 7200,
    ) -> dict[str, Any]:
        """Poll until a Glue job run completes. Returns run metadata."""
        client = self._get_client()
        start = time.monotonic()

        if client == "mock":
            return {
                "JobRunState": "SUCCEEDED",
                "ExecutionTime": 120,
                "DPUSeconds": 600.0,
            }

        while True:
            elapsed = time.monotonic() - start
            if elapsed > timeout:
                raise TimeoutError(f"Glue job {job_name}/{run_id} timed out after {timeout}s")

            response = client.get_job_run(JobName=job_name, RunId=run_id)
            run = response["JobRun"]
            state = run["JobRunState"]

            logger.info("glue_job_poll", job_name=job_name, run_id=run_id, state=state)

            if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"):
                if state != "SUCCEEDED":
                    error_msg = run.get("ErrorMessage", "Unknown error")
                    logger.error("glue_job_failed", state=state, error=error_msg)
                    raise RuntimeError(f"Glue job {job_name} {state}: {error_msg}")
                return {
                    "JobRunState": state,
                    "ExecutionTime": run.get("ExecutionTime", 0),
                    "DPUSeconds": run.get("DPUSeconds", 0),
                }

            time.sleep(poll_interval)

    def run_transformation(
        self,
        entity_type: str,
        source_path: str,
        output_path: str,
        job_name: Optional[str] = None,
    ) -> dict[str, Any]:
        """High-level: create job, run it, wait for completion."""
        if job_name is None:
            job_name = f"migration-transform-{entity_type}"

        script_location = f"s3://{self.staging_bucket}/scripts/{entity_type}_transform.py"

        self.create_job(job_name, script_location, entity_type)
        run_id = self.run_job(job_name, source_path, output_path)
        result = self.wait_for_completion(job_name, run_id)

        logger.info(
            "transformation_complete",
            entity_type=entity_type,
            execution_time=result["ExecutionTime"],
            dpu_seconds=result["DPUSeconds"],
        )

        return {
            "job_name": job_name,
            "run_id": run_id,
            **result,
        }
