import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio
from minio.error import S3Error
import io
import logging
# Initialize logger
logger = logging.getLogger(__name__)

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=os.getenv('MINIO_ENDPOINT', config.get("endpoint_url")),
        access_key=os.getenv('MINIO_ACCESS_KEY', config.get("aws_access_key_id")),
        secret_key=os.getenv('MINIO_SECRET_KEY', config.get("aws_secret_access_key")),
        secure=config.get("secure", False),
    )
    try:
        yield client
    except S3Error as e:
        logger.error("MinIO connection error", exc_info=True)
        raise RuntimeError("Failed to connect to MinIO") from e

class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = os.path.join(
            "tmp", f"file-{datetime.today().strftime('%Y%m%d%H%M%S')}-{'-'.join(context.asset_key.path)}.parquet"
        )
        return f"{key}.pq", tmp_file_path
    def _check_config(self):
        required_keys = ["endpoint_url", "bucket", "aws_access_key_id", "aws_secret_access_key"]
        for key in required_keys:
            if key not in self._config:
                raise ValueError(f"Missing required configuration key: {key}")
        logger.info("MinIO configuration validated successfully")

    def _upload_to_minio(self, key_name: str, df: pd.DataFrame):
        with connect_minio(self._config) as client:
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer)
            parquet_buffer.seek(0)
            client.put_object(
                bucket_name=self._config["bucket"],
                object_name=key_name,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes
            )
        logger.info(f"Successfully uploaded {key_name} to MinIO bucket {self._config['bucket']}")

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        if obj.empty:
            logger.warning("DataFrame is empty. Skipping upload to MinIO.")
            return

        key_name, _ = self._get_path(context)
        try:
            self._upload_to_minio(key_name, obj)
            context.add_output_metadata({
                "path": key_name,
                "records": len(obj),
            })
        except Exception as e:
            logger.error(f"Failed to upload {key_name} to MinIO: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to handle output for {key_name}") from e

    def load_input(self, context: InputContext) -> pd.DataFrame:
        key_name, _ = self._get_path(context)
        try:
            with connect_minio(self._config) as client:
                response = client.get_object(
                    bucket_name=self._config["bucket"],
                    object_name=key_name
                )
                df = pd.read_parquet(io.BytesIO(response.data))
                logger.info(f"Successfully loaded {key_name} from MinIO bucket {self._config['bucket']}")
                return df
        except Exception as e:
            logger.error(f"Failed to load {key_name} from MinIO: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to load input for {key_name}") from e