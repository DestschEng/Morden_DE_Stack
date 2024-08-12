from dagster import Definitions, load_assets_from_modules
from .resources.minio_io_manager import MinIOIOManager
from .assets import *
all_assets = load_assets_from_modules([assets])

MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}

defs = Definitions(
    assets=all_assets,
    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),      
    },
)
