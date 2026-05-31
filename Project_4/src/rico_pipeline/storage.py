from __future__ import annotations

import boto3

from rico_pipeline.config import Settings


def s3_client(settings: Settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_url,
        aws_access_key_id=settings.minio_key,
        aws_secret_access_key=settings.minio_secret,
        region_name="us-east-1",
    )
