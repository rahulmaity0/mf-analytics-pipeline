"""Thin wrapper over the S3 API (SeaweedFS locally, AWS S3 in the cloud)."""
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from mfpipeline.config import Settings


class LakeStorage:
    def __init__(self, settings: Settings, client=None):
        self.bucket = settings.lake_bucket
        self.client = client or boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
        )

    def ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
                raise
            self.client.create_bucket(Bucket=self.bucket)

    def put_bytes(self, key: str, body: bytes, content_type: str = "text/plain") -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=content_type)

    def list_keys(self, prefix: str) -> list[str]:
        keys = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys
