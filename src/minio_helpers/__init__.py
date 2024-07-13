import io
from logging import Logger
from typing import Any

import orjson
from minio import Minio


def minio_create_bucket(
    bucket_name: str,
    minio_client: Minio,
    logger: Logger,
) -> None:
    if not minio_client.bucket_exists(bucket_name=bucket_name):
        minio_client.make_bucket(bucket_name=bucket_name)
        logger.info(f"minio bucket {bucket_name=} created")


def minio_put_object_in_json(
    minio_client: Minio,
    logger: Logger,
    bucket_name: str,
    object_name: str,
    obj: Any,
) -> None:
    minio_create_bucket(
        bucket_name=bucket_name,
        minio_client=minio_client,
        logger=logger,
    )

    obj_json = orjson.dumps(obj, option=orjson.OPT_INDENT_2)

    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=io.BytesIO(obj_json),
        length=len(obj_json),
        content_type="application/json",
    )
    logger.info(f"minio put to {object_name=} succeed")


def minio_fput_object(
    minio_client: Minio,
    logger: Logger,
    bucket_name: str,
    object_name: str,
    file_path: str,
    content_type: str,
) -> None:
    minio_create_bucket(
        bucket_name=bucket_name,
        minio_client=minio_client,
        logger=logger,
    )

    minio_client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=file_path,
        content_type=content_type,
    )
    logger.info(f"minio put to {object_name=} succeed")
