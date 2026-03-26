from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from aiodynamo.client import Client, Table
from aiodynamo.credentials import (
    ChainCredentials,
    ContainerMetadataCredentials,
    Credentials,
    EnvironmentCredentials,
    FileCredentials,
    InstanceMetadataCredentialsV1,
    Key,
    StaticCredentials,
)
from aiodynamo.http.httpx import HTTPX
from yarl import URL

default_credentials = ChainCredentials(
    candidates=[
        EnvironmentCredentials(),
        FileCredentials(),
        ContainerMetadataCredentials(),
        InstanceMetadataCredentialsV1(),
    ]
)

httpx_client = httpx.AsyncClient()


def _make_credentials(
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> Credentials:
    if aws_access_key_id and aws_secret_access_key:
        return StaticCredentials(
            key=Key(id=aws_access_key_id, secret=aws_secret_access_key)
        )
    return default_credentials


@asynccontextmanager
async def dynamodb(
    table: str,
    region: str,
    endpoint_url: str | URL | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> AsyncIterator[Table]:
    client = Client(
        HTTPX(httpx_client),
        _make_credentials(aws_access_key_id, aws_secret_access_key),
        region,
        endpoint=URL(endpoint_url) if isinstance(endpoint_url, str) else endpoint_url,
    )
    yield client.table(table)


@asynccontextmanager
async def dynamodb_client(
    region: str,
    endpoint_url: str | URL | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> AsyncIterator[Client]:
    client = Client(
        HTTPX(httpx_client),
        _make_credentials(aws_access_key_id, aws_secret_access_key),
        region,
        endpoint=URL(endpoint_url) if isinstance(endpoint_url, str) else endpoint_url,
    )
    yield client
