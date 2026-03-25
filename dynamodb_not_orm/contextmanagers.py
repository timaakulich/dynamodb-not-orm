from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from aiodynamo.client import Client, Table
from aiodynamo.credentials import (
    ChainCredentials,
    ContainerMetadataCredentials,
    EnvironmentCredentials,
    FileCredentials,
    InstanceMetadataCredentialsV1,
)
from aiodynamo.http.httpx import HTTPX

credentials = ChainCredentials(
    candidates=[
        EnvironmentCredentials(),
        FileCredentials(),
        ContainerMetadataCredentials(),
        InstanceMetadataCredentialsV1(),
    ]
)

httpx_client = httpx.AsyncClient()


@asynccontextmanager
async def dynamodb(table: str, region: str) -> AsyncIterator[Table]:
    client = Client(
        HTTPX(httpx_client),
        credentials,
        region,
    )
    yield client.table(table)


@asynccontextmanager
async def dynamodb_client(region: str) -> AsyncIterator[Client]:
    client = Client(
        HTTPX(httpx_client),
        credentials,
        region,
    )
    yield client
