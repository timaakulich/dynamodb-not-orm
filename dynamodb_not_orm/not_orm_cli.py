from typing import Optional

import httpx
import typer
from aiodynamo.client import Client
from aiodynamo.credentials import (
    ChainCredentials,
    ContainerMetadataCredentials,
    EnvironmentCredentials,
    FileCredentials,
    InstanceMetadataCredentialsV1,
)
from aiodynamo.http.httpx import HTTPX
from asyncer import runnify

from dynamodb_not_orm.commands import (
    create_empty_migration,
    init_migrations_table,
    run_migrations,
)

httpx_client = httpx.AsyncClient()
app = typer.Typer()

credentials = ChainCredentials(
    candidates=[
        EnvironmentCredentials(),
        FileCredentials(),
        ContainerMetadataCredentials(),
        InstanceMetadataCredentialsV1(),
    ]
)


def _make_client(aws_region: str) -> Client:
    return Client(
        http=HTTPX(httpx_client),
        region=aws_region,
        credentials=credentials,
    )


@app.command()
@runnify
async def init(
    aws_region: str = typer.Option(...),
    environment: str = typer.Option(...),
    app_name: str = typer.Option(...),
) -> None:
    client = _make_client(aws_region)
    result = await init_migrations_table(client, environment, app_name)
    typer.echo(result, color=True)


@app.command()
@runnify
async def create_empty(description: str = typer.Argument("auto")):
    result = create_empty_migration(description)
    typer.echo(result)


@app.command()
@runnify
async def migrate(
    migration_number: Optional[str] = typer.Argument(
        None,
        help="Migration number (e.g., 0001, 0002) or zero to unapply all",
    ),
    aws_region: str = typer.Option(...),
    environment: str = typer.Option(...),
    app_name: str = typer.Option(...),
) -> None:
    client = _make_client(aws_region)
    try:
        result = await run_migrations(
            client, environment, app_name, migration_number
        )
        typer.secho(result, fg=typer.colors.GREEN)
    except RuntimeError as e:
        typer.secho(str(e), fg=typer.colors.RED, err=True)


if __name__ == "__main__":
    app()
