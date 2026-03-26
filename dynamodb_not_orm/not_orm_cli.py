
import httpx
import typer
from aiodynamo.client import Client
from aiodynamo.http.httpx import HTTPX
from asyncer import runnify
from yarl import URL

from dynamodb_not_orm.commands import (
    create_empty_migration,
    init_migrations_table,
    run_migrations,
)
from dynamodb_not_orm.contextmanagers import _make_credentials

httpx_client = httpx.AsyncClient()
app = typer.Typer()


def _make_client(
    aws_region: str,
    endpoint_url: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> Client:
    return Client(
        http=HTTPX(httpx_client),
        region=aws_region,
        credentials=_make_credentials(aws_access_key_id, aws_secret_access_key),
        endpoint=URL(endpoint_url) if endpoint_url else None,
    )


@app.command()
@runnify
async def init(
    aws_region: str = typer.Option(...),
    environment: str = typer.Option(...),
    app_name: str = typer.Option(...),
    endpoint_url: str | None = typer.Option(None, help="Custom DynamoDB endpoint URL (e.g. http://localhost:4566 for LocalStack)"),
    aws_access_key_id: str | None = typer.Option(None),
    aws_secret_access_key: str | None = typer.Option(None),
) -> None:
    client = _make_client(aws_region, endpoint_url, aws_access_key_id, aws_secret_access_key)
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
    migration_number: str | None = typer.Argument(
        None,
        help="Migration number (e.g., 0001, 0002) or zero to unapply all",
    ),
    aws_region: str = typer.Option(...),
    environment: str = typer.Option(...),
    app_name: str = typer.Option(...),
    endpoint_url: str | None = typer.Option(None, help="Custom DynamoDB endpoint URL (e.g. http://localhost:4566 for LocalStack)"),
    aws_access_key_id: str | None = typer.Option(None),
    aws_secret_access_key: str | None = typer.Option(None),
) -> None:
    client = _make_client(aws_region, endpoint_url, aws_access_key_id, aws_secret_access_key)
    try:
        result = await run_migrations(
            client, environment, app_name, migration_number
        )
        typer.secho(result, fg=typer.colors.GREEN)
    except RuntimeError as e:
        typer.secho(str(e), fg=typer.colors.RED, err=True)


if __name__ == "__main__":
    app()
