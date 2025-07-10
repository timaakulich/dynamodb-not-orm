from pathlib import Path
import importlib.util
import time
import os
from typing import Optional

import httpx
import typer
from aiodynamo.client import Client
from aiodynamo.credentials import (
    ChainCredentials,
    EnvironmentCredentials,
    FileCredentials,
    ContainerMetadataCredentials,
    InstanceMetadataCredentialsV1
)
from aiodynamo.http.httpx import HTTPX
from aiodynamo.models import KeySchema, KeySpec, KeyType, Throughput
from asyncer import runnify


# Use current working directory for migrations
MIGRATIONS_DIR = Path.cwd() / 'dynamodb_migrations'

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


@app.command()
@runnify
async def init(
        aws_region: str = typer.Option(...),
        environment: str = typer.Option(...),
        app_name: str = typer.Option(...),
) -> None:
    table_name = f'{environment}-{app_name}-migrations'
    client = Client(
        http=HTTPX(httpx_client),
        region=aws_region,
        credentials=credentials,
    )

    try:
        await client.describe_table(table_name)
    except Exception as exc:
        typer.echo(str(exc))
        keys = KeySchema(
            hash_key=KeySpec(name='migration_id', type=KeyType.string),
            range_key=KeySpec(name='applied_at', type=KeyType.number)
        )
        await client.create_table(
            name=table_name,
            keys=keys,
            throughput=Throughput(read=1, write=1),
        )

        typer.echo(f'Created {table_name} table', color=True)
    else:
        typer.echo(f'Table {table_name} already exists', color=True)


@app.command()
@runnify
async def create_empty(description: str = typer.Argument('auto')):
    MIGRATIONS_DIR.mkdir(exist_ok=True)

    migration_files = sorted(
        p for p in MIGRATIONS_DIR.glob('[0-9]*_*.py')
    )

    if migration_files:
        last_file = migration_files[-1]
        last_number = int(last_file.name.split('_')[0])
    else:
        last_number = 0

    next_number = last_number + 1
    number_str = f'{next_number:04d}'

    safe_description = (
        description.strip()
        .replace(' ', '_')
        .replace('-', '_')
        .lower()
    )
    safe_description = f'_{safe_description}' if safe_description else ''
    migration_id= f'{number_str}{safe_description}'

    filename = f'{migration_id}.py'
    filepath = MIGRATIONS_DIR / filename

    template = f'''\"\"\"
Migration {number_str}: {description}
\"\"\"
from aiodynamo.client import Client


migration_id = '{migration_id}'
description = '{description}'


async def upgrade(client: Client):
    pass


async def downgrade(client: Client):
    pass

'''

    filepath.write_text(template, encoding='utf-8')
    typer.echo(f'Created migration: {filepath}')


@app.command()
@runnify
async def migrate(
        migration_number: Optional[str] = typer.Argument(
            None,
            help='Migration number (e.g., 0001, 0002) or zero to unapply all'
        ),
        aws_region: str = typer.Option(...),
        environment: str = typer.Option(...),
        app_name: str = typer.Option(...),
) -> None:
    table_name = f'{environment}-{app_name}-migrations'
    client = Client(
        http=HTTPX(httpx_client),
        region=aws_region,
        credentials=credentials,
    )

    MIGRATIONS_DIR.mkdir(exist_ok=True)
    migration_files = sorted(
        p for p in MIGRATIONS_DIR.glob('[0-9]*_*.py')
    )

    if not migration_files:
        typer.secho(
            'No migration files found',
            fg=typer.colors.RED,
            err=True
        )
        return

    applied_migrations = {}
    try:
        async for item in client.scan(table_name):
            applied_migrations[item['migration_id']] = item['applied_at']
    except Exception:
        typer.secho(
            f'Could not read from {table_name} table.'
            f'Make sure it exists.',
            fg=typer.colors.RED,
            err=True
        )
        return

    migration_data = []
    for file_path in migration_files:
        file_name = file_path.name
        if not file_name.endswith('.py'):
            continue
            
        parts = file_name.split('_')
        if not parts or not parts[0].isdigit():
            continue
            
        migration_num = parts[0]
        migration_id = file_name.replace('.py', '')
        migration_data.append({
            'file_path': file_path,
            'number': migration_num,
            'file_name': file_name,
            'migration_id': migration_id,
            'is_applied': migration_id in applied_migrations
        })

    if not migration_data:
        typer.secho(
            'No valid migration files found',
            fg=typer.colors.RED,
            err=True,
        )
        return

    if migration_number is None:
        target_migration = migration_data[-1]['number']
        target_action = 'upgrade'
    elif migration_number == 'zero':
        target_migration = 'zero'
        target_action = 'downgrade'
    else:
        target_migration = migration_number
        target_action = None

    target_migration_data = None
    for migration in migration_data:
        if migration['number'] == target_migration:
            target_migration_data = migration
            break

    if target_migration == 'zero':
        migrations_to_process = [m for m in migration_data if m['is_applied']]
        migrations_to_process.reverse()
        action = 'downgrade'
    else:
        if not target_migration_data:
            typer.secho(
                f'Migration {target_migration} not found',
                fg=typer.colors.RED,
                err=True
            )
            return

        if target_action is None:
            current_highest = max([
                int(m['number']) for m in migration_data if m['is_applied']],
                default=0
            )
            
            if int(target_migration) > current_highest:
                action = 'upgrade'
                migrations_to_process = [
                    m for m in migration_data
                    if not m['is_applied'] and int(m['number']) <= int(target_migration)  # noqa
                ]
            else:
                action = 'downgrade'
                migrations_to_process = [
                    m for m in migration_data
                    if m['is_applied'] and int(m['number']) > int(target_migration)  # noqa
                ]
                migrations_to_process.reverse()
        else:
            action = target_action
            if action == 'upgrade':
                migrations_to_process = [
                    m for m in migration_data
                    if not m['is_applied'] and int(m['number']) <= int(target_migration) # noqa
                ]
            else:  # downgrade
                migrations_to_process = [
                    m for m in migration_data
                    if m['is_applied'] and int(m['number']) > int(target_migration)  # noqa
                ]
                migrations_to_process.reverse()

    if not migrations_to_process:
        typer.secho(
            f'No migrations to {action}',
            fg=typer.colors.YELLOW
        )
        return

    for migration in migrations_to_process:
        typer.secho(
            f'{action.capitalize()[:-1]}ing '
            f'migration {migration['number']}...',
            fg=typer.colors.BLUE
        )
        
        spec = importlib.util.spec_from_file_location(
            'migration', migration['file_path'])
        migration_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration_module)

        try:
            if action == 'upgrade':
                await migration_module.upgrade(client)
                await client.put_item(
                    table_name,
                    {
                        'migration_id': migration['migration_id'],
                        'applied_at': int(time.time())
                    }
                )
                typer.secho(
                    f'Applied migration {migration['number']}',
                    fg=typer.colors.GREEN
                )
            else:
                await migration_module.downgrade(client)
                await client.delete_item(
                    table_name,
                    {
                        'migration_id': migration['migration_id'],
                        'applied_at': applied_migrations[migration['migration_id']]  # noqa
                    }
                )
                typer.secho(
                    f'Reverted migration {migration['number']}',
                    fg=typer.colors.GREEN,
                )
                
        except Exception as e:
            typer.secho(
                f'Failed to {action} migration '
                f'{migration['number']}: {e}',
                fg=typer.colors.RED,
                err=True
            )
            return

    typer.secho(
        f'Migration completed successfully!',
        fg=typer.colors.GREEN
    )


if __name__ == '__main__':
    app()
