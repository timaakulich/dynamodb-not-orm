import importlib.util
import time
from pathlib import Path

from aiodynamo.client import Client
from aiodynamo.models import KeySchema, KeySpec, KeyType, Throughput

MIGRATIONS_DIR = Path.cwd() / "dynamodb_migrations"


async def init_migrations_table(
    client: Client,
    environment: str,
    app_name: str,
) -> str:
    table_name = f"{environment}-{app_name}-migrations"

    try:
        await client.describe_table(table_name)
    except Exception:
        keys = KeySchema(
            hash_key=KeySpec(name="migration_id", type=KeyType.string),
            range_key=KeySpec(name="applied_at", type=KeyType.number),
        )
        await client.create_table(
            name=table_name,
            keys=keys,
            throughput=Throughput(read=1, write=1),
        )
        return f"Created {table_name} table"
    else:
        return f"Table {table_name} already exists"


def create_empty_migration(
    description: str = "auto",
    migrations_dir: Path = MIGRATIONS_DIR,
) -> str:
    migrations_dir.mkdir(exist_ok=True)

    migration_files = sorted(
        p for p in migrations_dir.glob("[0-9]*_*.py")
    )

    if migration_files:
        last_file = migration_files[-1]
        last_number = int(last_file.name.split("_")[0])
    else:
        last_number = 0

    next_number = last_number + 1
    number_str = f"{next_number:04d}"

    safe_description = (
        description.strip().replace(" ", "_").replace("-", "_").lower()
    )
    safe_description = f"_{safe_description}" if safe_description else ""
    migration_id = f"{number_str}{safe_description}"

    filename = f"{migration_id}.py"
    filepath = migrations_dir / filename

    template = f"""\"\"\"
Migration {number_str}: {description}
\"\"\"
from aiodynamo.client import Client
from aiodynamo.models import (
    KeySchema,
    KeySpec,
    KeyType,
    Throughput,
    PayPerRequest
)


migration_id = '{migration_id}'
description = '{description}'


async def upgrade(client: Client, environment: str, app_name: str):
    pass


async def downgrade(client: Client, environment: str, app_name: str):
    pass
"""

    filepath.write_text(template, encoding="utf-8")
    return f"Created migration: {filepath}"


async def run_migrations(
    client: Client,
    environment: str,
    app_name: str,
    migration_number: str | None = None,
    migrations_dir: Path = MIGRATIONS_DIR,
) -> str:
    table_name = f"{environment}-{app_name}-migrations"

    migrations_dir.mkdir(exist_ok=True)
    migration_files = sorted(
        p for p in migrations_dir.glob("[0-9]*_*.py")
    )

    if not migration_files:
        raise RuntimeError("No migration files found")

    applied_migrations = {}
    try:
        async for item in client.scan(table_name):
            applied_migrations[item["migration_id"]] = item["applied_at"]
    except Exception as exc:
        raise RuntimeError(
            f"Could not read from {table_name} table. "
            f"Make sure it exists."
        ) from exc

    migration_data = []
    for file_path in migration_files:
        file_name = file_path.name
        if not file_name.endswith(".py"):
            continue

        parts = file_name.split("_")
        if not parts or not parts[0].isdigit():
            continue

        migration_num = parts[0]
        migration_id = file_name.replace(".py", "")
        migration_data.append(
            {
                "file_path": file_path,
                "number": migration_num,
                "file_name": file_name,
                "migration_id": migration_id,
                "is_applied": migration_id in applied_migrations,
            }
        )

    if not migration_data:
        raise RuntimeError("No valid migration files found")

    if migration_number is None:
        target_migration = migration_data[-1]["number"]
        target_action = "upgrade"
    elif migration_number == "zero":
        target_migration = "zero"
        target_action = "downgrade"
    else:
        target_migration = migration_number
        target_action = None

    target_migration_data = None
    for migration in migration_data:
        if migration["number"] == target_migration:
            target_migration_data = migration
            break

    if target_migration == "zero":
        migrations_to_process = [m for m in migration_data if m["is_applied"]]
        migrations_to_process.reverse()
        action = "downgrade"
    else:
        if not target_migration_data:
            raise RuntimeError(f"Migration {target_migration} not found")

        if target_action is None:
            current_highest = max(
                [int(m["number"]) for m in migration_data if m["is_applied"]],
                default=0,
            )

            if int(target_migration) > current_highest:
                action = "upgrade"
                migrations_to_process = [
                    m
                    for m in migration_data
                    if not m["is_applied"]
                    and int(m["number"]) <= int(target_migration)
                ]
            else:
                action = "downgrade"
                migrations_to_process = [
                    m
                    for m in migration_data
                    if m["is_applied"]
                    and int(m["number"]) > int(target_migration)
                ]
                migrations_to_process.reverse()
        else:
            action = target_action
            if action == "upgrade":
                migrations_to_process = [
                    m
                    for m in migration_data
                    if not m["is_applied"]
                    and int(m["number"]) <= int(target_migration)
                ]
            else:
                migrations_to_process = [
                    m
                    for m in migration_data
                    if m["is_applied"]
                    and int(m["number"]) > int(target_migration)
                ]
                migrations_to_process.reverse()

    if not migrations_to_process:
        return f"No migrations to {action}"

    messages = []
    for migration in migrations_to_process:
        messages.append(
            f"{action.capitalize()[:-1]}ing "
            f"migration {migration['number']}..."
        )

        spec = importlib.util.spec_from_file_location(
            "migration", migration["file_path"]
        )
        migration_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration_module)

        if action == "upgrade":
            await migration_module.upgrade(client, environment, app_name)
            await client.put_item(
                table_name,
                {
                    "migration_id": migration["migration_id"],
                    "applied_at": int(time.time()),
                },
            )
            messages.append(
                f"Applied migration {migration['number']}"
            )
        else:
            await migration_module.downgrade(client, environment, app_name)
            await client.delete_item(
                table_name,
                {
                    "migration_id": migration["migration_id"],
                    "applied_at": applied_migrations[
                        migration["migration_id"]
                    ],
                },
            )
            messages.append(
                f"Reverted migration {migration['number']}"
            )

    messages.append("Migration completed successfully!")
    return "\n".join(messages)
