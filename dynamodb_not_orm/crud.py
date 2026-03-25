import time
from contextlib import suppress
from typing import Any, TypeVar, get_args

from aiodynamo.errors import ItemNotFound
from aiodynamo.expressions import (
    Condition,
    KeyCondition,
    ProjectionExpression,
    UpdateExpression,
)
from aiodynamo.models import BatchGetRequest, ReturnValues, Select

from .contextmanagers import dynamodb, dynamodb_client

__all__ = ("BaseCRUD",)

import builtins

from .data import DataModel, F, to_update_expression

T = TypeVar("T", bound=DataModel)


class BaseCRUD[T: DataModel]:
    model_cls: type[T] = None

    def __init_subclass__(cls):
        super().__init_subclass__()
        if cls.model_cls is None:
            cls.model_cls = get_args(cls.__orig_bases__[0])[0]

    def __init__(
        self,
        region_name: str,
        environment: str,
        auto_now: bool = False,
        auto_now_add: bool = False,
    ):
        self.table_name = self.model_cls.__table__
        self.region_name = region_name
        self.environment = environment
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add

    def full_table_name(self, table_or_index: str) -> str:
        return f"{self.environment}-{table_or_index}"

    async def get(
        self,
        key: dict,
        projection: ProjectionExpression | None = None,
        consistent_read: bool = False,
        raise_exc: bool = False,
    ) -> T | None:
        async with dynamodb(
            self.full_table_name(self.table_name), self.region_name
        ) as table:
            with suppress(ItemNotFound):
                item = await table.get_item(
                    key, projection=projection, consistent_read=consistent_read
                )
                if item is not None:
                    return self.model_cls.model_validate(item)
        if raise_exc:
            raise KeyError(f"Item not found {key}")
        return None

    async def query(
        self,
        key_condition_expression: KeyCondition,
        start_key: dict[str, Any] | None = None,
        filter_expression: Condition | None = None,
        scan_forward: bool = True,
        index: str | None = None,
        limit: int | None = None,
        projection: ProjectionExpression | None = None,
        select: Select = Select.all_attributes,
        consistent_read: bool = False,
    ) -> list[T]:
        async with dynamodb(
            self.full_table_name(self.table_name), self.region_name
        ) as table:
            result = []
            page = await table.query_single_page(
                key_condition_expression,
                start_key=start_key,
                filter_expression=filter_expression,
                scan_forward=scan_forward,
                index=index,
                limit=limit,
                projection=projection,
                select=select,
                consistent_read=consistent_read,
            )
            for item in page.items:
                result.append(self.model_cls.model_validate(item))
            return result

    async def list(
        self,
        key_condition_expression: KeyCondition,
        start_key: dict[str, Any] | None = None,
        filter_expression: Condition | None = None,
        scan_forward: bool = True,
        index: str | None = None,
        limit: int | None = None,
        projection: ProjectionExpression | None = None,
        select: Select = Select.all_attributes,
        consistent_read: bool = False,
    ) -> list[T]:
        async with dynamodb(
            self.full_table_name(self.table_name), self.region_name
        ) as table:
            result = []
            async for item in table.query(
                key_condition_expression,
                start_key=start_key,
                filter_expression=filter_expression,
                scan_forward=scan_forward,
                index=index,
                limit=limit,
                projection=projection,
                select=select,
                consistent_read=consistent_read,
            ):
                result.append(self.model_cls.model_validate(item))
            return result

    async def page(
            self,
            key_condition_expression: KeyCondition,
            start_key: dict[str, Any] | None = None,
            filter_expression: Condition | None = None,
            scan_forward: bool = True,
            index: str | None = None,
            limit: int | None = None,
            projection: ProjectionExpression | None = None,
            select: Select = Select.all_attributes,
            consistent_read: bool = False,
    ) -> tuple[dict | None, builtins.list[T]]:
        async with dynamodb(
            self.full_table_name(self.table_name), self.region_name
        ) as table:
            result = await table.query_single_page(
                key_condition_expression,
                start_key=start_key,
                filter_expression=filter_expression,
                scan_forward=scan_forward,
                index=index,
                limit=limit,
                projection=projection,
                select=select,
                consistent_read=consistent_read,
            )
            return result.last_evaluated_key, [
                self.model_cls.model_validate(item)
                for item in result.items
            ]

    async def create(
        self,
        obj: T,
        return_values: ReturnValues = ReturnValues.none,
        condition: Condition | None = None,
    ) -> T | None:
        return await self.update(
            obj.key,
            to_update_expression(
                obj.model_dump(
                    exclude_key=True, exclude={"updated_at", "created_at"}
                )
            ),
            return_values=return_values,
            condition=condition,
        )

    async def update(
        self,
        key: dict,
        update_expression: UpdateExpression,
        return_values: ReturnValues = ReturnValues.none,
        condition: Condition | None = None,
    ) -> T | None:
        async with dynamodb(
            self.full_table_name(self.table_name), self.region_name
        ) as table:
            now = int(time.time())
            if self.auto_now:
                update_expression &= F("updated_at").set(now)
            if self.auto_now_add:
                update_expression &= F("created_at").set_if_not_exists(now)
            item = await table.update_item(
                key,
                update_expression,
                return_values=return_values,
                condition=condition,
            )
            if item:
                return self.model_cls.model_validate(item)
            return None

    async def delete(
        self,
        key: dict,
        return_values: ReturnValues = ReturnValues.none,
        condition: Condition | None = None,
    ) -> T | None:
        async with dynamodb(
            self.full_table_name(self.table_name), self.region_name
        ) as table:
            with suppress(ItemNotFound):
                item = await table.delete_item(
                    key, return_values=return_values, condition=condition
                )
                if item:
                    return self.model_cls.model_validate(item)
                return None

    async def scan(
        self,
        index: str | None = None,
        limit: int | None = None,
        start_key: dict[str, Any] | None = None,
        projection: ProjectionExpression | None = None,
        filter_expression: Condition | None = None,
        consistent_read: bool = False,
    ) -> builtins.list[T]:
        async with dynamodb(
            self.full_table_name(self.table_name), self.region_name
        ) as table:
            result = []
            async for item in table.scan(
                index=index,
                limit=limit,
                start_key=start_key,
                projection=projection,
                filter_expression=filter_expression,
                consistent_read=consistent_read,
            ):
                result.append(self.model_cls.model_validate(item))
            return result

    async def batch_get(
            self,
            keys: builtins.list[dict[str, Any]],
            projection: ProjectionExpression | None = None,
            consistent_read: bool = False,
    ) -> builtins.list[T]:
        async with dynamodb_client(self.region_name) as client:
            table_name = self.full_table_name(self.table_name)
            result = await client.batch_get(request={
                table_name: BatchGetRequest(
                    keys=keys,
                    projection=projection,
                    consistent_read=consistent_read,
                )
            })
            return [
                self.model_cls.model_validate(item)
                for item in result.items[table_name]
            ]
