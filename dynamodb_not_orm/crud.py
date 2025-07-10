import time
from contextlib import suppress
from typing import Type, Generic, Optional, List, TypeVar, Any

from aiodynamo.errors import ItemNotFound
from aiodynamo.expressions import (
    ProjectionExpression,
    UpdateExpression,
    KeyCondition,
    Condition,
)
from aiodynamo.models import ReturnValues, Select

from .contextmanagers import dynamodb

__all__ = (
    'BaseCRUD',
)

from .data import DataModel, F

T = TypeVar('T', bound=DataModel)


class BaseCRUD(Generic[T]):
    def __init__(
            self,
            model_cls: Type[T],
            region_name: str,
            environment: str,
            auto_now: bool = False,
            auto_now_add: bool = False,
    ):
        self.table_name = model_cls.__table__
        self.region_name = region_name
        self.environment = environment
        self.model_cls = model_cls
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add

    def full_table_name(self, table_or_index: str) -> str:
        return f'{self.environment}-{table_or_index}'

    async def get(
            self,
            key: dict,
            projection: ProjectionExpression | None = None,
            consistent_read: bool = False,
            raise_exc: bool = False,
    ) -> Optional[T]:
        async with dynamodb(self.full_table_name(self.table_name), self.region_name) as table:
            with suppress(ItemNotFound):
                item = await table.get_item(
                    key,
                    projection=projection,
                    consistent_read=consistent_read
                )
                if item is not None:
                    return self.model_cls.model_validate(item)
        if raise_exc:
            raise KeyError(f'Item not found {key}')
        return None

    async def query(
            self,
            key_condition_expression: KeyCondition,
            start_key: Optional[dict[str, Any]] = None,
            filter_expression: Optional[Condition] = None,
            scan_forward: bool = True,
            index: Optional[str] = None,
            limit: Optional[int] = None,
            projection: Optional[ProjectionExpression] = None,
            select: Select = Select.all_attributes,
            consistent_read: bool = False,
    ) -> List[T]:
        async with dynamodb(self.full_table_name(self.table_name), self.region_name) as table:
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
            start_key: Optional[dict[str, Any]] = None,
            filter_expression: Optional[Condition] = None,
            scan_forward: bool = True,
            index: Optional[str] = None,
            limit: Optional[int] = None,
            projection: Optional[ProjectionExpression] = None,
            select: Select = Select.all_attributes,
            consistent_read: bool = False,
    ) -> List[T]:
        async with dynamodb(self.full_table_name(self.table_name), self.region_name) as table:
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

    async def update(
            self,
            key: dict,
            update_expression: UpdateExpression,
            return_values: ReturnValues = ReturnValues.none,
            condition: Optional[Condition] = None,
    ) -> Optional[T]:
        async with dynamodb(self.full_table_name(self.table_name), self.region_name) as table:
            now = int(time.time())
            if self.auto_now:
                update_expression &= F('updated_at').set(now)
            if self.auto_now_add:
                update_expression &= F('created_at').set_if_not_exists(now)
            item = await table.update_item(
                key, update_expression,
                return_values=return_values,
                condition=condition
            )
            if item:
                return self.model_cls.model_validate(item)
            return None

    async def delete(
            self,
            key: dict,
            return_values: ReturnValues = ReturnValues.none,
            condition: Optional[Condition] = None,
    ) -> Optional[T]:
        async with dynamodb(self.full_table_name(self.table_name), self.region_name) as table:
            with suppress(ItemNotFound):
                item = await table.delete_item(
                    key,
                    return_values=return_values,
                    condition=condition
                )
                if item:
                    return self.model_cls.model_validate(item)
                return None

    async def scan(
            self,
            index: Optional[str] = None,
            limit: Optional[int] = None,
            start_key: Optional[dict[str, Any]] = None,
            projection: Optional[ProjectionExpression] = None,
            filter_expression: Optional[Condition] = None,
            consistent_read: bool = False,
    ) -> List[T]:
        async with dynamodb(self.full_table_name(self.table_name), self.region_name) as table:
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
