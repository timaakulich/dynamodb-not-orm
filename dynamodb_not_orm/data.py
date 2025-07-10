from dataclasses import asdict
from functools import reduce
from typing import Union, Self, Any, Type, ClassVar, Optional, Callable

from aiodynamo.expressions import F as AioF, KeyPath
from pydantic import TypeAdapter
from operator import and_ as _and_
from aiodynamo.expressions import UpdateExpression as BaseUpdateExpression


class UpdateExpression(BaseUpdateExpression):
    def __bool__(self):
        return any((self.set_updates, self.remove, self.add, self.delete))


def and_(*args):
    return reduce(_and_, args)


class F(AioF):

    def __init__(self, root: str | Self | Any, *path: Union[str, int]):
        if isinstance(root, F):
            self.path = root.path
        else:
            self.path = KeyPath(root, list(path))

    def __getattr__(self, item: str) -> Self:
        return F(self.path.root, *self.path.parts, item)

    def __getitem__(self, item: str) -> Self:
        return self.__getattr__(item)

    def set(self, value: Any) -> UpdateExpression:
        if isinstance(value, DataModel):
            value = value.model_dump()
        return super().set(value)

    def set_if_not_exists(self, value: Any) -> UpdateExpression:
        if isinstance(value, DataModel):
            value = value.model_dump()
        return super().set_if_not_exists(value)

    @staticmethod
    def and_(*args: UpdateExpression) -> UpdateExpression:
        return reduce(_and_, args)


class DataModelMeta(type):
    def __getattr__(cls, item) -> F:
        if item in cls.__annotations__:
            return F(item)
        return super().__getattribute__(item)

    def __getitem__(cls, item):
        return cls.__getattr__(item)


class KeyDescriptor:
    def __get__(self, obj, objtype=None) -> Callable[[str, str | None], dict[str, str | float | bytes]] | dict[str, str | float | bytes]:
        if obj is None:
            def create_key(pk: str, sk: str | None = None) -> dict[str, str | float | bytes]:
                _key = {objtype.__pk__: pk}
                if sk and objtype.__sk__:
                    _key[objtype.__sk__] = sk
                return _key
            return create_key
        else:
            _key = {
                objtype.__pk__: getattr(obj, objtype.__pk__),
            }
            if objtype.__sk__:
                _key[objtype.__sk__] = getattr(obj, objtype.__sk__)
            return _key


class DataModel(metaclass=DataModelMeta):
    __table__: ClassVar[str]
    __pk__: ClassVar[str]
    __sk__: ClassVar[Optional[str]] = None
    key = KeyDescriptor()

    def _get_exclude_keys(self) -> set[str]:
        _keys: set = {self.__pk__}
        if self.__sk__:
            _keys.add(self.__sk__)
        return _keys

    @classmethod
    def model_validate(cls, obj: Any) -> Type[Self]:
        return TypeAdapter(cls).validate_python(obj)

    def model_dump(self, exclude: set[str] | None = None) -> dict:
        result = asdict(self)
        for exclude in exclude or set():
            result.pop(exclude, None)
        return result

    def to_update_expression(
            self,
            data: dict | None = None,
            overrides: Optional[dict[str, UpdateExpression]] = None,
            prefix: str = None
    ) -> UpdateExpression:
        data = data or self.model_dump(exclude=self._get_exclude_keys())
        result: dict[str, UpdateExpression] = {}
        for key, value in data.items():
            result[key] = (F(prefix, key) if prefix else F(key)).set(value)
        result.update(overrides or {})
        return and_(*result.values()) if result else UpdateExpression()
