"""
DynamoDB Not ORM - A lightweight DynamoDB ORM alternative
"""
from contextlib import suppress

with suppress(ImportError):
    from .data import DataModel, F
    from .crud import BaseCRUD
    from .contextmanagers import dynamodb


__version__ = "0.1.0"
__all__ = ["DataModel", "F", "BaseCRUD", "dynamodb"]
