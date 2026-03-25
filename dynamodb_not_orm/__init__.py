"""
DynamoDB Not ORM - A lightweight DynamoDB ORM alternative
"""

from .contextmanagers import dynamodb
from .crud import BaseCRUD
from .data import DataModel, F

__version__ = "0.1.0"
__all__ = ["BaseCRUD", "DataModel", "F", "dynamodb"]
