# DynamoDB Not ORM

[![PyPI version](https://badge.fury.io/py/dynamodb-not-orm.svg)](https://badge.fury.io/py/dynamodb-not-orm)
[![Python](https://img.shields.io/pypi/pyversions/dynamodb-not-orm.svg)](https://pypi.org/project/dynamodb-not-orm/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/timaakulich/dynamodb-not-orm/actions/workflows/tests.yml/badge.svg)](https://github.com/timaakulich/dynamodb-not-orm/actions/workflows/tests.yml)

A lightweight DynamoDB ORM alternative that provides a simple way to work with DynamoDB without the complexity of a full ORM.

## Features

- Simple data model definition with dataclasses
- Type-safe field access and updates
- Migration system for schema changes
- CLI tools for database operations

## Installation

```bash
pip install dynamodb-not-orm
```

## Quick Start

```python
from dataclasses import dataclass, field
from dynamodb_not_orm import DataModel, F
from aiodynamo.models import ReturnValues


@dataclass
class UserProfile(DataModel):
    full_name: str | None = None


@dataclass
class UserDataModel(DataModel):
    __table__ = "users"
    __pk__ = "user_id"
    __sk__ = "email"

    user_id: str | None = None
    email: str | None = None
    name: str | None = None
    age: int | None = None
    profile: UserProfile | None = field(default_factory=UserProfile)


# Create a user
user = UserDataModel(user_id="123", email="user@example.com", name="John Doe", age=30)

# Get the key for DynamoDB operations
key = user.key  # {'user_id': '123', 'email': 'user@example.com'}

# Create a key for a specific user
key = UserDataModel.key("123", "user@example.com")  # {'user_id': '123', 'email': 'user@example.com'}

# create CRUD model
from dynamodb_not_orm import BaseCRUD


class UserCRUD(BaseCRUD[UserDataModel]):
    ...

user_crud = UserCRUD('eu-east-1', 'dev')
user = await user_crud.update(user.key, user.to_update_expression(), return_values=ReturnValues.all_new)
await user_crud.update(user.key, F(UserDataModel.profile.full_name).set("John Doe"))
await user_crud.update(
    user.key, F.and_(
        F(UserDataModel.age).set(18),
        F(UserDataModel.profile.full_name).set("Jane Doe"),
    )
)
```

## CLI Usage

Initialize the migration system:
```bash
not-orm init --aws-region us-east-1 --environment dev --app-name myapp
```

Create a new migration:
```bash
not-orm create-empty "add user table"
```

Run migrations:
```bash
not-orm migrate --aws-region us-east-1 --environment dev --app-name myapp
```

## License

MIT License 