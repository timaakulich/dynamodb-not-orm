from dataclasses import dataclass, field

from aiodynamo.expressions import KeyPath

from dynamodb_not_orm import DataModel, F


def test_data():
    @dataclass
    class InnerData(DataModel):
        inner: int

    @dataclass
    class TestDataModel(DataModel):
        __table__ = "test"
        __pk__ = "column1"

        column1: str | None = field(default_factory=lambda: None)
        column2: dict[str, InnerData] | None = field(default_factory=dict)

    assert F(TestDataModel.column1).path == KeyPath("column1", [])
    assert F(TestDataModel.column2["some_key"].inner).path == KeyPath(
        "column2", ["some_key", "inner"]
    )

    obj: TestDataModel = TestDataModel.model_validate(
        {"column1": "C1", "column2": {"some_key": {"inner": 1}}}
    )
    assert obj == TestDataModel(
        column1="C1", column2={"some_key": InnerData(1)}
    )
    assert obj.key == {"column1": "C1"}
