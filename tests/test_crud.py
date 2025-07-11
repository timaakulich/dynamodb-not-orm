from dynamodb_not_orm import DataModel, BaseCRUD


def test_crud():
    class TestDataModel(DataModel):
        __table__ = "test"

    class UserCRUD(BaseCRUD[TestDataModel]): ...

    user_crud = UserCRUD("eu-west-1", "dev")
    assert user_crud.model_cls is TestDataModel
    assert user_crud.table_name == "test"
    assert user_crud.full_table_name("test") == "dev-test"
