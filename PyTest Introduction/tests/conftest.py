import pytest
import pandas as pd

# Fixture to read the CSV file
@pytest.fixture(scope='session')
def path_to_file():
    return 'src/data/data.csv'

@pytest.fixture(scope='session')
def csv_df(path_to_file):
    df = pd.read_csv(path_to_file)
    return df

# Fixture to validate the schema of the file
@pytest.fixture(scope='session')
def actual_schema(csv_df):
    return list(csv_df.columns)

@pytest.fixture(scope='session')
def expected_schema():
    return ['id', 'name', 'age', 'email']

@pytest.fixture(scope='session')
def validate_schema(actual_schema, expected_schema):
    assert actual_schema == expected_schema, (
        f"Expected columns: {expected_schema}, but received: {actual_schema}")

# Pytest hook to mark unmarked tests with a custom mark
def pytest_collection_modifyitems(session, config, items):
    for item in items:
        if not item.own_markers:
            item.add_marker("unmarked")
