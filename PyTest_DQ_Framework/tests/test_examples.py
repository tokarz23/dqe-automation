"""
Description: Data Quality checks ...
Requirement(s): TICKET-1234
Author(s): Name Surname
"""

import pytest


@pytest.fixture(scope='module')
def source_data(db_connection):
    source_query = """
    SELECT * from visits
    """
    source_data = db_connection.get_data_sql(source_query)
    return source_data


@pytest.fixture(scope='module')
def target_data(parquet_reader):
    #target_path = '/parquet_data/facility_name_min_time_spent_per_visit_date'
    target_path = '/Users/tomasz_tokarzewski/Documents/DQE_repository/dqe-automation/PyTest DQ Framework/local_parquet_files/facility_name_min_time_spent_per_visit_date'
    target_data = parquet_reader.read_parquet(target_path)
    return target_data


@pytest.mark.example
def test_check_dataset_is_not_empty(target_data, dq_library):
    dq_library.check_dataset_is_not_empty(target_data)


@pytest.mark.example
def test_check_count(source_data, target_data, dq_library):
    dq_library.check_count(source_data, target_data)