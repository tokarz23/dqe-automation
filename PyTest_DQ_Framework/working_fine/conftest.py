import pytest
import os
import pandas as pd
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.data_quality.data_quality_validation_library import DataQualityLibrary
from src.connectors.file_system.parquet_reader import ParquetReader

def pytest_addoption(parser):
    parser.addoption("--db_host", action="store", default=os.environ.get("DB_HOST", "localhost"), help="Database host")
    parser.addoption("--db_port", action="store", default=os.environ.get("DB_PORT", "5432"), help="Database port")
    parser.addoption("--db_name", action="store", default=os.environ.get("DB_NAME", "postgres"), help="Database name")
    parser.addoption("--db_user", action="store", default=os.environ.get("DB_USER", "postgres"), help="Database user")
    parser.addoption("--db_password", action="store", default=os.environ.get("DB_PASSWORD", ""), help="Database password")
    parser.addoption("--parquet_path_facility_name_min_time_spent", action="store", default="/parquet_data/facility_name_min_time_spent_per_visit_date", help="Path to Parquet file: facility_name_min_time_spent_per_visit_date")
    parser.addoption("--parquet_path_facility_type_avg_time_spent", action="store", default="/parquet_data/facility_type_avg_time_spent_per_visit_date", help="Path to Parquet file: facility_type_avg_time_spent_per_visit_date")
    parser.addoption("--parquet_path_patient_sum_treatment_cost", action="store", default="/parquet_data/patient_sum_treatment_cost_per_facility_type", help="Path to Parquet file: patient_sum_treatment_cost_per_facility_type")
    parser.addoption("--mapping_path", action="store", default="src/data_quality/mapping.yaml", help="Path to mapping YAML")

def pytest_configure(config):
    required_options = [
        "--db_user", "--db_password"
    ]
    for option in required_options:
        if not config.getoption(option) and not os.environ.get(option.upper()):
            pytest.fail(f"Missing required option or environment variable: {option}")

@pytest.fixture(scope='session')
def db_connection(request):
    db_host = request.config.getoption("--db_host")
    db_name = request.config.getoption("--db_name")
    db_user = request.config.getoption("--db_user")
    db_password = request.config.getoption("--db_password")
    db_port = int(request.config.getoption("--db_port"))
    try:
        with PostgresConnectorContextManager(
            db_host=db_host,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            db_port=db_port) as db_connector:
            yield db_connector
    except Exception as e:
        pytest.fail(f"Failed to initialize PostgresConnectorContextManager: {e}")


@pytest.fixture(scope='session')
def parquet_reader():
    try:
        reader = ParquetReader()
        yield reader
    except Exception as e:
        pytest.fail(f"Failed to initialize ParquetReader: {e}")

# --- SRC Layer Fixtures ---
@pytest.fixture(scope='module')
def src_facilities(db_connection):
    query = "SELECT * FROM src_generated_facilities"
    return db_connection.get_data_sql(query)

@pytest.fixture(scope='module')
def src_patients(db_connection):
    query = "SELECT * FROM src_generated_patients"
    return db_connection.get_data_sql(query)

@pytest.fixture(scope='module')
def src_visits(db_connection):
    query = "SELECT * FROM src_generated_visits"
    return db_connection.get_data_sql(query)

# --- 3NF Layer Fixtures ---
@pytest.fixture(scope='module')
def nf3_facilities(db_connection):
    query = "SELECT * FROM facilities"
    return db_connection.get_data_sql(query)

@pytest.fixture(scope='module')
def nf3_patients(db_connection):
    query = "SELECT * FROM patients"
    return db_connection.get_data_sql(query)

@pytest.fixture(scope='module')
def nf3_visits(db_connection):
    query = "SELECT * FROM visits"
    return db_connection.get_data_sql(query)

# --- Parquet Output Fixtures ---
@pytest.fixture(scope='module')
def parquet_facility_name_min_time_spent(request, parquet_reader):
    path = request.config.getoption("--parquet_path_facility_name_min_time_spent")
    if not os.path.exists(path):
        pytest.skip(f"Parquet file not found: {path}")
    return parquet_reader.read_parquet(path)

@pytest.fixture(scope='module')
def parquet_facility_type_avg_time_spent(request, parquet_reader):
    path = request.config.getoption("--parquet_path_facility_type_avg_time_spent")
    if not os.path.exists(path):
        pytest.skip(f"Parquet file not found: {path}")
    return parquet_reader.read_parquet(path)

@pytest.fixture(scope='module')
def parquet_patient_sum_treatment_cost(request, parquet_reader):
    path = request.config.getoption("--parquet_path_patient_sum_treatment_cost")
    if not os.path.exists(path):
        pytest.skip(f"Parquet file not found: {path}")
    return parquet_reader.read_parquet(path)

# --- DQ Library Fixture ---
@pytest.fixture(scope='session')
def dq_library():
    return DataQualityLibrary()



