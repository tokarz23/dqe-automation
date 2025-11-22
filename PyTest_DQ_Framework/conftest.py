import os
import pytest
import pandas as pd

from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.connectors.file_system.parquet_reader import ParquetReader
from src.data_quality.data_quality_validation_library import DataQualityLibrary

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


def pytest_addoption(parser):
    parser.addoption("--db_host", action="store", default=os.environ.get("DB_HOST", "localhost"),
                     help="Database host")
    parser.addoption("--db_port", action="store", default=os.environ.get("DB_PORT", "5432"),
                     help="Database port")
    parser.addoption("--db_name", action="store", default=os.environ.get("DB_NAME", "postgres"),
                     help="Database name")
    parser.addoption("--db_user", action="store", default=os.environ.get("DB_USER", "postgres"),
                     help="Database user")
    parser.addoption("--db_password", action="store", default=os.environ.get("DB_PASSWORD", ""),
                     help="Database password")
    parser.addoption("--parquet_path_facility_name_min_time_spent", action="store",
                     default="/parquet_data/facility_name_min_time_spent_per_visit_date",
                     help="Path to Parquet file: facility_name_min_time_spent_per_visit_date")
    parser.addoption("--parquet_path_facility_type_avg_time_spent", action="store",
                     default="/parquet_data/facility_type_avg_time_spent_per_visit_date",
                     help="Path to Parquet file: facility_type_avg_time_spent_per_visit_date")
    parser.addoption("--parquet_path_patient_sum_treatment_cost", action="store",
                     default="/parquet_data/patient_sum_treatment_cost_per_facility_type",
                     help="Path to Parquet file: patient_sum_treatment_cost_per_facility_type")
    parser.addoption("--mapping_path", action="store", default="src/data_quality/mapping.yaml",
                     help="Path to mapping YAML file")


def pytest_configure(config):
    required_options = ["--db_user", "--db_password"]
    for option in required_options:
        if not config.getoption(option) and not os.environ.get(option.upper()):
            pytest.fail(f"Missing required option or environment variable: {option}")


@pytest.fixture(scope="session")
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
            db_port=db_port,
        ) as db_connector:
            yield db_connector
    except Exception as exc:  # pragma: no cover
        pytest.fail(f"Failed to initialize PostgresConnectorContextManager: {exc}")


@pytest.fixture(scope="session")
def parquet_reader():
    try:
        reader = ParquetReader()
        yield reader
    except Exception as exc:  # pragma: no cover
        pytest.fail(f"Failed to initialize ParquetReader: {exc}")


# --- Source-layer fixtures -------------------------------------------------

@pytest.fixture(scope="module")
def src_facilities(db_connection):
    return db_connection.get_data_sql("SELECT * FROM src_generated_facilities")


@pytest.fixture(scope="module")
def src_patients(db_connection):
    return db_connection.get_data_sql("SELECT * FROM src_generated_patients")


@pytest.fixture(scope="module")
def src_visits(db_connection):
    return db_connection.get_data_sql("SELECT * FROM src_generated_visits")


# --- 3NF-layer fixtures ----------------------------------------------------

@pytest.fixture(scope="module")
def nf3_facilities(db_connection):
    return db_connection.get_data_sql("SELECT * FROM facilities")


@pytest.fixture(scope="module")
def nf3_patients(db_connection):
    return db_connection.get_data_sql("SELECT * FROM patients")


@pytest.fixture(scope="module")
def nf3_visits(db_connection):
    return db_connection.get_data_sql("SELECT * FROM visits")


# --- Parquet fixtures ------------------------------------------------------

@pytest.fixture(scope="module")
def parquet_facility_name_min_time_spent(request, parquet_reader):
    path = request.config.getoption("--parquet_path_facility_name_min_time_spent")
    if not os.path.exists(path):
        pytest.skip(f"Parquet file not found: {path}")
    return parquet_reader.read_parquet(path)


@pytest.fixture(scope="module")
def parquet_facility_type_avg_time_spent(request, parquet_reader):
    path = request.config.getoption("--parquet_path_facility_type_avg_time_spent")
    if not os.path.exists(path):
        pytest.skip(f"Parquet file not found: {path}")
    return parquet_reader.read_parquet(path)


@pytest.fixture(scope="module")
def parquet_patient_sum_treatment_cost(request, parquet_reader):
    path = request.config.getoption("--parquet_path_patient_sum_treatment_cost")
    if not os.path.exists(path):
        pytest.skip(f"Parquet file not found: {path}")
    return parquet_reader.read_parquet(path)


# --- Supporting fixtures ---------------------------------------------------

@pytest.fixture(scope="session")
def dq_library():
    return DataQualityLibrary()


@pytest.fixture(scope="session")
def dq_mapping(request):
    """Expose column-transform mapping from YAML if available."""
    mapping_path = request.config.getoption("--mapping_path")
    if yaml is None or not os.path.exists(mapping_path):
        return {}

    with open(mapping_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


@pytest.fixture(scope="module")
def expected_parquet_outputs(nf3_visits, nf3_facilities, nf3_patients):
    """Generate expected Parquet results + metadata from the 3NF layer."""
    visits = nf3_visits.copy()
    visits["visit_timestamp"] = pd.to_datetime(visits["visit_timestamp"])
    visits["visit_date"] = visits["visit_timestamp"].dt.floor("D")

    facilities = (
        nf3_facilities[["id", "facility_name", "facility_type"]]
        .rename(columns={"id": "facility_id"})
    )
    patients = (
        nf3_patients[["id", "first_name", "last_name"]]
        .rename(columns={"id": "patient_id"})
    )

    visits_facilities = visits.merge(facilities, on="facility_id", how="left")

    facility_name_min = (
        visits_facilities.groupby(["facility_name", "visit_date"])["duration_minutes"]
        .min()
        .reset_index()
        .rename(columns={"duration_minutes": "min_time_spent"})
    )

    facility_type_avg = (
        visits_facilities.groupby(["facility_type", "visit_date"])["duration_minutes"]
        .mean()
        .round(2)
        .reset_index()
        .rename(columns={"duration_minutes": "avg_time_spent"})
    )

    visits_facilities_patients = visits_facilities.merge(patients, on="patient_id", how="left")
    visits_facilities_patients["full_name"] = (
        visits_facilities_patients["first_name"].fillna("") + " " +
        visits_facilities_patients["last_name"].fillna("")
    ).str.strip()

    patient_sum_cost = (
        visits_facilities_patients.groupby(["facility_type", "full_name"])["treatment_cost"]
        .sum()
        .reset_index()
        .rename(columns={"treatment_cost": "sum_treatment_cost"})
    )

    return {
        "facility_name_min_time_spent_per_visit_date": {
            "expected": facility_name_min,
            "partition": {"column": "partition_date", "type": "month", "source": "visit_date"},
            "coerce": {"visit_date": "datetime", "min_time_spent": "int"},
        },
        "facility_type_avg_time_spent_per_visit_date": {
            "expected": facility_type_avg,
            "partition": {"column": "partition_date", "type": "month", "source": "visit_date"},
            "coerce": {"visit_date": "datetime", "avg_time_spent": "float"},
            "allowed_values": {"facility_type": ["Hospital", "Clinic", "Specialty Center"]},
        },
        "patient_sum_treatment_cost_per_facility_type": {
            "expected": patient_sum_cost,
            "partition": {"column": "facility_type_partition", "type": "underscore", "source": "facility_type"},
            "coerce": {"sum_treatment_cost": "float"},
            "range_checks": [{"column": "sum_treatment_cost", "min": 0}],
        },
    }