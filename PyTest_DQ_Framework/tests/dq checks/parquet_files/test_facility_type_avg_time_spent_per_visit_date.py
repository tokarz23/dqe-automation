"""
Description: Data Quality checks for facility_type_avg_time_spent_per_visit_date dataset.
Requirement(s): TICKET-1234
Author(s): Your Name
"""

import pandas as pd
import pytest


DATASET_KEY = "facility_type_avg_time_spent_per_visit_date"


def _apply_coercions(df, coercions):
    df = df.copy()
    for column, dtype in coercions.items():
        if column not in df.columns:
            continue
        if dtype == "datetime":
            df[column] = pd.to_datetime(df[column])
        elif dtype == "float":
            df[column] = pd.to_numeric(df[column], errors="coerce").astype(float)
    return df


def _validate_partition(df, partition_cfg):
    column = partition_cfg["column"]
    assert column in df.columns, f"Missing partition column '{column}'"

    source = partition_cfg["source"]
    if partition_cfg["type"] == "month":
        derived = pd.to_datetime(df[source]).dt.to_period("M").astype(str)
        assert derived.equals(df[column].astype(str)), "Partition months do not match source visit_date"


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
@pytest.mark.smoke
def test_dataset_not_empty(parquet_facility_type_avg_time_spent, dq_library):
    assert dq_library.check_dataset_is_not_empty(parquet_facility_type_avg_time_spent)


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
@pytest.mark.data_quality
def test_dataset_no_nulls(parquet_facility_type_avg_time_spent, dq_library):
    assert dq_library.check_not_null_values(
        parquet_facility_type_avg_time_spent,
        ["facility_type", "visit_date", "avg_time_spent"],
    )


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
@pytest.mark.data_quality
def test_dataset_no_duplicates(parquet_facility_type_avg_time_spent, dq_library):
    assert dq_library.check_duplicates(
        parquet_facility_type_avg_time_spent,
        ["facility_type", "visit_date"],
    )


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
@pytest.mark.data_quality
def test_allowed_facility_types(parquet_facility_type_avg_time_spent, expected_parquet_outputs, dq_library):
    allowed = expected_parquet_outputs[DATASET_KEY]["allowed_values"]["facility_type"]
    assert dq_library.check_allowed_values(
        parquet_facility_type_avg_time_spent,
        "facility_type",
        allowed,
    )


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
@pytest.mark.data_quality
def test_partition_column(parquet_facility_type_avg_time_spent, expected_parquet_outputs):
    cfg = expected_parquet_outputs[DATASET_KEY]
    _validate_partition(parquet_facility_type_avg_time_spent, cfg["partition"])


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
@pytest.mark.data_completeness
def test_record_count(parquet_facility_type_avg_time_spent, expected_parquet_outputs, dq_library):
    expected_df = expected_parquet_outputs[DATASET_KEY]["expected"]
    assert dq_library.check_count(parquet_facility_type_avg_time_spent, expected_df)


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
@pytest.mark.data_completeness
def test_transformation_accuracy(parquet_facility_type_avg_time_spent, expected_parquet_outputs, dq_library):
    cfg = expected_parquet_outputs[DATASET_KEY]
    expected_df = _apply_coercions(cfg["expected"], cfg["coerce"])
    actual_df = _apply_coercions(parquet_facility_type_avg_time_spent, cfg["coerce"])

    expected_df = expected_df.sort_values(["facility_type", "visit_date"]).reset_index(drop=True)
    actual_df = actual_df.sort_values(["facility_type", "visit_date"]).reset_index(drop=True)

    comparable_actual = actual_df[expected_df.columns]
    assert dq_library.check_data_full_data_set(expected_df, comparable_actual)


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
@pytest.mark.data_completeness
def test_schema_matches_mapping(parquet_facility_type_avg_time_spent, dq_mapping):
    mapping = dq_mapping.get("datasets", dq_mapping).get(DATASET_KEY)
    if not mapping:
        pytest.skip("No mapping definition found for this dataset")
    expected_columns = {col["target_column"] for col in mapping.get("columns", []) if "target_column" in col}
    missing = expected_columns - set(parquet_facility_type_avg_time_spent.columns)
    assert not missing, f"Missing expected columns in Parquet output: {sorted(missing)}"