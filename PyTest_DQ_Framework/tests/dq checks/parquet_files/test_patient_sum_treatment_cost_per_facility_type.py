"""
Description: Data Quality checks for patient_sum_treatment_cost_per_facility_type dataset.
Requirement(s): TICKET-1234
Author(s): Your Name
"""

import pandas as pd
import pytest


DATASET_KEY = "patient_sum_treatment_cost_per_facility_type"


def _apply_coercions(df, coercions):
    df = df.copy()
    for column, dtype in coercions.items():
        if column not in df.columns:
            continue
        if dtype == "float":
            df[column] = pd.to_numeric(df[column], errors="coerce").astype(float)
    return df


def _validate_partition(df, partition_cfg):
    column = partition_cfg["column"]
    assert column in df.columns, f"Missing partition column '{column}'"

    source = partition_cfg["source"]
    if partition_cfg["type"] == "underscore":
        derived = df[source].astype(str).str.replace(" ", "_")
        assert derived.equals(df[column].astype(str)), "Partition values do not match facility_type"


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
@pytest.mark.smoke
def test_dataset_not_empty(parquet_patient_sum_treatment_cost, dq_library):
    assert dq_library.check_dataset_is_not_empty(parquet_patient_sum_treatment_cost)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
@pytest.mark.data_quality
def test_dataset_no_nulls(parquet_patient_sum_treatment_cost, dq_library):
    assert dq_library.check_not_null_values(
        parquet_patient_sum_treatment_cost,
        ["facility_type", "full_name", "sum_treatment_cost"],
    )


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
@pytest.mark.data_quality
def test_dataset_no_duplicates(parquet_patient_sum_treatment_cost, dq_library):
    assert dq_library.check_duplicates(
        parquet_patient_sum_treatment_cost,
        ["facility_type", "full_name"],
    )


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
@pytest.mark.data_quality
def test_value_ranges(parquet_patient_sum_treatment_cost, expected_parquet_outputs, dq_library):
    for check in expected_parquet_outputs[DATASET_KEY].get("range_checks", []):
        assert dq_library.check_value_range(
            parquet_patient_sum_treatment_cost,
            check["column"],
            check.get("min"),
            check.get("max"),
        )


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
@pytest.mark.data_quality
def test_partition_column(parquet_patient_sum_treatment_cost, expected_parquet_outputs):
    cfg = expected_parquet_outputs[DATASET_KEY]
    _validate_partition(parquet_patient_sum_treatment_cost, cfg["partition"])


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
@pytest.mark.data_completeness
def test_record_count(parquet_patient_sum_treatment_cost, expected_parquet_outputs, dq_library):
    expected_df = expected_parquet_outputs[DATASET_KEY]["expected"]
    assert dq_library.check_count(parquet_patient_sum_treatment_cost, expected_df)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
@pytest.mark.data_completeness
def test_transformation_accuracy(parquet_patient_sum_treatment_cost, expected_parquet_outputs, dq_library):
    cfg = expected_parquet_outputs[DATASET_KEY]
    expected_df = _apply_coercions(cfg["expected"], cfg["coerce"])
    actual_df = _apply_coercions(parquet_patient_sum_treatment_cost, cfg["coerce"])

    expected_df = expected_df.sort_values(["facility_type", "full_name"]).reset_index(drop=True)
    actual_df = actual_df.sort_values(["facility_type", "full_name"]).reset_index(drop=True)

    comparable_actual = actual_df[expected_df.columns]
    assert dq_library.check_data_full_data_set(expected_df, comparable_actual)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
@pytest.mark.data_completeness
def test_schema_matches_mapping(parquet_patient_sum_treatment_cost, dq_mapping):
    mapping = dq_mapping.get("datasets", dq_mapping).get(DATASET_KEY)
    if not mapping:
        pytest.skip("No mapping definition found for this dataset")
    expected_columns = {col["target_column"] for col in mapping.get("columns", []) if "target_column" in col}
    missing = expected_columns - set(parquet_patient_sum_treatment_cost.columns)
    assert not missing, f"Missing expected columns in Parquet output: {sorted(missing)}"