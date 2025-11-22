"""
Description: Data Quality checks for transformation from Postgres SRC layer to Parquet files.
Requirement(s): TICKET-1234
Author(s): Your Name
"""

import pytest
import pandas as pd


# --- Data Quality Tests ---

@pytest.mark.dq
def test_src_tables_not_empty(src_facilities, src_patients, src_visits, dq_library):
    assert dq_library.check_dataset_is_not_empty(src_facilities)
    assert dq_library.check_dataset_is_not_empty(src_patients)
    assert dq_library.check_dataset_is_not_empty(src_visits)

@pytest.mark.dq
def test_nf3_tables_not_empty(nf3_facilities, nf3_patients, nf3_visits, dq_library):
    assert dq_library.check_dataset_is_not_empty(nf3_facilities)
    assert dq_library.check_dataset_is_not_empty(nf3_patients)
    assert dq_library.check_dataset_is_not_empty(nf3_visits)

@pytest.mark.dq
def test_src_to_nf3_facilities_mapping(src_facilities, nf3_facilities, dq_library):
    # Check that all src facilities are present in nf3 (by external_id)
    src_ids = set(src_facilities['facility_id'])
    nf3_ids = set(nf3_facilities['external_id'])
    missing = src_ids - nf3_ids
    assert not missing, f"Facilities missing in nf3: {missing}"

@pytest.mark.dq
def test_src_to_nf3_patients_mapping(src_patients, nf3_patients, dq_library):
    src_ids = set(src_patients['patient_id'])
    nf3_ids = set(nf3_patients['external_id'])
    missing = src_ids - nf3_ids
    assert not missing, f"Patients missing in nf3: {missing}"

@pytest.mark.dq
def test_src_to_nf3_visits_mapping(src_visits, nf3_visits, dq_library):
    # Check that all src visits are present in nf3 (by patient_id, facility_id, visit_timestamp)
    src_keys = set(zip(src_visits['patient_id'], src_visits['facility_id'], pd.to_datetime(src_visits['visit_timestamp'])))
    nf3_keys = set(zip(nf3_visits['patient_id'], nf3_visits['facility_id'], pd.to_datetime(nf3_visits['visit_timestamp'])))
    missing = src_keys - nf3_keys
    assert not missing, f"Visits missing in nf3: {missing}"

# --- Parquet DQ Tests ---

@pytest.mark.dq
def test_parquet_facility_name_min_time_spent_not_empty(parquet_facility_name_min_time_spent, dq_library):
    assert dq_library.check_dataset_is_not_empty(parquet_facility_name_min_time_spent)

@pytest.mark.dq
def test_parquet_facility_name_min_time_spent_no_nulls(parquet_facility_name_min_time_spent, dq_library):
    assert dq_library.check_not_null_values(parquet_facility_name_min_time_spent, ['facility_name', 'visit_date', 'min_time_spent'])

@pytest.mark.dq
def test_parquet_facility_name_min_time_spent_transformation(nf3_visits, nf3_facilities, parquet_facility_name_min_time_spent, dq_library):
    # Reproduce the transformation in Python
    merged = nf3_visits.merge(nf3_facilities, left_on='facility_id', right_on='id')
    merged['visit_date'] = pd.to_datetime(merged['visit_timestamp']).dt.date
    expected = (
        merged.groupby(['facility_name', 'visit_date'])['duration_minutes']
        .min()
        .reset_index()
        .rename(columns={'duration_minutes': 'min_time_spent'})
    )
    # Sort and compare
    expected = expected.sort_values(['facility_name', 'visit_date']).reset_index(drop=True)
    actual = parquet_facility_name_min_time_spent.sort_values(['facility_name', 'visit_date']).reset_index(drop=True)
    assert dq_library.check_data_full_data_set(expected, actual)

@pytest.mark.dq
def test_parquet_facility_type_avg_time_spent_transformation(nf3_visits, nf3_facilities, parquet_facility_type_avg_time_spent, dq_library):
    merged = nf3_visits.merge(nf3_facilities, left_on='facility_id', right_on='id')
    merged['visit_date'] = pd.to_datetime(merged['visit_timestamp']).dt.date
    expected = (
        merged.groupby(['facility_type', 'visit_date'])['duration_minutes']
        .mean()
        .round(2)
        .reset_index()
        .rename(columns={'duration_minutes': 'avg_time_spent'})
    )
    expected = expected.sort_values(['facility_type', 'visit_date']).reset_index(drop=True)
    actual = parquet_facility_type_avg_time_spent.sort_values(['facility_type', 'visit_date']).reset_index(drop=True)
    assert dq_library.check_data_full_data_set(expected, actual)

@pytest.mark.dq
def test_parquet_patient_sum_treatment_cost_transformation(nf3_visits, nf3_facilities, nf3_patients, parquet_patient_sum_treatment_cost, dq_library):
    merged = nf3_visits.merge(nf3_facilities, left_on='facility_id', right_on='id')
    merged = merged.merge(nf3_patients, left_on='patient_id', right_on='id')
    merged['full_name'] = merged['first_name'] + ' ' + merged['last_name']
    expected = (
        merged.groupby(['facility_type', 'full_name'])['treatment_cost']
        .sum()
        .reset_index()
        .rename(columns={'treatment_cost': 'sum_treatment_cost'})
    )
    expected = expected.sort_values(['facility_type', 'full_name']).reset_index(drop=True)
    actual = parquet_patient_sum_treatment_cost.sort_values(['facility_type', 'full_name']).reset_index(drop=True)
    assert dq_library.check_data_full_data_set(expected, actual)