import pytest
import pandas as pd

@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_not_empty(parquet_patient_sum_treatment_cost, dq_library):
    assert dq_library.check_dataset_is_not_empty(parquet_patient_sum_treatment_cost)

@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_no_nulls(parquet_patient_sum_treatment_cost, dq_library):
    assert dq_library.check_not_null_values(parquet_patient_sum_treatment_cost, ['facility_type', 'full_name', 'sum_treatment_cost'])

@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_no_duplicates(parquet_patient_sum_treatment_cost, dq_library):
    assert dq_library.check_duplicates(parquet_patient_sum_treatment_cost, ['facility_type', 'full_name'])

@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_sum_treatment_cost_not_negative(parquet_patient_sum_treatment_cost, dq_library):
    # Only allow negative values for 'Clinic' if business logic allows, otherwise check all >= 0
    # Here, let's check all values are >= 0 for simplicity
    assert dq_library.check_value_range(parquet_patient_sum_treatment_cost, 'sum_treatment_cost', min_value=0)

@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_column_mapping(nf3_visits, nf3_facilities, nf3_patients, parquet_patient_sum_treatment_cost, dq_library):
    mapping_rules = [
        {'source_column': 'facility_type', 'target_column': 'facility_type', 'transformation': 'none'},
        {'source_column': 'full_name', 'target_column': 'full_name', 'transformation': 'concat'},
        {'source_column': 'sum_treatment_cost', 'target_column': 'sum_treatment_cost', 'transformation': 'sum'}
    ]
    merged = nf3_visits.merge(nf3_facilities, left_on='facility_id', right_on='id')
    merged = merged.merge(nf3_patients, left_on='patient_id', right_on='id')
    merged['full_name'] = merged['first_name'] + ' ' + merged['last_name']
    expected = (
        merged.groupby(['facility_type', 'full_name'])['treatment_cost']
        .sum()
        .reset_index()
        .rename(columns={'treatment_cost': 'sum_treatment_cost'})
    )
    assert dq_library.check_column_mapping(expected, parquet_patient_sum_treatment_cost, mapping_rules)

@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_transformation(nf3_visits, nf3_facilities, nf3_patients, parquet_patient_sum_treatment_cost, dq_library):
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