import pytest
import pandas as pd

@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_not_empty(parquet_facility_type_avg_time_spent, dq_library):
    assert dq_library.check_dataset_is_not_empty(parquet_facility_type_avg_time_spent)

@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_no_nulls(parquet_facility_type_avg_time_spent, dq_library):
    assert dq_library.check_not_null_values(parquet_facility_type_avg_time_spent, ['facility_type', 'visit_date', 'avg_time_spent'])

@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_no_duplicates(parquet_facility_type_avg_time_spent, dq_library):
    assert dq_library.check_duplicates(parquet_facility_type_avg_time_spent, ['facility_type', 'visit_date'])

@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_allowed_facility_types(parquet_facility_type_avg_time_spent, dq_library):
    allowed_types = ['Hospital', 'Clinic', 'Specialty Center']
    assert dq_library.check_allowed_values(parquet_facility_type_avg_time_spent, 'facility_type', allowed_types)

@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_column_mapping(nf3_visits, nf3_facilities, parquet_facility_type_avg_time_spent, dq_library):
    mapping_rules = [
        {'source_column': 'facility_type', 'target_column': 'facility_type', 'transformation': 'none'},
        {'source_column': 'visit_date', 'target_column': 'visit_date', 'transformation': 'none'},
        {'source_column': 'avg_time_spent', 'target_column': 'avg_time_spent', 'transformation': 'avg'}
    ]
    merged = nf3_visits.merge(nf3_facilities, left_on='facility_id', right_on='id')
    merged['visit_date'] = pd.to_datetime(merged['visit_timestamp']).dt.date
    expected = (
        merged.groupby(['facility_type', 'visit_date'])['duration_minutes']
        .mean()
        .round(2)
        .reset_index()
        .rename(columns={'duration_minutes': 'avg_time_spent'})
    )
    assert dq_library.check_column_mapping(expected, parquet_facility_type_avg_time_spent, mapping_rules)

@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_transformation(nf3_visits, nf3_facilities, parquet_facility_type_avg_time_spent, dq_library):
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