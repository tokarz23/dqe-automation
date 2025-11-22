"""
Description: Data Quality checks across SRC and 3NF relational layers.
Requirement(s): TICKET-1234
Author(s): Your Name
"""

import pandas as pd
import pytest


@pytest.mark.dq
@pytest.mark.smoke
def test_src_tables_not_empty(src_facilities, src_patients, src_visits, dq_library):
    assert dq_library.check_dataset_is_not_empty(src_facilities)
    assert dq_library.check_dataset_is_not_empty(src_patients)
    assert dq_library.check_dataset_is_not_empty(src_visits)


@pytest.mark.dq
@pytest.mark.smoke
def test_nf3_tables_not_empty(nf3_facilities, nf3_patients, nf3_visits, dq_library):
    assert dq_library.check_dataset_is_not_empty(nf3_facilities)
    assert dq_library.check_dataset_is_not_empty(nf3_patients)
    assert dq_library.check_dataset_is_not_empty(nf3_visits)


@pytest.mark.dq
@pytest.mark.data_completeness
def test_src_nf3_row_counts(src_facilities, src_patients, src_visits,
                            nf3_facilities, nf3_patients, nf3_visits, dq_library):
    assert dq_library.check_count(src_facilities, nf3_facilities)
    assert dq_library.check_count(src_patients, nf3_patients)
    assert dq_library.check_count(src_visits, nf3_visits)


@pytest.mark.dq
@pytest.mark.data_completeness
def test_src_nf3_facilities_alignment(src_facilities, nf3_facilities):
    src_ids = set(src_facilities["facility_id"])
    nf3_ids = set(nf3_facilities["external_id"])
    missing = src_ids - nf3_ids
    assert not missing, f"Facilities missing in 3NF: {sorted(missing)}"


@pytest.mark.dq
@pytest.mark.data_completeness
def test_src_nf3_patients_alignment(src_patients, nf3_patients):
    src_ids = set(src_patients["patient_id"])
    nf3_ids = set(nf3_patients["external_id"])
    missing = src_ids - nf3_ids
    assert not missing, f"Patients missing in 3NF: {sorted(missing)}"


@pytest.mark.dq
@pytest.mark.data_completeness
def test_src_nf3_visits_alignment(src_visits, nf3_visits):
    src_keys = set(
        zip(
            src_visits["patient_id"],
            src_visits["facility_id"],
            pd.to_datetime(src_visits["visit_timestamp"]),
        )
    )
    nf3_keys = set(
        zip(
            nf3_visits["patient_id"],
            nf3_visits["facility_id"],
            pd.to_datetime(nf3_visits["visit_timestamp"]),
        )
    )
    missing = src_keys - nf3_keys
    assert not missing, f"Visits missing in 3NF: {sorted(missing)[:5]}"


@pytest.mark.dq
@pytest.mark.data_quality
def test_nf3_visits_uniqueness(nf3_visits, dq_library):
    assert dq_library.check_duplicates(
        nf3_visits, ["patient_id", "facility_id", "visit_timestamp"]
    ), "Duplicate visits detected in 3NF layer"