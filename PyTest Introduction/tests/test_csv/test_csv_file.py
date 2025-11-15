import pytest
import re


def test_file_not_empty(csv_df):
    """Validate that file is not empty."""
    assert not csv_df.empty, "CSV file is empty"


@pytest.mark.validate_csv
@pytest.mark.xfail(reason="This test is expected - known issue with duplicates.")
def test_duplicates(csv_df):
    duplicates = csv_df[csv_df.duplicated()]
    assert duplicates.empty, f"Duplicate rows found: {duplicates.count()}"


@pytest.mark.validate_csv
def test_validate_schema(validate_schema):
    pass


@pytest.mark.validate_csv
@pytest.mark.skip("Known issue with Age column.")
def test_age_column_valid(csv_df):
    invalid_ages = csv_df[~csv_df['age'].between(0, 100)]
    assert invalid_ages.empty, f"Invalid age records found: {invalid_ages['age'].tolist()}"


@pytest.mark.validate_csv
def test_email_column_valid(csv_df):
    email_pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    invalid_emails = csv_df[~csv_df['email'].str.match(email_pattern)]
    assert invalid_emails.empty, f"Invalid emails: {invalid_emails['email'].tolist()}"


@pytest.mark.parametrize("id, expected_active_status", [(1, False), (2, True)])
def test_active_players(csv_df, id, expected_active_status):
    row = csv_df[csv_df['id'] == id]
    assert not row.empty, f"No row found for id={id}"
    actual = row['is_active'].iloc[0]
    assert actual == expected_active_status, f"For id={id}, expected is_active={expected_active_status}, got {actual}"


def test_active_player(csv_df):
    row = csv_df[csv_df['id'] == 2]
    assert not row.empty, "No row found for id=2"
    actual = row['is_active'].iloc[0]
    assert actual is True, f"For id=2, expected is_active=True, got {actual}"
