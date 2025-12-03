from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_datetime64tz_dtype


def path_to_uri(path: str) -> str:
    """Turn a filesystem path into a file:// URI that Chrome understands."""
    return Path(path).resolve().as_uri()


def read_svg_table(cell_texts: Iterable[str], num_columns: int) -> pd.DataFrame:
    """
    The report renders its table with <text class="cell-text"> nodes arranged column-first,
    with the header as the last value in each column. Rebuild the table from that layout.
    """
    values = [text.strip() for text in cell_texts if text is not None]
    if not values:
        raise ValueError("No SVG cell values were found.")

    try:
        num_columns = int(num_columns)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"num_columns must be an integer, got {num_columns!r}.") from exc

    if num_columns <= 0:
        raise ValueError("num_columns must be positive.")

    total_cells = len(values)
    if total_cells % num_columns:
        raise ValueError(
            f"{total_cells} cell values cannot be split evenly across {num_columns} columns."
        )

    rows_per_col = total_cells // num_columns
    raw_columns = []
    headers = []

    for idx in range(num_columns):
        start = idx * rows_per_col
        column_cells = values[start : start + rows_per_col]
        if not column_cells:
            raise ValueError(f"Column #{idx + 1} has no data.")

        headers.append(column_cells[-1])
        raw_columns.append(column_cells[:-1])

    lengths = {len(col) for col in raw_columns}
    if len(lengths) != 1:
        raise ValueError(f"Uneven column sizes: {[len(col) for col in raw_columns]}")

    rows = list(zip(*raw_columns))
    frame = pd.DataFrame(rows, columns=headers)
    return frame.reset_index(drop=True)


RENAME_MAP = {
    "facility_type": "Facility Type",
    "visit_date": "Visit Date",
    "avg_time_spent": "Average Time Spent",
}


def read_parquet_dataset(
    folder_path: str,
    filter_date: Optional[str] = None,
    date_column: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load the Parquet snapshots, optionally filter by a partition/date column,
    rename columns to match the report, and return only the fields the report shows.
    """
    frame = pd.read_parquet(folder_path)

    if filter_date:
        if date_column and date_column in frame.columns:
            candidate = date_column
        else:
            candidates = [col for col in frame.columns if "date" in col.lower()]
            if not candidates:
                raise ValueError("FILTER_DATE was set but no date-like column exists.")
            candidate = candidates[0]

        column = frame[candidate]
        if is_datetime64_any_dtype(column):
            target = pd.to_datetime(filter_date).date()
            frame = frame[column.dt.date == target]
        else:
            frame = frame[column.astype(str) == str(filter_date)]

    frame = frame.rename(columns=RENAME_MAP)

    missing = [col for col in RENAME_MAP.values() if col not in frame.columns]
    if missing:
        raise ValueError(f"Renamed Parquet data is missing columns: {missing}")

    frame = frame[list(RENAME_MAP.values())]

    frame = frame.apply(
        lambda col: col.map(lambda value: "" if pd.isna(value) else str(value).strip())
    )

    return frame.reset_index(drop=True)


def _clean_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result.columns = [str(col).strip() for col in result.columns]

    for column in result.select_dtypes(include=["object", "string"]):
        result[column] = result[column].map(
            lambda value: value.strip() if isinstance(value, str) else value
        )

    for column in result.columns:
        if is_datetime64tz_dtype(result[column]):
            result[column] = result[column].dt.tz_convert(None)

    return result


def _sort_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.sort_index(axis=1)

    if frame.empty:
        return frame.reset_index(drop=True)

    try:
        frame = frame.sort_values(list(frame.columns), kind="mergesort")
    except TypeError:
        frame = frame.sort_values(
            list(frame.columns),
            kind="mergesort",
            key=lambda col: col.astype(str),
        )

    return frame.reset_index(drop=True)


def compare_dataframes(report_df: pd.DataFrame, parquet_df: pd.DataFrame) -> str:
    """
    Return "" when frames match. If not, return a readable description of what differs.
    """
    expected = _sort_dataframe(_clean_dataframe(report_df))
    actual = _sort_dataframe(_clean_dataframe(parquet_df))

    diff_messages = []

    missing = set(expected.columns) - set(actual.columns)
    extra = set(actual.columns) - set(expected.columns)
    if missing or extra:
        if missing:
            diff_messages.append(f"Missing columns in Parquet data: {sorted(missing)}")
        if extra:
            diff_messages.append(f"Unexpected columns in Parquet data: {sorted(extra)}")
        common = sorted(set(expected.columns) & set(actual.columns))
        expected = expected[common]
        actual = actual[common]

    if len(expected) != len(actual):
        diff_messages.append(
            f"Row count mismatch (report={len(expected)}, parquet={len(actual)})."
        )

    if diff_messages:
        return "\n".join(diff_messages)

    try:
        pd.testing.assert_frame_equal(expected, actual, check_dtype=False)
        return ""
    except AssertionError as exc:
        mismatch = expected.compare(
            actual,
            align_axis=0,
            keep_shape=True,
            keep_equal=False,
        )
        message_lines = [str(exc)]
        if not mismatch.empty:
            message_lines.append("Detailed differences:\n" + mismatch.to_string())
        else:
            message_lines.append("Differences exist but could not be rendered.")
        return "\n".join(message_lines)