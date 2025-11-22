import pandas as pd


class DataQualityLibrary:
    """Reusable DQ checks."""

    @staticmethod
    def check_duplicates(df: pd.DataFrame, column_names=None) -> bool:
        duplicates = df.duplicated(subset=column_names) if column_names else df.duplicated()
        if duplicates.any():
            print(f"Duplicate rows found:\n{df[duplicates]}")
            return False
        return True

    @staticmethod
    def check_count(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
        if len(df1) != len(df2):
            print(f"Row count mismatch: df1 has {len(df1)} rows, df2 has {len(df2)} rows")
            return False
        return True

    @staticmethod
    def check_data_full_data_set(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
        if not df1.sort_index(axis=1).equals(df2.sort_index(axis=1)):
            print("Data mismatch between the two DataFrames")
            return False
        return True

    @staticmethod
    def check_dataset_is_not_empty(df: pd.DataFrame) -> bool:
        if df.empty:
            print("DataFrame is empty")
            return False
        return True

    @staticmethod
    def check_not_null_values(df: pd.DataFrame, column_names=None) -> bool:
        columns = column_names if column_names else df.columns
        for col in columns:
            if df[col].isnull().any():
                print(f"Null values found in column: {col} ({df[col].isnull().sum()} nulls)")
                return False
        return True

    @staticmethod
    def check_column_mapping(source_df: pd.DataFrame, target_df: pd.DataFrame, mapping_rules: list) -> bool:
        for rule in mapping_rules:
            if rule["source_column"] not in source_df.columns or rule["target_column"] not in target_df.columns:
                print(f"Missing column: {rule['source_column']} in source or {rule['target_column']} in target")
                return False
        return True

    @staticmethod
    def check_transformed_values(source_df: pd.DataFrame, target_df: pd.DataFrame, mapping_rules: list) -> bool:
        for rule in mapping_rules:
            if rule.get("transformation", "none") == "none":
                if not source_df[rule["source_column"]].equals(target_df[rule["target_column"]]):
                    print(f"Mismatch in column: {rule['source_column']} vs {rule['target_column']}")
                    return False
        return True

    @staticmethod
    def check_value_range(df: pd.DataFrame, column: str, min_value=None, max_value=None) -> bool:
        if min_value is not None and (df[column] < min_value).any():
            print(f"Values in column {column} below minimum {min_value}")
            return False
        if max_value is not None and (df[column] > max_value).any():
            print(f"Values in column {column} above maximum {max_value}")
            return False
        return True

    @staticmethod
    def check_allowed_values(df: pd.DataFrame, column: str, allowed_values: list) -> bool:
        invalid = set(df[column]) - set(allowed_values)
        if invalid:
            print(f"Invalid values in column {column}: {invalid}")
            return False
        return True