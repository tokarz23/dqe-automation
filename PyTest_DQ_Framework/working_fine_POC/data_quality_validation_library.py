import pandas as pd

class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.
    """

    @staticmethod
    def check_duplicates(df: pd.DataFrame, column_names=None) -> bool:
        """
        Checks for duplicate rows in the DataFrame.
        """
        duplicates = df.duplicated(subset=column_names) if column_names else df.duplicated()
        if duplicates.any():
            print(f"Duplicate rows found:\n{df[duplicates]}")
            return False
        return True

    @staticmethod
    def check_count(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
        """
        Checks if the number of rows in two DataFrames is equal.
        """
        if len(df1) != len(df2):
            print(f"Row count mismatch: df1 has {len(df1)} rows, df2 has {len(df2)} rows")
            return False
        return True

    @staticmethod
    def check_data_full_data_set(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
        """
        Checks if two DataFrames are equal (after sorting columns).
        """
        df1_sorted = df1.sort_index(axis=1)
        df2_sorted = df2.sort_index(axis=1)
        if not df1_sorted.equals(df2_sorted):
            print("Data mismatch between the two DataFrames")
            return False
        return True

    @staticmethod
    def check_dataset_is_not_empty(df: pd.DataFrame) -> bool:
        """
        Checks if the DataFrame is not empty.
        """
        if df.empty:
            print("DataFrame is empty")
            return False
        return True

    @staticmethod
    def check_not_null_values(df: pd.DataFrame, column_names=None) -> bool:
        """
        Checks for null values in specified columns.
        """
        columns = column_names if column_names else df.columns
        for col in columns:
            if df[col].isnull().any():
                print(f"Null values found in column: {col} ({df[col].isnull().sum()} nulls)")
                return False
        return True

    @staticmethod
    def check_column_mapping(source_df: pd.DataFrame, target_df: pd.DataFrame, mapping_rules: list) -> bool:
        """
        Checks if columns in source and target DataFrames match the mapping rules.
        """
        for rule in mapping_rules:
            src_col = rule['source_column']
            tgt_col = rule['target_column']
            if src_col not in source_df.columns or tgt_col not in target_df.columns:
                print(f"Missing column: {src_col} in source or {tgt_col} in target")
                return False
        return True

    @staticmethod
    def check_transformed_values(source_df: pd.DataFrame, target_df: pd.DataFrame, mapping_rules: list) -> bool:
        """
        Checks if transformed values in target DataFrame match expected values from source DataFrame.
        """
        for rule in mapping_rules:
            src_col = rule['source_column']
            tgt_col = rule['target_column']
            if rule.get('transformation', 'none') == 'none':
                if not source_df[src_col].equals(target_df[tgt_col]):
                    print(f"Mismatch in column: {src_col} vs {tgt_col}")
                    return False
        return True

    @staticmethod
    def check_value_range(df: pd.DataFrame, column: str, min_value=None, max_value=None) -> bool:
        """
        Checks if values in a column are within a specified range.
        """
        if min_value is not None and (df[column] < min_value).any():
            print(f"Values in column {column} below minimum {min_value}")
            return False
        if max_value is not None and (df[column] > max_value).any():
            print(f"Values in column {column} above maximum {max_value}")
            return False
        return True

    @staticmethod
    def check_allowed_values(df: pd.DataFrame, column: str, allowed_values: list) -> bool:
        """
        Checks if all values in a column are within the allowed set.
        """
        invalid = set(df[column]) - set(allowed_values)
        if invalid:
            print(f"Invalid values in column {column}: {invalid}")
            return False
        return True