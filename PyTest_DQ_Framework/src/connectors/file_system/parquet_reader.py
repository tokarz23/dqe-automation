import pandas as pd

class ParquetReader:
    @staticmethod
    def read_parquet(path: str) -> pd.DataFrame:
        return pd.read_parquet(path)