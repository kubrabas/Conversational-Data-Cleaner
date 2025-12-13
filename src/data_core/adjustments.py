import pandas as pd


class TableRefiner:
    def __init__(self, table: pd.DataFrame):
        """
        Initialize the TableRefiner with a pandas DataFrame.

        Parameters
        ----------
        table : pd.DataFrame
            The table to be refined.
        """
        self.table = table
        self.columns = list(table.columns)

    def clean_table(self) -> pd.DataFrame:
        """
        Remove columns and rows that are entirely NaN.

        Returns
        -------
        pd.DataFrame
            The cleaned DataFrame.
        """
        # Drop columns that are completely NaN
        self.table = self.table.dropna(axis=1, how="all")

        # Drop rows that are completely NaN
        self.table = self.table.dropna(axis=0, how="all")

        # Update columns after cleaning
        self.columns = list(self.table.columns)

        return self.table

    def keep_only_moment_and_consumption(
        self,
        *,
        moment_col: str = "moment",
        consumption_col: str = "consumption_kwh",
    ) -> pd.DataFrame:
        """
        Keep only `moment_col` and `consumption_col` in the table.
        Drops all other columns (in-place) and updates `self.columns`.

        Returns
        -------
        pd.DataFrame
            The reduced DataFrame containing only [moment_col, consumption_col].
        """
        missing = [c for c in (moment_col, consumption_col) if c not in self.table.columns]
        if missing:
            raise KeyError(f"Missing required columns: {missing}")

        self.table = self.table[[moment_col, consumption_col]].copy()
        self.columns = list(self.table.columns)
        return self.table
