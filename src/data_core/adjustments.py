import pandas as pd


class TableRefiner:
    def __init__(self, table: pd.DataFrame):
        self.table = table
        self.columns = list(table.columns)

    def clean_table(self) -> pd.DataFrame:
        """
        Remove columns/rows that are entirely empty (NaN),
        also treat empty/whitespace-only strings as empty,
        and trim trailing empty rows at the bottom.
        """
        # Drop columns that are completely NaN
        self.table = self.table.dropna(axis=1, how="all")

        # Drop columns that are empty strings / whitespace-only in every cell
        self.drop_empty_columns()

        # Drop rows that are completely NaN
        self.table = self.table.dropna(axis=0, how="all")

        # Trim trailing empty rows at bottom (incl. empty strings)
        self.drop_trailing_empty_rows()

        self.columns = list(self.table.columns)
        return self.table

    def keep_only_moment_and_consumption(
        self,
        *,
        moment_col: str = "moment",
        consumption_col: str = "consumption_kwh",
    ) -> pd.DataFrame:
        missing = [c for c in (moment_col, consumption_col) if c not in self.table.columns]
        if missing:
            raise KeyError(f"Missing required columns: {missing}")

        self.table = self.table[[moment_col, consumption_col]].copy()
        self.columns = list(self.table.columns)
        return self.table

    def drop_trailing_empty_rows(self) -> pd.DataFrame:
        if self.table.empty:
            return self.table

        def _cell_is_empty(x) -> bool:
            if pd.isna(x):
                return True
            if isinstance(x, str) and x.strip() == "":
                return True
            return False

        empty_row_mask = self.table.applymap(_cell_is_empty).all(axis=1)

        if not empty_row_mask.any():
            return self.table

        non_empty_positions = (~empty_row_mask).to_numpy().nonzero()[0]
        if len(non_empty_positions) == 0:
            self.table = self.table.iloc[0:0].copy()
        else:
            last_keep_pos = non_empty_positions[-1]
            self.table = self.table.iloc[: last_keep_pos + 1].copy()

        self.columns = list(self.table.columns)
        return self.table

    def drop_empty_columns(self) -> pd.DataFrame:
        """
        Drop columns that are completely empty.

        "Empty" means: NaN OR empty/whitespace-only strings in every cell.
        """
        if self.table.empty:
            return self.table

        def _cell_is_empty(x) -> bool:
            if pd.isna(x):
                return True
            if isinstance(x, str) and x.strip() == "":
                return True
            return False

        empty_col_mask = self.table.applymap(_cell_is_empty).all(axis=0)
        if empty_col_mask.any():
            self.table = self.table.loc[:, ~empty_col_mask].copy()

        self.columns = list(self.table.columns)
        return self.table

    # ==========================================================================
    # ✅ NEW METHOD: apply the "subtract 15 minutes" rule (no dtype conversion)
    # ==========================================================================
    def shift_moment_minus_15_if_first15_last00(self, *, moment_col: str = "moment") -> pd.DataFrame:
        """
        Checks the first and last row of the `moment` column.

        Rule:
        - If first row minute == 15
        - AND last row minute == 00
        -> subtract 15 minutes from EVERY value in the `moment` column.

        Assumptions:
        - `moment` is already datetime64[ns] (timezone-naive).
        - No parsing / no pd.to_datetime conversion is done.
        - Output remains datetime64[ns] (tz-naive).
        """
        if self.table is None or self.table.empty:
            return self.table

        if moment_col not in self.table.columns:
            return self.table

        s = self.table[moment_col]

        # Only operate if dtype is exactly datetime64[ns] (tz-naive)
        if not pd.api.types.is_datetime64_ns_dtype(s.dtype):
            return self.table

        first_val = s.iloc[0]
        last_val = s.iloc[-1]

        # If first/last are NaT, do nothing
        if pd.isna(first_val) or pd.isna(last_val):
            return self.table

        # Apply rule
        if int(first_val.minute) == 15 and int(last_val.minute) == 0:
            self.table[moment_col] = s - pd.Timedelta(minutes=15)

        self.columns = list(self.table.columns)
        return self.table
