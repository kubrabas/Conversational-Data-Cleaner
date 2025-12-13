from typing import List, Dict, Optional
import pandas as pd

from .base import BaseColumnDetector


class TimeColumnDetector(BaseColumnDetector):
    """
    Detect time-related columns in a table based purely on column names.

    This basic version:
    - does NOT inspect the actual cell values,
    - does NOT parse datetimes,
    - does NOT handle timezones,
    - only looks at column names and checks for time-related keywords.

    Later versions can extend this class with value-based heuristics
    (dtype checks, datetime parsing, combining date+time columns, etc.).
    """

    TIME_KEYWORDS = [
        "time",
        "date",
        "datum",
        "timestamp",
        "zeit",
        "uhrzeit",
        "datetime",
    ]

    def __init__(self, table: pd.DataFrame) -> None:
        super().__init__(table)

    def _has_time_keyword(self, name: str) -> bool:
        n = self._norm(name)
        return any(keyword in n for keyword in self.TIME_KEYWORDS)

    def detect_time_columns(self) -> List[str]:
        """
        Return all column names that look time-related based on their name.
        """
        candidates: List[str] = []
        for col in self.columns:
            if self._has_time_keyword(col):
                candidates.append(col)
        return candidates

    # ----------------------------
    # NEW METHOD (Part 1 + Part 2)
    # ----------------------------
    def infer_time_schema_value_based(
        self,
        candidates: Optional[List[str]] = None,
        *,
        sample_size: int = 200,
        min_parse_rate: float = 0.6,
        midnight_rate_for_date_only: float = 0.98,
    ) -> Dict:
        """
        (Part 1+2) Setup + dtype grouping:
        - decide candidate columns
        - prepare output buckets
        - group columns by dtype (datetime / numeric / text)

        Returns dict:
          {
            "datetime_cols": [...],
            "date_only_cols": [...],   # (empty for now; will be used in later steps)
            "time_only_cols": [...],   # (empty for now)
            "epoch_cols": [...],       # (empty for now)
            "unknown_cols": [...],     # (empty for now)
            "text_cols": [...],        # NEW: text-like columns
            "meta": {col: {"dtype": "...", "dtype_group": "..."}}
          }
        """
        # Part 1: choose candidates
        if candidates is None:
            candidates = self.detect_time_columns()

        # Part 1: prepare buckets
        out = {
            "datetime_cols": [],
            "date_only_cols": [],
            "time_only_cols": [],
            "epoch_cols": [],
            "unknown_cols": [],
            "text_cols": [],  # NEW
            "meta": {},
        }

        # Part 2: dtype-based grouping
        for col in candidates:
            ser = self.table[col]
            dtype = ser.dtype

            out["meta"][col] = {"dtype": str(dtype)}

            if pd.api.types.is_datetime64_any_dtype(ser):
                out["datetime_cols"].append(col)
                out["meta"][col]["dtype_group"] = "datetime"
            elif pd.api.types.is_numeric_dtype(ser):
                out["meta"][col]["dtype_group"] = "numeric"
                # epoch_cols will be decided later
            else:
                out["text_cols"].append(col)
                out["meta"][col]["dtype_group"] = "text"

        return out
