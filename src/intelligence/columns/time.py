from typing import List, Dict, Optional
import pandas as pd

from .base import BaseColumnDetector


class TimeColumnDetector(BaseColumnDetector):
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
        candidates: List[str] = []
        for col in self.columns:
            if self._has_time_keyword(col):
                candidates.append(col)
        return candidates

    # ----------------------------
    # NEW METHOD (Part 1 only)
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
        (Part 1) Setup only:
        - decide which candidate columns to inspect
        - prepare output buckets
        """
        # If user didn't pass candidates, use the name-based detector as a starting point
        if candidates is None:
            candidates = self.detect_time_columns()

        # Output buckets (will be filled in later parts)
        out = {
            "datetime_cols": [],
            "date_only_cols": [],
            "time_only_cols": [],
            "epoch_cols": [],
            "unknown_cols": [],
            "meta": {},
        }

        return out
