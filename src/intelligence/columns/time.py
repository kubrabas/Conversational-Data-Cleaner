from typing import List

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

    # Keywords used to identify time-related columns by name.
    # These are matched against a normalized (lowercased, stripped) name.
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
        """
        Parameters
        ----------
        table : pd.DataFrame
            Input table that may contain one or more time-related columns.
        """
        super().__init__(table)

    def _has_time_keyword(self, name: str) -> bool:
        """
        Return True if the (normalized) column name looks time-related.

        Parameters
        ----------
        name : str
            Original column name.

        Returns
        -------
        bool
            True if any of the TIME_KEYWORDS appears in the normalized name.
        """
        n = self._norm(name)
        return any(keyword in n for keyword in self.TIME_KEYWORDS)

    def detect_time_columns(self) -> List[str]:
        """
        Return all column names that look time-related based on their name.

        This method only considers the column *names*, not the values.
        It is intentionally simple and conservative; it is a first step
        before applying more advanced, value-based logic.

        Returns
        -------
        list of str
            List of column names that contain time-related keywords
            in their (normalized) name.

        Examples
        --------
        - "Datum/Zeit"  -> detected (contains "datum" / "zeit")
        - "Timestamp"   -> detected (contains "timestamp")
        - "Wirkleistung in kW" -> NOT detected (no time keyword)
        """
        candidates: List[str] = []

        for col in self.columns:
            if self._has_time_keyword(col):
                candidates.append(col)

        return candidates
