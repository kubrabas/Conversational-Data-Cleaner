from __future__ import annotations

from typing import List
import pandas as pd
import re

from .base import BaseColumnDetector


# ==============================================================================
# 1) Detector
# ==============================================================================
class TimeColumnDetector(BaseColumnDetector):
    """
    Detect time-related columns based on column names only.
    """

    TIME_KEYWORDS = [
        "time",
        "date",
        "datum",
        "timestamp",
        "zeit",
        "uhrzeit",
        "datetime",
        "from",
        "to",
        "von",
        "bis",
        "ab",
    ]

    def __init__(self, table: pd.DataFrame) -> None:
        super().__init__(table)

    def _has_time_keyword(self, name: str) -> bool:
        n = self._norm(name)
        return any(keyword in n for keyword in self.TIME_KEYWORDS)

    def detect_time_columns(self) -> List[str]:
        return [col for col in self.columns if self._has_time_keyword(col)]


# ==============================================================================
# 2) Date + Hour -> Single timestamp
# ==============================================================================
import re
import pandas as pd


class Preference_Date_And_Hour:
    """
    User selected two columns:
      - DATE column (day-month-year)
      - HOUR column (hour/minute/seconds)

    Goal:
      - Normalize DATE -> "YYYY-MM-DD" (string)
      - Normalize HOUR -> "HH:MM:SS" (string)
      - Combine into a single datetime column named "moment"
    """

    def __init__(self, table: pd.DataFrame, date_col: str, hour_col: str):
        self.table = table
        self.date_col = date_col
        self.hour_col = hour_col

    def detect_date_dtype(self) -> str:
        """
        Normalizes DATE column into "YYYY-MM-DD" (string). Returns "string".
        """
        if self.date_col not in self.table.columns:
            raise KeyError(f"Date column not found: {self.date_col}")

        s = self.table[self.date_col]

        # datetime-like -> normalize -> YYYY-MM-DD string
        if pd.api.types.is_datetime64_any_dtype(s):
            norm = s.dt.normalize()
            self.table[self.date_col] = norm.dt.strftime("%Y-%m-%d").astype("string")
            return "string"

        # string/object -> parse -> normalize -> YYYY-MM-DD string
        if pd.api.types.is_string_dtype(s) or pd.api.types.is_object_dtype(s):
            parsed = pd.to_datetime(s, errors="coerce")
            if parsed.notna().sum() == 0 and s.dropna().shape[0] > 0:
                raise ValueError(
                    f"Could not parse any values in DATE column '{self.date_col}' as datetime."
                )

            norm = parsed.dt.normalize()
            self.table[self.date_col] = norm.dt.strftime("%Y-%m-%d").astype("string")
            return "string"

        raise TypeError(
            f"Preference_Date_And_Hour expects DATE column to be datetime or string/object, "
            f"but got dtype={s.dtype} for column '{self.date_col}'."
        )

    def normalize_hour_column(self) -> str:
        """
        Normalizes HOUR column into "HH:MM:SS" (string). Returns "string".
        """
        if self.hour_col not in self.table.columns:
            raise KeyError(f"Hour column not found: {self.hour_col}")

        s = self.table[self.hour_col]

        # datetime-like -> take time-of-day
        if pd.api.types.is_datetime64_any_dtype(s):
            self.table[self.hour_col] = s.dt.strftime("%H:%M:%S").astype("string")
            return "string"

        # string/object -> parse by rules
        if pd.api.types.is_string_dtype(s) or pd.api.types.is_object_dtype(s):

            def _to_hhmmss(v):
                if pd.isna(v):
                    return pd.NA

                txt = str(v).strip()
                if txt == "":
                    return pd.NA

                # normalize separators to ':'
                txt2 = re.sub(r"[^\d]+", ":", txt)
                txt2 = re.sub(r":+", ":", txt2).strip(":")

                parts = txt2.split(":") if ":" in txt2 else [txt2]

                # no separators -> HHMMSS / HHMM / HMM / H / HH
                if len(parts) == 1:
                    digits = parts[0]
                    if not digits.isdigit():
                        return pd.NA

                    if len(digits) == 6:      # HHMMSS
                        hh, mm, ss = digits[:2], digits[2:4], digits[4:6]
                    elif len(digits) == 4:    # HHMM
                        hh, mm, ss = digits[:2], digits[2:4], "00"
                    elif len(digits) == 3:    # HMM -> 9:30
                        hh, mm, ss = digits[:1], digits[1:3], "00"
                    elif len(digits) == 2:    # HH
                        hh, mm, ss = digits, "00", "00"
                    elif len(digits) == 1:    # H
                        hh, mm, ss = digits, "00", "00"
                    else:
                        return pd.NA
                else:
                    # separators -> H:M(:S)
                    if len(parts) == 2:
                        hh, mm = parts
                        ss = "00"
                    else:
                        hh, mm, ss = parts[0], parts[1], parts[2]

                try:
                    h = int(hh)
                    m = int(mm)
                    sec = int(ss)
                except Exception:
                    return pd.NA

                if not (0 <= h <= 23 and 0 <= m <= 59 and 0 <= sec <= 59):
                    return pd.NA

                return f"{h:02d}:{m:02d}:{sec:02d}"

            self.table[self.hour_col] = s.map(_to_hhmmss).astype("string")
            return "string"

        raise TypeError(
            f"Preference_Date_And_Hour expects HOUR column to be datetime or string/object, "
            f"but got dtype={s.dtype} for column '{self.hour_col}'."
        )

    def create_moment_column(self, out_col: str = "moment") -> float:
        """
        Combines normalized DATE + HOUR into a datetime column named `out_col`.
        Returns parse success rate (0..1).
        """
        if self.date_col not in self.table.columns:
            raise KeyError(f"Date column not found: {self.date_col}")
        if self.hour_col not in self.table.columns:
            raise KeyError(f"Hour column not found: {self.hour_col}")

        date_s = self.table[self.date_col].astype("string")
        hour_s = self.table[self.hour_col].astype("string")

        combined = (date_s + " " + hour_s).astype("string")
        dt = pd.to_datetime(combined, errors="coerce", format="%Y-%m-%d %H:%M:%S")

        self.table[out_col] = dt
        return float(dt.notna().mean()) if len(dt) else 0.0
