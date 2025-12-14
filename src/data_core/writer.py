# src/data_core/writer.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd


Format = Literal["xlsx", "csv"]


@dataclass
class TableWriter:
    """
    Save prepared tables into <project_root>/PreparedTables.

    - User provides ONLY base name (no extension).
    - Default format is xlsx.
    - Always overwrites existing files (no versioning).
    - Does not modify the user's filename.
    """
    output_dir_name: str = "PreparedTables"

    def __post_init__(self):
        here = Path(__file__).resolve()
        project_root = self._find_project_root(here.parent)
        self.output_dir = project_root / self.output_dir_name
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _find_project_root(start: Path) -> Path:
        """
        Walk upwards looking for common project markers.
        Falls back to current working directory if not found.
        """
        markers = ("pyproject.toml", "setup.cfg", "setup.py", ".git")
        p = start.resolve()

        for parent in [p] + list(p.parents):
            for m in markers:
                if (parent / m).exists():
                    return parent

        return Path.cwd().resolve()

    @staticmethod
    def _validate_user_filename(name: str) -> None:
        """
        Do NOT modify user input. Only validate to prevent writing outside PreparedTables.
        """
        if name is None or name == "":
            raise ValueError("Filename cannot be empty.")

        if "/" in name or "\\" in name:
            raise ValueError("Filename must not contain '/' or '\\\\'.")

        if ".." in name:
            raise ValueError("Filename must not contain '..'.")

        # since you said user won't write extension:
        if name.lower().endswith((".xlsx", ".csv")):
            raise ValueError("Please enter filename WITHOUT extension (no .xlsx / .csv).")

    def save(
        self,
        table: pd.DataFrame,
        name: str,
        fmt: Format = "xlsx",
        *,
        index: bool = False,
    ) -> Path:
        """
        Save as PreparedTables/<name>.<fmt>. Always overwrites.
        """
        self._validate_user_filename(name)

        fmt = fmt.lower().strip()  # type: ignore
        if fmt not in ("xlsx", "csv"):
            raise ValueError(f"Unsupported format: {fmt}. Use 'xlsx' or 'csv'.")

        out_path = self.output_dir / f"{name}.{fmt}"

        if fmt == "xlsx":
            table.to_excel(out_path, index=index, engine="openpyxl")
        else:
            table.to_csv(out_path, index=index)

        return out_path

    def save_xlsx(self, table: pd.DataFrame, name: str, *, index: bool = False) -> Path:
        return self.save(table, name, fmt="xlsx", index=index)

    def save_csv(self, table: pd.DataFrame, name: str, *, index: bool = False) -> Path:
        return self.save(table, name, fmt="csv", index=index)
