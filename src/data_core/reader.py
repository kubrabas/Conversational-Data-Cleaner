import pandas as pd
import os
import csv


class DataReader:
    """
    Responsibility: To safely read the raw Excel data file (XLSX/XLS),
    detect its format, and return a Pandas DataFrame.
    CSV support added with automatic separator detection.

    NEW:
    - If an Excel file contains multiple sheets and we're running inside Streamlit,
      it will ask the user to pick a sheet, preview first/last 20 rows, and require
      confirmation before continuing.
    - If not running in Streamlit, it will raise a ValueError asking for sheet_name.
    """

    def __init__(self, file_path, sheet_name=None):
        self.file_path = file_path
        self.file_extension = os.path.splitext(file_path)[1].lower()
        self.sheet_name = sheet_name  # str/int/None
        self.table = None

    def _detect_csv_separator(self, sample_bytes: int = 65536) -> str:
        """
        Detect CSV delimiter by sampling the file content.
        Tries csv.Sniffer first; falls back to common delimiters.
        """
        encodings_to_try = ["utf-8-sig", "utf-8", "cp1252", "latin1"]

        for enc in encodings_to_try:
            try:
                with open(self.file_path, "r", encoding=enc, newline="") as f:
                    sample = f.read(sample_bytes)

                if not sample.strip():
                    return ","

                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
                    return dialect.delimiter
                except Exception:
                    candidates = [",", ";", "\t", "|"]
                    counts = {d: sample.count(d) for d in candidates}
                    best = max(counts, key=counts.get)
                    return best if counts[best] > 0 else ","
            except Exception:
                continue

        return ","

    def _get_excel_sheet_names(self) -> list:
        try:
            xls = pd.ExcelFile(self.file_path)
            return list(xls.sheet_names or [])
        except Exception as e:
            raise ValueError(f"Could not inspect Excel sheets: {e}")

    def _maybe_streamlit_sheet_picker(self, sheet_names: list) -> str:
        """
        If Streamlit is available and there are multiple sheets, ask user to select one.
        Requires confirmation. Shows head/tail preview before continuing.

        Returns:
            Selected sheet name (str) if confirmed; otherwise stops execution.
        """
        try:
            import streamlit as st
        except Exception:
            raise ValueError(
                "Excel file contains multiple sheets but Streamlit is not available. "
                f"Sheets found: {sheet_names}. "
                "Please pass `sheet_name=...` to DataReader."
            )

        base = "datareader_excel_sheet_picker"
        sig_key = f"{base}_signature"
        selected_key = f"{base}_selected"
        confirmed_key = f"{base}_confirmed"

        signature = tuple(sheet_names)

        # Reset if this is a different workbook (different sheet list)
        if st.session_state.get(sig_key) != signature:
            st.session_state[sig_key] = signature
            st.session_state[selected_key] = sheet_names[0] if sheet_names else None
            st.session_state[confirmed_key] = False

        st.warning("I found multiple sheets in this Excel file.")
        st.write("Which sheet should I use?")

        selected = st.radio(
            "Available sheets:",
            options=sheet_names,
            index=sheet_names.index(st.session_state.get(selected_key, sheet_names[0])),
            key=f"{base}_radio",
        )

        # If user changes selection after confirming, unconfirm
        if selected != st.session_state.get(selected_key):
            st.session_state[selected_key] = selected
            st.session_state[confirmed_key] = False

        # Preview selected sheet (first 20 + last 20)
        try:
            preview_df = pd.read_excel(
                self.file_path,
                sheet_name=st.session_state[selected_key],
                skiprows=0,
                header=None,
            )
            st.write("### Preview (first 20 rows):")
            st.dataframe(preview_df.head(20), use_container_width=True)
            st.write("### Preview (last 20 rows):")
            st.dataframe(preview_df.tail(20), use_container_width=True)
        except Exception as e:
            st.error(f"Could not preview the selected sheet: {e}")
            st.stop()

        if st.button("Confirm sheet selection"):
            st.session_state[confirmed_key] = True

        if not st.session_state.get(confirmed_key, False):
            st.info("Please confirm the sheet selection to continue.")
            st.stop()

        return st.session_state[selected_key]

    def read_data(self):
        """
        Reads the data using the appropriate Pandas function based on file extension.
        Supports XLSX/XLS and CSV (with auto separator detection).

        Excel multi-sheet behavior:
        - If `sheet_name` is provided, reads that sheet.
        - If not and there are multiple sheets:
            - In Streamlit: asks the user to choose + confirm, shows first/last 20, then continues.
            - Outside Streamlit: raises ValueError asking for sheet_name.
        """
        if self.file_extension in [".xlsx", ".xls"]:
            if self.sheet_name is not None:
                self.table = pd.read_excel(
                    self.file_path,
                    sheet_name=self.sheet_name,
                    skiprows=0,
                    header=None,
                )
            else:
                sheet_names = self._get_excel_sheet_names()
                if not sheet_names:
                    raise ValueError("No sheets found in the Excel file.")

                if len(sheet_names) == 1:
                    self.table = pd.read_excel(
                        self.file_path,
                        sheet_name=sheet_names[0],
                        skiprows=0,
                        header=None,
                    )
                else:
                    chosen_sheet = self._maybe_streamlit_sheet_picker(sheet_names)
                    self.table = pd.read_excel(
                        self.file_path,
                        sheet_name=chosen_sheet,
                        skiprows=0,
                        header=None,
                    )

        elif self.file_extension == ".csv":
            sep = self._detect_csv_separator()

            encodings_to_try = ["utf-8-sig", "utf-8", "cp1252", "latin1"]
            last_err = None
            for enc in encodings_to_try:
                try:
                    self.table = pd.read_csv(
                        self.file_path,
                        sep=sep,
                        header=None,
                        encoding=enc,
                        engine="python",
                    )
                    last_err = None
                    break
                except Exception as e:
                    last_err = e

            if last_err is not None:
                raise ValueError(f"CSV could not be read. Detected sep='{sep}'. Last error: {last_err}")

        else:
            raise ValueError(
                f"Unsupported file type: {self.file_extension}. Only XLSX/XLS/CSV formats are accepted."
            )

        if self.table is None or self.table.empty:
            raise ValueError("Data failed to load or the file is empty after reading.")

        return self.table
