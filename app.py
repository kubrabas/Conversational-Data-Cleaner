import os
import tempfile
import streamlit as st
import pandas as pd

from src.data_core.reader import DataReader
from src.data_core.adjustments import TableRefiner
from src.intelligence.header import HeaderDetector
from src.intelligence.columns import ConsumptionColumnDetector, TimeColumnDetector

from src.intelligence.columns.time import Preference_Date_And_Hour  # <- adjust if needed


# ==============================================================================
# Session State
# ==============================================================================
def init_state():
    defaults = {
        "step": 0,  # 0: upload, 1: preview+time select
        "df_processed": None,
        "consumption_col": None,
        "time_candidates": [],
        "time_selected": [],
        "time_pair_mode": None,
        "time_from_col": None,
        "time_to_col": None,
        "date_col": None,
        "time_col": None,
        "log": [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def log(msg: str):
    st.session_state.log.append(msg)


# ==============================================================================
# Core pipeline (automatic)
# ==============================================================================
def run_automatic_pipeline(file_path: str) -> dict:
    reader = DataReader(file_path)
    df_processed = reader.read_data()

    table = getattr(reader, "table", None)
    if table is None:
        table = df_processed

    refiner = TableRefiner(table)
    refiner.clean_table()
    table = refiner.table

    header_det = HeaderDetector(table)
    header_det.apply_header()
    table = header_det.table

    refiner = TableRefiner(table)
    refiner.clean_table()
    table = refiner.table

    cons_det = ConsumptionColumnDetector(table)
    consumption_col = cons_det.detect_consumption_column()
    _cons_kwh_series = cons_det.to_kwh()
    final_table = cons_det.table

    time_det = TimeColumnDetector(final_table)
    time_candidates = time_det.detect_time_columns()

    return {
        "df_processed": final_table,
        "consumption_col": consumption_col,
        "time_candidates": time_candidates,
    }


# ==============================================================================
# UI
# ==============================================================================
init_state()

st.title("Your Table Unification Assistant")
st.markdown("## Let's Get Your Table Ready!")

# ------------------------------------------------------------------------------
# STEP 0: Upload
# ------------------------------------------------------------------------------
if st.session_state.step == 0:
    st.write("Hey buddy! Upload your Excel file to start the cleaning and standardization process.")
    uploaded_file = st.file_uploader("Upload Data File (XLSX/XLS)", type=["xlsx", "xls"])

    if uploaded_file:
        try:
            suffix = os.path.splitext(uploaded_file.name)[-1].lower() or ".xlsx"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(uploaded_file.getbuffer())
                temp_path = tmp.name

            log(f"Uploaded file saved to temp: {temp_path}")

            results = run_automatic_pipeline(temp_path)

            st.session_state.df_processed = results["df_processed"]
            st.session_state.consumption_col = results["consumption_col"]
            st.session_state.time_candidates = results["time_candidates"]

            st.session_state.time_selected = []
            st.session_state.time_pair_mode = None
            st.session_state.time_from_col = None
            st.session_state.time_to_col = None
            st.session_state.date_col = None
            st.session_state.time_col = None

            os.remove(temp_path)

            st.session_state.step = 1
            st.rerun()

        except ValueError as e:
            st.error(f"Buddy, there was a ValueError: {e}")
        except Exception as e:
            st.error(f"Oops! An unexpected error occurred: {e}")

# ------------------------------------------------------------------------------
# STEP 1: Preview + Time selection
# ------------------------------------------------------------------------------
if st.session_state.step == 1:
    st.subheader("Data Processed (Automatic Steps Done)")
    st.info(
        "I automatically cleaned your table, applied header detection, "
        "and standardized the consumption column to kWh. Now let's handle time columns."
    )

    df = st.session_state.df_processed
    st.write("### Preview (first 10 rows)")
    st.dataframe(df.head(10), use_container_width=True)

    st.write("### Detected consumption column")
    if st.session_state.consumption_col:
        st.success(f"Consumption column detected: **{st.session_state.consumption_col}**")
    else:
        st.warning("I could not confidently detect a consumption column.")

    st.write("---")
    st.write("### Time-related columns detected")
    candidates = st.session_state.time_candidates or []

    if not candidates:
        st.warning("I could not find any time-related columns based on column names.")
    else:
        st.write("We found the following time-related columns in your uploaded file:")
        st.code("\n".join([f"- {c}" for c in candidates]), language="text")

        a, b, c = st.columns(3)
        with a:
            if st.button("Select all time columns"):
                st.session_state.time_selected = candidates
                st.rerun()
        with b:
            if st.button("Clear selection"):
                st.session_state.time_selected = []
                st.session_state.time_pair_mode = None
                st.session_state.time_from_col = None
                st.session_state.time_to_col = None
                st.session_state.date_col = None
                st.session_state.time_col = None
                st.rerun()
        with c:
            st.write(f"Selected: {len(st.session_state.time_selected)}")

        st.session_state.time_selected = st.multiselect(
            "Which of these columns would you like to use for time?",
            options=candidates,
            default=st.session_state.time_selected,
        )

        if st.session_state.time_selected:
            st.success(f"Selected time columns: {st.session_state.time_selected}")
        else:
            st.info("No time columns selected yet.")

        if len(st.session_state.time_selected) == 2:
            c1, c2 = st.session_state.time_selected

            st.markdown("#### You selected two time-related columns")
            st.write(
                "How should I interpret these two columns? "
                "This helps me decide whether to combine them into a single timestamp "
                "or treat them as separate time fields."
            )

            st.session_state.time_pair_mode = st.radio(
                "Choose one option:",
                options=[
                    "These two columns represent a start and end time (from → to).",
                    "These two columns together form a single timestamp (e.g., date + time).",
                    "These are two independent time fields (keep them separate).",
                    "Not sure yet (I want to decide later).",
                ],
                index=1,
            )

            if st.session_state.time_pair_mode.startswith("These two columns represent a start and end time"):
                st.session_state.time_from_col = st.selectbox(
                    "Start time column (from):",
                    options=[c1, c2],
                    index=0,
                )
                st.session_state.time_to_col = c2 if st.session_state.time_from_col == c1 else c1
                st.info(f"End time column (to): **{st.session_state.time_to_col}**")

            if st.session_state.time_pair_mode.startswith("These two columns together form a single timestamp"):
                st.session_state.date_col = st.selectbox(
                    "Which one is the date part?",
                    options=[c1, c2],
                    index=0,
                )
                st.session_state.time_col = c2 if st.session_state.date_col == c1 else c1
                st.info(f"Time part column: **{st.session_state.time_col}**")

                # Normalize DATE + HOUR and create "moment"
                try:
                    date_col = st.session_state.date_col
                    hour_col = st.session_state.time_col

                    before_date_dtype = str(df[date_col].dtype)
                    before_hour_dtype = str(df[hour_col].dtype)

                    pref = Preference_Date_And_Hour(df, date_col=date_col, hour_col=hour_col)

                    pref.detect_date_dtype()
                    pref.normalize_hour_column()
                    moment_rate = pref.create_moment_column(out_col="moment")

                    st.session_state.df_processed = pref.table
                    df = st.session_state.df_processed

                    after_date_dtype = str(df[date_col].dtype)
                    after_hour_dtype = str(df[hour_col].dtype)
                    moment_dtype = str(df["moment"].dtype) if "moment" in df.columns else "N/A"

                    st.success(
                        f"✅ Date column **{date_col}** normalized "
                        f"(dtype: `{before_date_dtype}` → `{after_date_dtype}`)."
                    )
                    st.success(
                        f"✅ Hour column **{hour_col}** normalized to `HH:MM:SS` "
                        f"(dtype: `{before_hour_dtype}` → `{after_hour_dtype}`)."
                    )
                    st.success(
                        f"✅ Created **moment** column (dtype: `{moment_dtype}`), parse success rate: **{moment_rate:.2%}**"
                    )

                    st.caption("Preview after normalization (first 10 rows):")
                    st.dataframe(df.head(10), use_container_width=True)

                except Exception as e:
                    st.error(f"❌ I couldn't normalize/merge date+hour: {e}")

    st.write("---")

    colA, colB = st.columns(2)
    with colA:
        if st.button("Back to upload"):
            st.session_state.step = 0
            st.session_state.df_processed = None
            st.session_state.consumption_col = None
            st.session_state.time_candidates = []
            st.session_state.time_selected = []
            st.session_state.time_pair_mode = None
            st.session_state.time_from_col = None
            st.session_state.time_to_col = None
            st.session_state.date_col = None
            st.session_state.time_col = None
            st.rerun()

    with colB:
        proceed_disabled = (len(st.session_state.time_selected) == 0)
        if st.button("Continue (next step)", disabled=proceed_disabled):
            st.info(
                "Next step: we will profile selected time columns and ask how to interpret them "
                "(combine, epoch, time-only, etc.)."
            )

    with st.expander("Debug log"):
        st.write(st.session_state.log)
