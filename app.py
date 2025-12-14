import os
import tempfile
import streamlit as st
import pandas as pd

from src.data_core.reader import DataReader
from src.data_core.adjustments import TableRefiner
from src.intelligence.header import HeaderDetector
from src.intelligence.columns import ConsumptionColumnDetector, TimeColumnDetector

from src.intelligence.columns.time import (
    Preference_Date_And_Hour,
    Preference_SingleDateTime,
)

from src.data_core.writer import TableWriter


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
        "save_name": "",
        "saved_path": None,
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

            # reset save state on new upload
            st.session_state.save_name = ""
            st.session_state.saved_path = None

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

        # ✅ single-column flow
        if len(st.session_state.time_selected) == 1:
            single_col = st.session_state.time_selected[0]

            st.markdown("#### You selected one time-related column")
            st.write(
                "If this column already contains both **date + time** in one string "
                "(e.g., `01.01.2024, 00:00:00`), I can split it into normalized "
                "`date_norm` + `hour_norm` and create a `moment` timestamp."
            )

            single_mode = st.radio(
                "How should I interpret this column?",
                options=[
                    "This one column contains a full timestamp (date + time).",
                    "Keep it as-is for now (decide later).",
                ],
                index=0,
            )

            if single_mode.startswith("This one column contains a full timestamp"):
                try:
                    before_dtype = str(df[single_col].dtype)

                    pref = Preference_SingleDateTime(df, datetime_col=single_col)

                    extract_rate = pref.extract_date_and_hour()
                    moment_rate = pref.create_moment_column()

                    refiner2 = TableRefiner(pref.table)
                    refiner2.keep_only_moment_and_consumption(
                        moment_col="moment",
                        consumption_col="consumption_kwh",
                    )
                    # ✅ trim trailing empty rows + drop empty columns
                    refiner2.drop_trailing_empty_rows()
                    refiner2.drop_empty_columns()
                    pref.table = refiner2.table

                    st.session_state.df_processed = pref.table
                    df = st.session_state.df_processed

                    moment_dtype = str(df["moment"].dtype) if "moment" in df.columns else "N/A"

                    st.success(
                        f"✅ Parsed single column **{single_col}** "
                        f"(dtype: `{before_dtype}`) into `moment`."
                    )
                    st.success(f"✅ Extract success rate (date+hour): **{extract_rate:.2%}**")
                    st.success(
                        f"✅ Created **moment** column (dtype: `{moment_dtype}`), parse success rate: **{moment_rate:.2%}**"
                    )
                    st.success("✅ Dropped all other columns (kept only `moment` and `consumption_kwh`).")

                except Exception as e:
                    st.error(f"❌ I couldn't normalize the single datetime column: {e}")

        # two-column flow (unchanged)
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

                try:
                    date_col = st.session_state.date_col
                    hour_col = st.session_state.time_col

                    before_date_dtype = str(df[date_col].dtype)
                    before_hour_dtype = str(df[hour_col].dtype)

                    pref = Preference_Date_And_Hour(df, date_col=date_col, hour_col=hour_col)

                    pref.detect_date_dtype()
                    pref.normalize_hour_column()
                    moment_rate = pref.create_moment_column(out_col="moment")

                    refiner2 = TableRefiner(pref.table)
                    refiner2.keep_only_moment_and_consumption(
                        moment_col="moment",
                        consumption_col="consumption_kwh",
                    )
                    # ✅ trim trailing empty rows + drop empty columns
                    refiner2.drop_trailing_empty_rows()
                    refiner2.drop_empty_columns()
                    pref.table = refiner2.table

                    st.session_state.df_processed = pref.table
                    df = st.session_state.df_processed

                    moment_dtype = str(df["moment"].dtype) if "moment" in df.columns else "N/A"

                    st.success(
                        f"✅ Date column **{date_col}** normalized "
                        f"(dtype: `{before_date_dtype}` → `string`)."
                    )
                    st.success(
                        f"✅ Hour column **{hour_col}** normalized to `HH:MM:SS` "
                        f"(dtype: `{before_hour_dtype}` → `string`)."
                    )
                    st.success(
                        f"✅ Created **moment** column (dtype: `{moment_dtype}`), parse success rate: **{moment_rate:.2%}**"
                    )
                    st.success("✅ Dropped all other columns (kept only `moment` and `consumption_kwh`).")

                except Exception as e:
                    st.error(f"❌ I couldn't normalize/merge date+hour: {e}")

    # ==============================================================================
    # Final preview + Save
    # ==============================================================================
    df = st.session_state.df_processed
    final_ready = (
        isinstance(df, pd.DataFrame)
        and set(df.columns) == {"moment", "consumption_kwh"}
        and len(df) > 0
    )

    if final_ready:
        # ✅ ensure trailing empty rows removed at the very end too
        # ✅ and drop empty columns before saving
        try:
            ref_final = TableRefiner(df)
            ref_final.drop_trailing_empty_rows()
            ref_final.drop_empty_columns()
            df = ref_final.table
            st.session_state.df_processed = df
        except Exception:
            pass

        st.write("---")
        st.subheader("Final table preview")

        st.write("### First 10 rows")
        st.dataframe(df.head(10), use_container_width=True)

        st.write("### Last 10 rows")
        st.dataframe(df.tail(10), use_container_width=True)

        st.info(
            "This is your final table. To save it, please give your table a name.\n\n"
            "Example formats:\n"
            "- `ContractNumber_89578345`\n"
            "- `ContractId_8458_7384djfnjd_`"
        )

        st.session_state.save_name = st.text_input(
            "Table name (no extension):",
            value=st.session_state.save_name,
            placeholder="ContractNumber_89578345",
        )

        save_disabled = (st.session_state.save_name.strip() == "")

        s1, s2 = st.columns(2)
        with s1:
            if st.button("Save as Excel (.xlsx)", disabled=save_disabled):
                try:
                    writer = TableWriter()
                    out_path = writer.save_xlsx(df, st.session_state.save_name.strip(), index=False)
                    st.session_state.saved_path = str(out_path)
                    st.success(f"✅ Saved! File written to: `{st.session_state.saved_path}`")
                except Exception as e:
                    st.error(f"❌ Could not save file: {e}")

        with s2:
            if st.button("Save as CSV (.csv)", disabled=save_disabled):
                try:
                    writer = TableWriter()
                    out_path = writer.save_csv(df, st.session_state.save_name.strip(), index=False)
                    st.session_state.saved_path = str(out_path)
                    st.success(f"✅ Saved! File written to: `{st.session_state.saved_path}`")
                except Exception as e:
                    st.error(f"❌ Could not save file: {e}")

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
            st.session_state.save_name = ""
            st.session_state.saved_path = None
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
