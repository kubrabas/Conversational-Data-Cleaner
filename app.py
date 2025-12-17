import os
import tempfile
import streamlit as st
import pandas as pd
import matplotlib.dates as mdates  # <-- NEW

from src.data_core.reader import DataReader
from src.data_core.adjustments import TableRefiner
from src.intelligence.header import HeaderDetector
from src.intelligence.columns import ConsumptionColumnDetector, TimeColumnDetector

from src.intelligence.columns.time import (
    Preference_Date_And_Hour,
    Preference_SingleDateTime,
)

from src.data_core.writer import TableWriter

# --- plotting helper class ---
from src.plot.data_plotter import DataPlotter


# ==============================================================================
# Session State
# ==============================================================================
def init_state():
    defaults = {
        "step": 0,  # 0: upload, 1: preview+time select
        "df_raw": None,  # raw upload (no changes)
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
        "pipeline_summary": None,  # still stored, but not shown

        # --- keep temp file across reruns (needed for multi-sheet pick) ---
        "uploaded_temp_path": None,
        "uploaded_file_name": None,

        # --- UI confirm gates (no dropdowns + must confirm) ---
        "time_cols_confirmed": False,
        "time_selected_snapshot": [],

        "single_mode_confirmed": False,
        "single_mode_value": None,

        "pair_mode_confirmed": False,
        "pair_mode_value": None,

        "from_to_confirmed": False,
        "from_col_snapshot": None,
        "to_col_snapshot": None,

        "date_hour_confirmed": False,
        "date_col_snapshot": None,
        "time_col_snapshot": None,

        # --- plot flow state ---
        "plot_wants": "No",  # "No" | "Yes"
        "random_week_info": None,  # dict or None
        "random_week_clicks": 0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def log(msg: str):
    st.session_state.log.append(msg)


def _cleanup_uploaded_temp_if_exists():
    temp_path = st.session_state.get("uploaded_temp_path")
    if temp_path and os.path.exists(temp_path):
        try:
            os.remove(temp_path)
        except Exception:
            pass
    st.session_state.uploaded_temp_path = None
    st.session_state.uploaded_file_name = None


# --- NEW: format x-axis ticks as DD-MM-YYYY HH:MM ---
def _format_datetime_xaxis(fig):
    try:
        if fig and getattr(fig, "axes", None):
            ax = fig.axes[0]
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%m-%Y %H:%M"))
            fig.autofmt_xdate()
    except Exception:
        pass
    return fig


# ==============================================================================
# Core pipeline (automatic)
# ==============================================================================
def run_automatic_pipeline(file_path: str) -> dict:
    reader = DataReader(file_path)
    df_processed = reader.read_data()

    table = getattr(reader, "table", None)
    if table is None:
        table = df_processed

    raw_table = table.copy()
    raw_shape = raw_table.shape

    refiner1 = TableRefiner(table)
    refiner1.clean_table()
    table = refiner1.table
    clean1_shape = table.shape

    header_det = HeaderDetector(table)
    header_det.apply_header()
    table = header_det.table
    header_shape = table.shape

    refiner2 = TableRefiner(table)
    refiner2.clean_table()
    table = refiner2.table
    clean2_shape = table.shape

    cons_det = ConsumptionColumnDetector(table)
    consumption_col = cons_det.detect_consumption_column()
    _cons_kwh_series = cons_det.to_kwh()
    final_table = cons_det.table
    final_shape = final_table.shape

    time_det = TimeColumnDetector(final_table)
    time_candidates = time_det.detect_time_columns()

    summary = {
        "raw_shape": raw_shape,
        "clean1_shape": clean1_shape,
        "header_shape": header_shape,
        "clean2_shape": clean2_shape,
        "final_shape": final_shape,
        "consumption_col": consumption_col,
        "time_candidates_count": len(time_candidates) if time_candidates else 0,
    }

    return {
        "df_raw": raw_table,
        "df_processed": final_table,
        "consumption_col": consumption_col,
        "time_candidates": time_candidates,
        "summary": summary,
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
    st.write("Upload your data file to start the cleaning and standardization process.")
    uploaded_file = st.file_uploader(
        "Upload Data File (XLSX/XLS/CSV)", type=["xlsx", "xls", "csv"]
    )

    if uploaded_file:
        try:
            if st.session_state.uploaded_file_name != uploaded_file.name:
                _cleanup_uploaded_temp_if_exists()

            if st.session_state.uploaded_temp_path is None:
                suffix = os.path.splitext(uploaded_file.name)[-1].lower() or ".xlsx"
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp.write(uploaded_file.getbuffer())
                    st.session_state.uploaded_temp_path = tmp.name
                st.session_state.uploaded_file_name = uploaded_file.name
                log(f"Uploaded file saved to temp: {st.session_state.uploaded_temp_path}")

            temp_path = st.session_state.uploaded_temp_path

            results = run_automatic_pipeline(temp_path)

            st.session_state.df_raw = results["df_raw"]
            st.session_state.df_processed = results["df_processed"]
            st.session_state.consumption_col = results["consumption_col"]
            st.session_state.time_candidates = results["time_candidates"]
            st.session_state.pipeline_summary = results.get("summary")

            st.session_state.time_selected = []
            st.session_state.time_pair_mode = None
            st.session_state.time_from_col = None
            st.session_state.time_to_col = None
            st.session_state.date_col = None
            st.session_state.time_col = None

            st.session_state.save_name = ""
            st.session_state.saved_path = None

            st.session_state.plot_wants = "No"
            st.session_state.random_week_info = None
            st.session_state.random_week_clicks = 0

            st.session_state.time_cols_confirmed = False
            st.session_state.time_selected_snapshot = []

            st.session_state.single_mode_confirmed = False
            st.session_state.single_mode_value = None

            st.session_state.pair_mode_confirmed = False
            st.session_state.pair_mode_value = None

            st.session_state.from_to_confirmed = False
            st.session_state.from_col_snapshot = None
            st.session_state.to_col_snapshot = None

            st.session_state.date_hour_confirmed = False
            st.session_state.date_col_snapshot = None
            st.session_state.time_col_snapshot = None

            _cleanup_uploaded_temp_if_exists()

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
    df_raw = st.session_state.df_raw
    if isinstance(df_raw, pd.DataFrame):
        st.subheader("Original upload preview")
        st.write("### First 20 rows:")
        st.dataframe(df_raw.head(20), use_container_width=True)
        st.write("### Last 20 rows:")
        st.dataframe(df_raw.tail(20), use_container_width=True)

    st.write("---")
    st.subheader("Consumption column")

    consumption_col = st.session_state.consumption_col
    if consumption_col:
        st.success(
            f"Selected consumption column: **{consumption_col}** → "
            f"standardized to **consumption_kwh**."
        )
    else:
        st.warning(
            "I could not confidently detect a consumption column, so no kWh standardization was applied."
        )

    st.write("---")
    st.write("### Time-related column")
    candidates = st.session_state.time_candidates or []

    if not candidates:
        st.warning("I could not find any time-related columns based on column names.")
    else:
        st.write("We found the following time-related columns in your uploaded file:")
        st.code("\n".join([f"- {c}" for c in candidates]), language="text")

        st.markdown("#### Select time-related columns")
        new_selected = []
        for colname in candidates:
            checked = st.checkbox(
                colname,
                value=(colname in st.session_state.time_selected),
                key=f"timecol_chk_{colname}",
            )
            if checked:
                new_selected.append(colname)

        if new_selected != st.session_state.time_selected_snapshot:
            st.session_state.time_cols_confirmed = False

        st.session_state.time_selected = new_selected
        df = st.session_state.df_processed

        if not st.session_state.time_selected:
            st.info("No time related columns selected yet.")

        if st.session_state.time_selected:
            if st.button("Confirm selection"):
                st.session_state.time_cols_confirmed = True
                st.session_state.time_selected_snapshot = list(st.session_state.time_selected)
                st.rerun()
        else:
            st.session_state.time_cols_confirmed = False
            st.session_state.time_selected_snapshot = []

        if st.session_state.time_cols_confirmed and len(st.session_state.time_selected) == 1:
            single_col = st.session_state.time_selected[0]

            st.markdown("#### You selected one time-related column")
            st.write("What is the format of the column you selected?")

            single_mode = st.radio(
                "Choose one option:",
                options=[
                    "It contains both date and hour information (e.g., `01.01.2024, 00:00:00`).",
                ],
                index=0,
            )

            if st.session_state.single_mode_value != single_mode:
                st.session_state.single_mode_confirmed = False
            st.session_state.single_mode_value = single_mode

            if st.button("Confirm this interpretation"):
                st.session_state.single_mode_confirmed = True
                st.rerun()

            if not st.session_state.single_mode_confirmed:
                st.warning("Confirm the interpretation to proceed.")
            else:
                if single_mode.startswith("It contains both date and hour information"):
                    try:
                        pref = Preference_SingleDateTime(df, datetime_col=single_col)
                        pref.extract_date_and_hour()
                        pref.create_moment_column()

                        refiner2 = TableRefiner(pref.table)
                        refiner2.keep_only_moment_and_consumption(
                            moment_col="moment",
                            consumption_col="consumption_kwh",
                        )
                        refiner2.drop_trailing_empty_rows()
                        refiner2.drop_empty_columns()
                        pref.table = refiner2.table

                        st.session_state.df_processed = pref.table
                        df = st.session_state.df_processed

                        st.success("Success! Your final table is ready.")
                    except Exception as e:
                        st.error(f"I couldn't normalize the single datetime column: {e}")

        if st.session_state.time_cols_confirmed and len(st.session_state.time_selected) == 2:
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
                    "These two columns together form a single timestamp (date + hour).",
                ],
                index=1,
            )

            if st.session_state.pair_mode_value != st.session_state.time_pair_mode:
                st.session_state.pair_mode_confirmed = False
            st.session_state.pair_mode_value = st.session_state.time_pair_mode

            if st.button("Confirm this interpretation"):
                st.session_state.pair_mode_confirmed = True
                st.rerun()

            if not st.session_state.pair_mode_confirmed:
                st.warning("Confirm the interpretation to continue.")

            if (
                st.session_state.pair_mode_confirmed
                and st.session_state.time_pair_mode.startswith(
                    "These two columns represent a start and end time"
                )
            ):
                st.session_state.time_from_col = st.radio(
                    "Start time column (from):",
                    options=[c1, c2],
                    index=0,
                    key="from_col_radio",
                )
                from_col = st.session_state.time_from_col
                to_col = c2 if from_col == c1 else c1
                st.session_state.time_to_col = to_col
                st.info(f"End time column (to): **{to_col}**")

                if (from_col != st.session_state.from_col_snapshot) or (
                    to_col != st.session_state.to_col_snapshot
                ):
                    st.session_state.from_to_confirmed = False

                if st.button("Confirm from → to mapping"):
                    st.session_state.from_to_confirmed = True
                    st.session_state.from_col_snapshot = from_col
                    st.session_state.to_col_snapshot = to_col
                    st.rerun()

                if not st.session_state.from_to_confirmed:
                    st.warning("Confirm from/to to proceed.")

            if (
                st.session_state.pair_mode_confirmed
                and st.session_state.time_pair_mode.startswith(
                    "These two columns together form a single timestamp"
                )
            ):
                st.session_state.date_col = st.radio(
                    "From the two columns you selected, which one should be used to extract the date information?",
                    options=[c1, c2],
                    index=0,
                    key="date_col_radio",
                )
                date_col = st.session_state.date_col
                hour_col = c2 if date_col == c1 else c1
                st.session_state.time_col = hour_col

                st.info(
                    f"Date information will be extracted using the **{date_col}** column.\n\n"
                    f"Hour information will be extracted using the **{hour_col}** column."
                )

                if (date_col != st.session_state.date_col_snapshot) or (
                    hour_col != st.session_state.time_col_snapshot
                ):
                    st.session_state.date_hour_confirmed = False

                if st.button("Confirm date + hour mapping"):
                    st.session_state.date_hour_confirmed = True
                    st.session_state.date_col_snapshot = date_col
                    st.session_state.time_col_snapshot = hour_col
                    st.rerun()

                if not st.session_state.date_hour_confirmed:
                    st.warning("Confirm date/hour to proceed with merging & parsing.")
                else:
                    try:
                        pref = Preference_Date_And_Hour(df, date_col=date_col, hour_col=hour_col)
                        pref.detect_date_dtype()
                        pref.normalize_hour_column()
                        pref.create_moment_column(out_col="moment")

                        refiner2 = TableRefiner(pref.table)
                        refiner2.keep_only_moment_and_consumption(
                            moment_col="moment",
                            consumption_col="consumption_kwh",
                        )
                        refiner2.drop_trailing_empty_rows()
                        refiner2.drop_empty_columns()
                        pref.table = refiner2.table

                        st.session_state.df_processed = pref.table
                        df = st.session_state.df_processed

                        st.success("Success! Your final table is ready.")
                    except Exception as e:
                        st.error(f"I couldn't normalize/merge date+hour: {e}")

    # ==============================================================================
    # Final preview + Plot (optional) + Save
    # ==============================================================================
    df = st.session_state.df_processed

    try:
        ref_shift = TableRefiner(df)
        ref_shift.shift_moment_minus_15_if_first15_last00(moment_col="moment")
        df = ref_shift.table
        st.session_state.df_processed = df
    except Exception:
        pass

    final_ready = (
        isinstance(df, pd.DataFrame)
        and set(df.columns) == {"moment", "consumption_kwh"}
        and len(df) > 0
    )

    if final_ready:
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

        st.write("### First 20 rows:")
        st.dataframe(df.head(20), use_container_width=True)

        st.write("### Last 20 rows:")
        st.dataframe(df.tail(20), use_container_width=True)

        st.write("---")
        st.subheader("Optional: Plot your unified data")

        st.session_state.plot_wants = st.radio(
            "Do you want to plot this data before downloading?",
            options=["No", "Yes"],
            index=0 if st.session_state.plot_wants != "Yes" else 1,
            key="plot_wants_radio",
        )

        if st.session_state.plot_wants == "Yes":
            try:
                plotter = DataPlotter(df)

                st.markdown("#### Full time range")
                fig_full = plotter.plot_full()
                fig_full = _format_datetime_xaxis(fig_full)  # <-- NEW
                st.pyplot(fig_full, use_container_width=True)

                total_weeks = plotter.total_weeks()
                st.info(f"Total available weeks in this dataset: **{total_weeks}**")

                st.markdown("#### Last week")
                last_info = plotter.plot_last_week()
                last_info["fig"] = _format_datetime_xaxis(last_info["fig"])  # <-- NEW
                st.info(
                    f"Plotting last week: **Week {last_info['week_index']} / {last_info.get('total_weeks', total_weeks)}** "
                    f"({last_info['start']} → {last_info['end']})"
                )
                st.pyplot(last_info["fig"], use_container_width=True)

                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Plot another random week"):
                        st.session_state.random_week_clicks += 1
                        st.session_state.random_week_info = plotter.plot_random_week()
                        st.rerun()

                with c2:
                    st.button("Continue to download")

                if st.session_state.random_week_info is not None:
                    info = st.session_state.random_week_info
                    st.markdown("#### Random week")
                    st.info(
                        f"Randomly selected: **Week {info['week_index']} / {info.get('total_weeks', total_weeks)}** "
                        f"({info['start']} → {info['end']})"
                    )
                    fig_rand = _format_datetime_xaxis(info["fig"])  # <-- NEW
                    st.pyplot(fig_rand, use_container_width=True)

            except Exception as e:
                st.error(f"Plotting failed: {e}")

        st.write("---")
        st.info(
            "Your final table is ready. To save it, please give your table a name.\n\n"
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
                    st.success(f"Saved! File written to: `{st.session_state.saved_path}`")
                except Exception as e:
                    st.error(f"Could not save file: {e}")

        with s2:
            if st.button("Save as CSV (.csv)", disabled=save_disabled):
                try:
                    writer = TableWriter()
                    out_path = writer.save_csv(df, st.session_state.save_name.strip(), index=False)
                    st.session_state.saved_path = str(out_path)
                    st.success(f"Saved! File written to: `{st.session_state.saved_path}`")
                except Exception as e:
                    st.error(f"Could not save file: {e}")

    st.write("---")

    colA, colB = st.columns(2)
    with colA:
        if st.button("Back to upload"):
            st.session_state.step = 0
            st.session_state.df_raw = None
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
            st.session_state.pipeline_summary = None

            st.session_state.plot_wants = "No"
            st.session_state.random_week_info = None
            st.session_state.random_week_clicks = 0

            st.session_state.time_cols_confirmed = False
            st.session_state.time_selected_snapshot = []

            st.session_state.single_mode_confirmed = False
            st.session_state.single_mode_value = None

            st.session_state.pair_mode_confirmed = False
            st.session_state.pair_mode_value = None

            st.session_state.from_to_confirmed = False
            st.session_state.from_col_snapshot = None
            st.session_state.to_col_snapshot = None

            st.session_state.date_hour_confirmed = False
            st.session_state.date_col_snapshot = None
            st.session_state.time_col_snapshot = None

            _cleanup_uploaded_temp_if_exists()
            st.rerun()

    with colB:
        proceed_disabled = (len(st.session_state.time_selected) == 0) or (not st.session_state.time_cols_confirmed)
        if st.button("Continue (next step)", disabled=proceed_disabled):
            st.info(
                "Next step: we will profile selected time columns and ask how to interpret them "
                "(combine, epoch, time-only, etc.)."
            )

    with st.expander("Debug log"):
        st.write(st.session_state.log)
