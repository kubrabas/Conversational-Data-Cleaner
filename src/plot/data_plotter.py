# src/plot/data_plotter.py
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


class DataPlotter:
    """
    Plots unified consumption data with columns:
      - moment (datetime-like)
      - consumption_kwh (numeric)

    Provides:
      - plot_full() -> matplotlib Figure
      - total_weeks() -> int
      - plot_last_week() -> dict(fig, week_index, start, end, total_weeks)
      - plot_random_week() -> dict(fig, week_index, start, end, total_weeks)
    """

    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe.copy()
        self._prepare()

    def _prepare(self) -> None:
        if "moment" not in self.df.columns or "consumption_kwh" not in self.df.columns:
            raise ValueError("Data must contain columns: 'moment' and 'consumption_kwh'.")

        self.df["moment"] = pd.to_datetime(self.df["moment"], errors="coerce")
        self.df["consumption_kwh"] = pd.to_numeric(self.df["consumption_kwh"], errors="coerce")

        self.df = self.df.dropna(subset=["moment", "consumption_kwh"]).sort_values("moment")

        # Week grouping: Monday->Sunday weeks (pandas default 'W' ends on Sunday)
        self.df["_week_start"] = self.df["moment"].dt.to_period("W").apply(lambda p: p.start_time)

        self._weeks_sorted = sorted(self.df["_week_start"].dropna().unique().tolist())
        self._week_to_index = {ws: i + 1 for i, ws in enumerate(self._weeks_sorted)}

    def total_weeks(self) -> int:
        return len(self._weeks_sorted)

    def plot_full(self):
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(self.df["moment"], self.df["consumption_kwh"], label="Full Data")
        ax.set_xlabel("Moment")
        ax.set_ylabel("Consumption (kWh)")
        ax.set_title("Full time range")
        ax.legend()
        fig.autofmt_xdate()
        return fig

    def _plot_week_start(self, week_start: pd.Timestamp):
        week_data = self.df[self.df["_week_start"] == week_start].copy()
        if week_data.empty:
            raise ValueError("Selected week has no data to plot.")

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(week_data["moment"], week_data["consumption_kwh"], label="Weekly Data")
        ax.set_xlabel("Moment")
        ax.set_ylabel("Consumption (kWh)")

        week_index = self._week_to_index.get(week_start, None)
        ax.set_title(f"Week {week_index} / {self.total_weeks()}")
        ax.legend()
        fig.autofmt_xdate()

        start = week_data["moment"].min()
        end = week_data["moment"].max()

        info = {
            "fig": fig,
            "week_index": int(week_index) if week_index is not None else None,
            "start": str(start),
            "end": str(end),
            "total_weeks": self.total_weeks(),
        }
        return info

    def plot_last_week(self):
        if not self._weeks_sorted:
            raise ValueError("No weekly segments found in the dataset.")
        last_week_start = self._weeks_sorted[-1]
        return self._plot_week_start(last_week_start)

    def plot_random_week(self):
        if not self._weeks_sorted:
            raise ValueError("No weekly segments found in the dataset.")
        rng = np.random.default_rng()
        random_week_start = rng.choice(self._weeks_sorted)
        return self._plot_week_start(random_week_start)
