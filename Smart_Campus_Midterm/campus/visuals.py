import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from campus.config import CO2_LIMIT, TEMP_LIMIT

COLORS = {"C101": "#197998", "C102": "#DC7447", "C103": "#7162A3"}


def save_summary_and_charts(directory):
    frame = pd.read_csv(directory / "metrics.csv")
    if frame.empty:
        return pd.DataFrame()
    summary = frame.groupby("room").agg(
        accepted=("event_id", "size"),
        mean_co2_ppm=("co2_ppm", "mean"),
        max_co2_ppm=("co2_ppm", "max"),
        mean_temperature_c=("temperature_c", "mean"),
        max_temperature_c=("temperature_c", "max"),
        mean_occupancy=("occupancy", "mean"),
        alert_observations=("alert", "sum"),
        imputed_records=("is_imputed", "sum"),
    ).round(2).reset_index()
    summary.to_csv(directory / "summary_by_room.csv", index=False)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["minute"] = (frame["timestamp"] - frame["timestamp"].min()).dt.total_seconds() / 60
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "axes.spines.top": False, "axes.spines.right": False})
    figure, axes = plt.subplots(2, 1, figsize=(10, 6.3), sharex=True, layout="constrained")
    for room, group in frame.groupby("room"):
        axes[0].plot(group["minute"], group["co2_mean_60s"], color=COLORS[room], label=room, lw=1.8)
        axes[1].plot(group["minute"], group["temperature_c"], color=COLORS[room], label=room, lw=1.2)
    axes[0].axhline(CO2_LIMIT, color="#666666", ls="--", lw=1, label="Demo rule: 1200 ppm")
    axes[1].axhline(TEMP_LIMIT, color="#666666", ls="--", lw=1, label="Demo rule: 28 C")
    axes[0].set(ylabel="CO2 rolling mean (ppm)", title="Classroom conditions during the synthetic stream")
    axes[1].set(ylabel="Temperature (C)", xlabel="Event time: minutes from 04:00 UTC")
    for axis in axes:
        axis.legend(loc="upper left", ncol=4, fontsize=8)
        axis.grid(alpha=0.2)
    figure.savefig(directory / "stream_results.png", dpi=180)
    plt.close(figure)

    figure, axes = plt.subplots(1, 2, figsize=(10, 3.4), layout="constrained")
    axes[0].bar(summary["room"], summary["alert_observations"], color=[COLORS[r] for r in summary["room"]])
    axes[0].set(title="Flagged observations by room", ylabel="Observations")
    axes[0].set_ylim(0, max(1, float(summary["alert_observations"].max()) * 1.2 + 1))
    for i, count in enumerate(summary["alert_observations"]):
        axes[0].text(i, count + 0.5, str(count), ha="center")
    counts = {rule: int(frame["flags"].fillna("").str.contains(rule, regex=False).sum())
              for rule in ["CO2_HIGH", "TEMP_HIGH", "CO2_JUMP"]}
    axes[1].bar(list(counts), list(counts.values()), color="#197998")
    axes[1].set(title="Rule matches can overlap", ylabel="Matches")
    axes[1].set_ylim(0, max(1, max(counts.values()) * 1.2 + 1))
    for i, count in enumerate(counts.values()):
        axes[1].text(i, count + 0.5, str(count), ha="center")
    figure.savefig(directory / "alert_summary.png", dpi=180)
    plt.close(figure)
    return summary
