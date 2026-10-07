import argparse
import json
import platform
from collections import Counter
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

from campus.config import (
    CAPACITY, CO2_LIMIT, JUMP_LIMIT, MIN_CO2_SAMPLES, SAMPLE_SECONDS,
    START_TIME, TEMP_LIMIT, WINDOW_SECONDS,
)
from campus.processing import Cleaner, StreamProcessor
from campus.source import generate_stream, paced_stream, replay_stream
from campus.storage import ResultStore
from campus.visuals import save_summary_and_charts


def run_pipeline(steps=180, seed=42, delay=0, input_path=None, output="outputs/run",
                 print_every=30):
    if steps < 1 or delay < 0 or print_every < 0:
        raise ValueError("steps must be positive; delay and print_every cannot be negative")
    directory = Path(output)
    if directory.exists() and any(directory.iterdir()):
        raise FileExistsError(f"Output folder is not empty: {directory}. Choose a new --output.")
    if input_path is not None and not Path(input_path).is_file():
        raise FileNotFoundError(input_path)
    source = replay_stream(input_path) if input_path else generate_stream(steps, seed)
    cleaner, processor = Cleaner(), StreamProcessor()
    store = ResultStore(directory)
    log = (directory / "console.log").open("w", encoding="utf-8")
    counts, rejection_reasons, rule_counts = Counter(), Counter(), Counter()
    interrupted = False
    first_event, last_event = None, None

    def emit(line):
        log.write(line + "\n")
        log.flush()
        if print_every:
            print(line, flush=True)

    emit("SYNTHETIC CLASSROOM STREAM | UTC event timestamps | window: (t-60s, t]")
    emit("received accepted rejected | event time          room meanCO2 stdCO2 maxTemp n60 status")
    try:
        for raw in paced_stream(source, delay):
            counts["received"] += 1
            store.write_raw(raw)
            record, reason = cleaner.clean(raw)
            if reason:
                counts["rejected"] += 1
                rejection_reasons[reason] += 1
                store.reject(raw, reason)
                if print_every:
                    emit(f"REJECT #{counts['received']}: {reason}")
            else:
                first_event = min(first_event or record["timestamp"], record["timestamp"])
                last_event = max(last_event or record["timestamp"], record["timestamp"])
                metrics = processor.update(record)
                store.accept(metrics)
                counts["accepted"] += 1
                counts["imputed_records"] += int(record["is_imputed"])
                counts["normalized_records"] += int(record["normalized_decimal"])
                counts["alert_observations"] += int(metrics["alert"])
                if metrics["flags"]:
                    rule_counts.update(metrics["flags"].split(";"))
                if print_every and (counts["received"] == 1
                                    or counts["received"] % print_every == 0 or metrics["alert"]):
                    emit(f"{counts['received']:8d} {counts['accepted']:8d} {counts['rejected']:8d} | "
                         f"{record['timestamp'][11:19]} {record['room']} "
                         f"{metrics['co2_mean_60s']:7.2f} {metrics['co2_std_60s']:6.2f} "
                         f"{metrics['temperature_max_60s']:7.2f} {metrics['event_count_60s']:3d} "
                         f"{metrics['flags'] or 'OK'}")
            store.flush()
    except KeyboardInterrupt:
        interrupted = True
        emit("Stopped by user; results received so far are retained.")
    finally:
        store.close()
        log.close()

    manifest = {
        "source": "JSONL replay" if input_path else "seeded synthetic generator",
        "source_file": str(input_path) if input_path else None,
        "seed": seed if input_path is None else None,
        "requested_steps": steps if input_path is None else None,
        "sample_interval_seconds_per_room": SAMPLE_SECONDS if input_path is None else None,
        "event_start_utc": first_event,
        "event_end_utc": last_event,
        "wall_delay_seconds_per_record": delay,
        "window_seconds": WINDOW_SECONDS,
        "window_boundary": "(t - 60 seconds, t]",
        "thresholds": {"co2_mean_ppm": CO2_LIMIT, "temperature_c": TEMP_LIMIT,
                       "positive_co2_jump_ppm": JUMP_LIMIT, "min_co2_samples": MIN_CO2_SAMPLES,
                       "classroom_capacity": CAPACITY},
        "counts": {name: counts[name] for name in ["received", "accepted", "rejected",
                    "imputed_records", "normalized_records", "alert_observations"]},
        "rejection_reasons": dict(rejection_reasons),
        "rule_matches": dict(rule_counts),
        "interrupted": interrupted,
        "versions": {"python": platform.python_version(), "pandas": pd.__version__,
                     "numpy": np.__version__, "matplotlib": matplotlib.__version__},
    }
    (directory / "run_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    summary = save_summary_and_charts(directory)
    if print_every:
        print("\nFinal summary\n" + summary.to_string(index=False))
        print(f"Saved to {directory}")
    return manifest


def main():
    parser = argparse.ArgumentParser(description="Smart campus streaming demo")
    parser.add_argument("--steps", type=int, default=180, help="samples per room")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--delay", type=float, default=0.15, help="wall seconds between records")
    parser.add_argument("--input", dest="input_path", help="replay a JSONL source line by line")
    parser.add_argument("--output", default="outputs/run")
    parser.add_argument("--print-every", type=int, default=30)
    args = parser.parse_args()
    try:
        run_pipeline(**vars(args))
    except (ValueError, FileExistsError, FileNotFoundError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
