import json
import time
from datetime import datetime, timedelta

import numpy as np

from campus.config import ROOMS, SAMPLE_SECONDS, START_TIME


def generate_stream(steps=180, seed=42):
    """Yield one synthetic observation at a time without building a dataset."""
    rng = np.random.default_rng(seed)
    start = datetime.fromisoformat(START_TIME)
    for step in range(steps):
        for index, room in enumerate(ROOMS):
            occupancy = int(np.clip(np.rint(
                16 + 8 * np.sin(step / 23 + index) + rng.normal(0, 2)
            ), 0, 40))
            temperature = 22 + 0.009 * step + 0.5 * index + rng.normal(0, 0.22)
            co2 = 520 + 17 * occupancy + rng.normal(0, 28)
            if room == "C102" and 60 <= step < 110:
                co2 += 850
                temperature += 3
            if room == "C103" and 130 <= step < 139:
                temperature += 6
            if room == "C101" and step == 145:
                co2 += 1000
            record = {
                "event_id": f"{room}-{step:04d}",
                "timestamp": (start + timedelta(seconds=step * SAMPLE_SECONDS)).isoformat(),
                "room": room,
                "temperature_c": round(float(temperature), 2),
                "co2_ppm": round(float(co2), 1),
                "occupancy": occupancy,
            }
            if (room, step) == ("C101", 7):
                record["temperature_c"] = None
            if (room, step) == ("C102", 17):
                record["temperature_c"] = str(record["temperature_c"]).replace(".", ",")
            if (room, step) == ("C102", 22):
                record["co2_ppm"] = "bad"
            if (room, step) == ("C103", 35):
                record["occupancy"] = None
            if (room, step) == ("C101", 48):
                record["temperature_c"] = 99
            if (room, step) == ("C103", 72):
                record["timestamp"] = "bad-time"
            if (room, step) == ("C101", 96):
                record["occupancy"] = -1
            if (room, step) == ("C103", 151):
                record["co2_ppm"] = None
            yield record
            if (room, step) == ("C102", 118):
                yield record.copy()


def replay_stream(path):
    """Read JSONL incrementally; malformed JSON is quarantined by the cleaner."""
    def reject_constant(value):
        raise ValueError(f"Non-standard JSON constant: {value}")

    with open(path, encoding="utf-8") as file:
        for line_number, line in enumerate(file, 1):
            if not line.strip():
                continue
            try:
                yield json.loads(line, parse_constant=reject_constant)
            except ValueError:
                yield {"_source_error": f"invalid JSON at line {line_number}",
                       "raw_line": line.rstrip()}


def paced_stream(records, delay):
    for record in records:
        if delay:
            time.sleep(delay)
        yield record
