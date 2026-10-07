from collections import defaultdict, deque

import numpy as np
import pandas as pd

from campus.config import (
    CAPACITY, CO2_LIMIT, JUMP_LIMIT, MIN_CO2_SAMPLES, ROOMS,
    TEMP_LIMIT, WINDOW_SECONDS,
)


class Cleaner:
    def __init__(self):
        self.seen_ids = set()
        self.last_time = {}
        self.last_observed = defaultdict(dict)

    def clean(self, raw):
        if not isinstance(raw, dict):
            return None, "record is not an object"
        if "_source_error" in raw:
            return None, raw["_source_error"]
        event_id = raw.get("event_id")
        if not isinstance(event_id, str) or not event_id.strip():
            return None, "missing event_id"
        if event_id in self.seen_ids:
            return None, "duplicate event_id"
        room = raw.get("room")
        if room not in ROOMS:
            return None, "unknown room"
        value = raw.get("timestamp")
        if not isinstance(value, str):
            return None, "invalid timestamp"
        timestamp = pd.to_datetime(value, format="ISO8601", utc=True, errors="coerce")
        if pd.isna(timestamp):
            return None, "invalid timestamp"
        if room in self.last_time and timestamp <= self.last_time[room]:
            return None, "non-increasing room timestamp"

        columns = ["temperature_c", "co2_ppm", "occupancy"]
        values = [raw.get(column) for column in columns]
        if any(isinstance(v, (dict, list, bool)) for v in values):
            return None, "invalid numeric type"
        text = pd.Series(values, index=columns, dtype="object").astype("string")
        normalized = bool(text.str.contains(",", regex=False).fillna(False).any())
        numbers = pd.to_numeric(text.str.replace(",", ".", regex=False), errors="coerce")
        numbers = numbers.astype(float)
        imputed = []
        for column in columns:
            if pd.isna(numbers[column]):
                previous = self.last_observed[room].get(column)
                if (column == "occupancy" or previous is None
                        or (timestamp - previous[1]).total_seconds() > WINDOW_SECONDS):
                    return None, f"missing {column} without recent observation"
                numbers[column] = previous[0]
                imputed.append(column)
        if not np.isfinite(numbers.to_numpy()).all():
            return None, "non-finite numeric value"
        bounds = {"temperature_c": (0, 50), "co2_ppm": (250, 5000), "occupancy": (0, 60)}
        for column, (low, high) in bounds.items():
            if not low <= numbers[column] <= high:
                return None, f"out-of-range {column}"
        if not float(numbers["occupancy"]).is_integer():
            return None, "occupancy is not an integer"

        clean = {"event_id": event_id, "timestamp": timestamp.isoformat(), "room": room,
                 "temperature_c": float(numbers["temperature_c"]),
                 "co2_ppm": float(numbers["co2_ppm"]),
                 "occupancy": int(numbers["occupancy"]),
                 "is_imputed": bool(imputed), "imputed_fields": ";".join(imputed),
                 "normalized_decimal": normalized}
        self.seen_ids.add(event_id)
        self.last_time[room] = timestamp
        for column in columns:
            if column not in imputed:
                self.last_observed[room][column] = (float(numbers[column]), timestamp)
        return clean, None


class StreamProcessor:
    def __init__(self):
        self.windows = defaultdict(deque)
        self.total_by_room = defaultdict(int)

    def update(self, record):
        room = record["room"]
        timestamp = pd.Timestamp(record["timestamp"])
        window = self.windows[room]
        cutoff = timestamp - pd.Timedelta(seconds=WINDOW_SECONDS)
        while window and pd.Timestamp(window[0]["timestamp"]) <= cutoff:
            window.popleft()
        previous = window[-1] if window else None
        window.append(record)
        self.total_by_room[room] += 1
        frame = pd.DataFrame(window)
        frame.index = pd.to_datetime(frame["timestamp"], utc=True)
        mean_co2 = float(frame["co2_ppm"].rolling(
            f"{WINDOW_SECONDS}s", closed="right", min_periods=1
        ).mean().iloc[-1])
        co2 = frame["co2_ppm"].to_numpy(dtype=float)
        jump = 0.0
        if (previous is not None
                and "co2_ppm" not in previous["imputed_fields"]
                and "co2_ppm" not in record["imputed_fields"]):
            jump = record["co2_ppm"] - previous["co2_ppm"]
        flags = []
        if len(window) >= MIN_CO2_SAMPLES and mean_co2 > CO2_LIMIT:
            flags.append("CO2_HIGH")
        if record["temperature_c"] > TEMP_LIMIT:
            flags.append("TEMP_HIGH")
        if jump > JUMP_LIMIT:
            flags.append("CO2_JUMP")
        return {
            **record,
            "co2_mean_60s": round(mean_co2, 2),
            "co2_std_60s": round(float(np.std(co2, ddof=0)), 2),
            "temperature_max_60s": round(float(np.max(frame["temperature_c"])), 2),
            "event_count_60s": len(window),
            "occupancy_pct": round(record["occupancy"] / CAPACITY * 100, 2),
            "total_room_events": self.total_by_room[room],
            "co2_change_ppm": round(jump, 2),
            "alert": bool(flags),
            "flags": ";".join(flags),
        }
