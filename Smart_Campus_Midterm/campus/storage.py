import csv
import json

RAW_FIELDS = ["event_id", "timestamp", "room", "temperature_c", "co2_ppm", "occupancy"]
CLEAN_FIELDS = RAW_FIELDS + ["is_imputed", "imputed_fields", "normalized_decimal"]
METRIC_FIELDS = CLEAN_FIELDS + ["co2_mean_60s", "co2_std_60s", "temperature_max_60s",
                               "event_count_60s", "occupancy_pct", "total_room_events",
                               "co2_change_ppm", "alert", "flags"]


class ResultStore:
    def __init__(self, directory):
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)
        self.handles = []
        self.raw = self.open_text("raw_stream.jsonl")
        self.rejected = self.open_text("rejected.jsonl")
        self.writers = {}
        for name, fields in [("cleaned", CLEAN_FIELDS), ("metrics", METRIC_FIELDS),
                             ("alerts", METRIC_FIELDS)]:
            file = self.open_text(f"{name}.csv")
            writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            self.writers[name] = writer

    def open_text(self, name):
        file = (self.directory / name).open("w", encoding="utf-8", newline="")
        self.handles.append(file)
        return file

    def write_raw(self, record):
        self.raw.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")

    def reject(self, record, reason):
        self.rejected.write(json.dumps({"reason": reason, "record": record},
                                       ensure_ascii=False) + "\n")

    def accept(self, metrics):
        self.writers["cleaned"].writerow(metrics)
        self.writers["metrics"].writerow(metrics)
        if metrics["alert"]:
            self.writers["alerts"].writerow(metrics)

    def flush(self):
        for file in self.handles:
            file.flush()

    def close(self):
        for file in self.handles:
            file.close()
