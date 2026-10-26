"""
Kafka Commit-Log Broker Internals & Pydantic Data Contract Gatekeeper (DLQ).
"""

import hashlib
import json
from pathlib import Path
from pydantic import BaseModel, Field, ValidationError
from rich.console import Console
from rich.table import Table

console = Console()


# 1. DATA CONTRACT (Acts as your Schema Registry Gatekeeper)
class OffshoreTelemetryContract(BaseModel):
    asset_id: str = Field(..., min_length=3)
    asset_name: str
    wind_speed_ms: float = Field(..., ge=0.0, le=75.0, description="Physical wind limit 0..75 m/s")
    swh_wave_m: float = Field(..., ge=0.0, le=30.0, description="Significant wave height 0..30 m")
    bearing_temp_c: float = Field(..., ge=-30.0, le=150.0, description="Gearbox bearing temp C")


# 2. DETERMINISTIC PARTITION ROUTER (Kafka Murmur2 style)
def hash_key_to_partition(key: str, num_partitions: int) -> int:
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % num_partitions


# 3. PHYSICAL COMMIT-LOG BROKER ON DISK
class LocalKafkaBroker:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.topics: dict[str, dict[int, list[dict]]] = {}
        self.consumer_offsets: dict[str, dict[str, int]] = {}

    def create_topic(self, topic: str, num_partitions: int):
        self.topics[topic] = {p: [] for p in range(num_partitions)}
        for p in range(num_partitions):
            part_dir = self.base_dir / f"{topic}-{p}"
            part_dir.mkdir(parents=True, exist_ok=True)
            for ext in (".log", ".index"):
                f = part_dir / f"00000000000000000000{ext}"
                if f.exists():
                    f.unlink()

    def produce(self, topic: str, key: str, value: dict, event_time: str) -> dict:
        partitions = self.topics[topic]
        part_id = hash_key_to_partition(key, len(partitions))
        offset = len(partitions[part_id])

        envelope = {
            "topic": topic,
            "partition": part_id,
            "offset": offset,
            "key": key,
            "event_time": event_time,
            "payload": value,
        }
        partitions[part_id].append(envelope)

        part_dir = self.base_dir / f"{topic}-{part_id}"
        log_file = part_dir / "00000000000000000000.log"
        idx_file = part_dir / "00000000000000000000.index"

        raw_line = json.dumps(envelope) + "\n"
        with open(log_file, "a", encoding="utf-8") as lf:
            byte_pos = lf.tell()
            lf.write(raw_line)
        with open(idx_file, "a", encoding="utf-8") as ix:
            ix.write(f"offset={offset} | byte_pos={byte_pos} | key={key} | time={event_time}\n")

        return envelope

    def produce_validated_telemetry(
        self, raw_topic: str, dlq_topic: str, key: str, raw_payload: dict, event_time: str
    ):
        """Validates against Pydantic contract; routes failures to Dead-Letter Queue (DLQ)."""
        try:
            validated = OffshoreTelemetryContract(**raw_payload)
            return self.produce(raw_topic, key, validated.model_dump(), event_time), True
        except ValidationError as err:
            dlq_payload = {
                "failed_key": key,
                "error_summary": err.errors()[0]["msg"],
                "failed_field": str(err.errors()[0]["loc"]),
                "raw_payload": raw_payload,
            }
            return self.produce(dlq_topic, key, dlq_payload, event_time), False


if __name__ == "__main__":
    broker = LocalKafkaBroker(Path("data/broker_logs"))
    raw_topic = "uk.energy.telemetry.raw"
    dlq_topic = "uk.energy.telemetry.dlq"
    broker.create_topic(raw_topic, num_partitions=3)
    broker.create_topic(dlq_topic, num_partitions=1)

    sample_events = [
        ("UK-WIND-01", {"asset_id": "UK-WIND-01", "asset_name": "Dogger Bank", "wind_speed_ms": 18.4, "swh_wave_m": 4.7, "bearing_temp_c": 64.0}, "2026-10-26T12:00:00Z"),
        ("NS-PLAT-02", {"asset_id": "NS-PLAT-02", "asset_name": "Forties Rig", "wind_speed_ms": 22.1, "swh_wave_m": 5.3, "bearing_temp_c": 71.5}, "2026-10-26T12:00:00Z"),
        # Corrupted sensor spikes (should be caught by Pydantic and routed to DLQ!)
        ("NS-PLAT-02", {"asset_id": "NS-PLAT-02", "asset_name": "Forties Rig", "wind_speed_ms": 19.0, "swh_wave_m": -999.0, "bearing_temp_c": 70.0}, "2026-10-26T12:05:00Z"),
        ("UK-WIND-01", {"asset_id": "UK-WIND-01", "asset_name": "Dogger Bank", "wind_speed_ms": 450.0, "swh_wave_m": 4.8, "bearing_temp_c": 65.0}, "2026-10-26T12:05:00Z"),
    ]

    table = Table(title="Step 2: Broker Partition Routing & Pydantic DLQ Gatekeeper")
    table.add_column("Asset Key", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Target Topic", style="magenta")
    table.add_column("Partition", justify="center")
    table.add_column("Offset", justify="center")
    table.add_column("Details")

    for key, payload, ts in sample_events:
        env, is_valid = broker.produce_validated_telemetry(raw_topic, dlq_topic, key, payload, ts)
        status = "[green]VALID -> INGESTED[/green]" if is_valid else "[red]REJECTED -> DLQ[/red]"
        detail = f"wave={payload['swh_wave_m']}m, wind={payload['wind_speed_ms']}m/s" if is_valid else f"{env['payload']['failed_field']}: {env['payload']['error_summary']}"
        table.add_row(key, status, env["topic"], str(env["partition"]), str(env["offset"]), detail)

    console.print(table)