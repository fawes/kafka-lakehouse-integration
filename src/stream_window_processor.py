"""
Stateful Stream Processing — 15-Minute Event-Time Tumbling Windows,
Out-of-Order Satellite Watermarks, and Consumer Group Offset Commits.
"""

from datetime import datetime, timedelta
import json
from pathlib import Path
import pandas as pd
from rich.console import Console
from rich.table import Table
from broker_and_contracts import LocalKafkaBroker

console = Console()


def seed_stream_with_late_satellite_events(broker: LocalKafkaBroker):
    raw_topic = "uk.energy.telemetry.raw"
    dlq_topic = "uk.energy.telemetry.dlq"
    broker.create_topic(raw_topic, num_partitions=3)
    broker.create_topic(dlq_topic, num_partitions=1)

    events = [
        # Window 12:00 - 12:15 UTC
        ("UK-WIND-01", {"asset_id": "UK-WIND-01", "asset_name": "Dogger Bank", "wind_speed_ms": 17.5, "swh_wave_m": 4.4, "bearing_temp_c": 63.0}, "2026-10-26T12:00:00Z", False),
        ("NS-PLAT-02", {"asset_id": "NS-PLAT-02", "asset_name": "Forties Rig", "wind_speed_ms": 21.0, "swh_wave_m": 4.9, "bearing_temp_c": 70.0}, "2026-10-26T12:05:00Z", False),
        # Window 12:15 - 12:30 UTC
        ("UK-WIND-01", {"asset_id": "UK-WIND-01", "asset_name": "Dogger Bank", "wind_speed_ms": 19.2, "swh_wave_m": 4.8, "bearing_temp_c": 66.5}, "2026-10-26T12:15:00Z", False),
        ("NS-PLAT-02", {"asset_id": "NS-PLAT-02", "asset_name": "Forties Rig", "wind_speed_ms": 23.4, "swh_wave_m": 5.2, "bearing_temp_c": 74.0}, "2026-10-26T12:20:00Z", False),
        # LATE SATELLITE PACKET: Measured at 12:10 UTC (5.6m wave breach!), but arrived after 12:20 UTC
        ("NS-PLAT-02", {"asset_id": "NS-PLAT-02", "asset_name": "Forties Rig", "wind_speed_ms": 24.8, "swh_wave_m": 5.6, "bearing_temp_c": 75.5}, "2026-10-26T12:10:00Z", True),
        # Corrupted spike -> Routed to DLQ
        ("UK-WIND-01", {"asset_id": "UK-WIND-01", "asset_name": "Dogger Bank", "wind_speed_ms": -50.0, "swh_wave_m": 4.5, "bearing_temp_c": 65.0}, "2026-10-26T12:25:00Z", False),
    ]

    for key, payload, event_ts, is_late in events:
        env, is_valid = broker.produce_validated_telemetry(raw_topic, dlq_topic, key, payload, event_ts)
        if is_valid:
            env["payload"]["late_satellite_packet"] = is_late

    return raw_topic, dlq_topic


def consume_and_compute_tumbling_windows(broker: LocalKafkaBroker, group_id: str, topic: str) -> pd.DataFrame:
    """Consumes partitions, commits Consumer Group offsets, and buckets by 15-min Event Time."""
    records = []
    broker.consumer_offsets.setdefault(group_id, {})

    for part_id, msgs in broker.topics[topic].items():
        records.extend(msgs)
        broker.consumer_offsets[group_id][f"{topic}-{part_id}"] = len(msgs)

    # Persist Consumer Group Offset Checkpoint (__consumer_offsets.json)
    offsets_path = broker.base_dir / "__consumer_offsets.json"
    offsets_path.write_text(json.dumps(broker.consumer_offsets, indent=2))

    rows = []
    for msg in records:
        dt = datetime.strptime(msg["event_time"], "%Y-%m-%dT%H:%M:%SZ")
        win_min = (dt.minute // 15) * 15
        win_start = dt.replace(minute=win_min, second=0)
        win_end = win_start + timedelta(minutes=15)

        p = msg["payload"]
        rows.append({
            "window_start": win_start.strftime("%H:%M"),
            "window_end": win_end.strftime("%H:%M"),
            "asset_id": p["asset_id"],
            "partition": msg["partition"],
            "wind_speed_ms": p["wind_speed_ms"],
            "swh_wave_m": p["swh_wave_m"],
            "bearing_temp_c": p["bearing_temp_c"],
            "late_packet": int(p.get("late_satellite_packet", False)),
        })

    df = pd.DataFrame(rows)
    agg = (
        df.groupby(["window_start", "window_end", "asset_id", "partition"], as_index=False)
        .agg(
            events=("wind_speed_ms", "count"),
            late_reconciled=("late_packet", "sum"),
            avg_wind_ms=("wind_speed_ms", "mean"),
            max_wave_m=("swh_wave_m", "max"),
            max_temp_c=("bearing_temp_c", "max"),
        )
        .round(2)
    )
    agg["operational_action"] = agg["max_wave_m"].apply(
        lambda w: "HALT_SAP_HELIDECK (>=5.0m)" if w >= 5.0 else "NORMAL_OPS"
    )
    return agg


if __name__ == "__main__":
    broker = LocalKafkaBroker(Path("data/broker_logs"))
    raw_topic, _ = seed_stream_with_late_satellite_events(broker)
    win_df = consume_and_compute_tumbling_windows(broker, "cg_tumbling_window_engine", raw_topic)

    table = Table(title="Step 3: 15-Minute Event-Time Tumbling Windows (With Late Satellite Watermark)")
    for col in win_df.columns:
        table.add_column(col)

    for _, r in win_df.iterrows():
        table.add_row(*[str(val) for val in r.values])

    console.print(table)
    console.print(f"\n[bold green]Committed Consumer Group Offsets:[/bold green] {broker.consumer_offsets}")