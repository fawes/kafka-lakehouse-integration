"""
Automated Pytest Verification & Architecture Visual Generator.
"""

import sys
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from broker_and_contracts import LocalKafkaBroker, hash_key_to_partition
from stream_window_processor import (
    seed_stream_with_late_satellite_events,
    consume_and_compute_tumbling_windows,
)
from event_sourcing_and_lakehouse import (
    produce_and_replay_lifecycle_events,
    sink_to_duckdb_lakehouse,
)


def test_deterministic_partition_routing():
    """Same asset_id must always route to the exact same partition."""
    p1 = hash_key_to_partition("NS-PLAT-02", 3)
    p2 = hash_key_to_partition("NS-PLAT-02", 3)
    assert p1 == p2


def test_end_to_end_dlq_windowing_and_cqrs(tmp_path: Path):
    broker = LocalKafkaBroker(tmp_path / "broker_logs")
    raw_topic, dlq_topic = seed_stream_with_late_satellite_events(broker)

    # 1. Verify corrupted wind spike (-50.0 m/s) was quarantined in DLQ
    dlq_msgs = broker.topics[dlq_topic][0]
    assert len(dlq_msgs) == 1
    assert dlq_msgs[0]["key"] == "UK-WIND-01"

    # 2. Verify late 12:10 UTC satellite packet (5.6m wave) reconciled into 12:00-12:15 window
    win_df = consume_and_compute_tumbling_windows(broker, "cg_test", raw_topic)
    ns_w1 = win_df[(win_df["asset_id"] == "NS-PLAT-02") & (win_df["window_start"] == "12:00")].iloc[0]
    assert ns_w1["late_reconciled"] == 1
    assert ns_w1["max_wave_m"] == 5.6
    assert "HALT_SAP_HELIDECK" in ns_w1["operational_action"]

    # 3. Verify Event Sourcing & DuckDB CQRS state fold
    audit_df, cqrs_df = produce_and_replay_lifecycle_events(broker)
    db_path = tmp_path / "test_olap.duckdb"
    queried = sink_to_duckdb_lakehouse(db_path, win_df, audit_df, cqrs_df)

    wind_state = queried[queried["asset_id"] == "UK-WIND-01"].iloc[0]
    assert wind_state["current_mode"] == "SAFE_HALT"
    assert bool(wind_state["sap_locked"]) is True
    assert wind_state["sap_order_ref"] == "SAP-PM-99482"


def generate_architecture_diagram():
    out_path = Path("data/output/kafka_lakehouse_architecture.png")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
    ax.set_title("Pillar 1: Event-Driven Kafka Commit Log, DLQ, Tumbling Windows & CQRS Lakehouse", fontsize=12, fontweight="bold")
    ax.axis("off")

    stages = [
        (0.05, 0.76, "#cfe2f3", "#0b5394", "1. PRODUCER & PYDANTIC SCHEMA GATEKEEPER",
         "• Validates physical bounds (0<=wind<=75 m/s, 0<=wave<=30 m) • Routes corrupted spikes to uk.energy.telemetry.dlq"),
        (0.05, 0.53, "#fff2cc", "#b45f06", "2. PARTITIONED COMMIT LOGS ON DISK (Murmur2 Key Hashing)",
         "• Appends immutable JSON records to 00000000.log + byte-offset .index files • Strict ordering per asset_id"),
        (0.05, 0.30, "#ead1dc", "#741b47", "3. STATEFUL STREAM PROCESSOR & EVENT SOURCING REPLAY",
         "• 15-Min Event-Time Tumbling Windows (reconciles late satellite packets) • Replays lifecycle events from Offset 0"),
        (0.05, 0.07, "#d9ead3", "#274e13", "4. REAL-TIME DUCKDB OLAP LAKEHOUSE SINK",
         "• Materializes rt_tumbling_window_metrics, es_immutable_audit_log, and cqrs_current_asset_state"),
    ]
    for x, y, fc, ec, title, desc in stages:
        ax.add_patch(FancyBboxPatch((x, y), 0.90, 0.17, boxstyle="round,pad=0.02", facecolor=fc, edgecolor=ec, lw=1.5))
        ax.text(x + 0.02, y + 0.10, title, fontsize=10, fontweight="bold", color=ec)
        ax.text(x + 0.02, y + 0.03, desc, fontsize=8.8)

    plt.tight_layout()
    plt.savefig(out_path)
    plt.close(fig)


if __name__ == "__main__":
    generate_architecture_diagram()

