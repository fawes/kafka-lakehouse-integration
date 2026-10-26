"""
Event Sourcing, CQRS State Projector (Replay from Offset 0),
and Real-Time DuckDB OLAP Lakehouse Sink.
"""

import json
from pathlib import Path
import duckdb
import pandas as pd
from rich.console import Console
from rich.table import Table
from broker_and_contracts import LocalKafkaBroker
from stream_window_processor import (
    seed_stream_with_late_satellite_events,
    consume_and_compute_tumbling_windows,
)

console = Console()


def produce_and_replay_lifecycle_events(
    broker: LocalKafkaBroker,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Appends immutable domain lifecycle events and reconstructs current asset state
    from Offset 0 using a deterministic fold reducer (zero SQL UPDATEs).
    """
    es_topic = "uk.energy.asset.lifecycle.events"
    broker.create_topic(es_topic, num_partitions=1)

    lifecycle_events = [
        ("2026-10-26T06:00:00Z", "UK-WIND-01", "ASSET_ONLINE", {"mode": "GENERATING", "mw": 95, "sap_lock": False}),
        ("2026-10-26T06:00:00Z", "NS-PLAT-02", "ASSET_ONLINE", {"mode": "OPERATIONAL", "mw": 0, "sap_lock": False}),
        ("2026-10-26T12:10:00Z", "NS-PLAT-02", "WAVE_HAZARD_DETECTED", {"mode": "RESTRICTED", "wave_m": 5.6}),
        ("2026-10-26T12:11:00Z", "NS-PLAT-02", "SAP_WORK_ORDER_LOCKED", {"mode": "LOCKED_DOWN", "sap_lock": True, "sap_ref": "SAP-PM-99481"}),
        ("2026-10-26T12:15:00Z", "UK-WIND-01", "HIGH_WIND_RAMP", {"mode": "PEAK_LOAD", "mw": 118}),
        ("2026-10-26T12:20:00Z", "UK-WIND-01", "BEARING_TEMP_SPIKE", {"mode": "DERATED", "mw": 60}),
        ("2026-10-26T12:25:00Z", "UK-WIND-01", "BLADE_PITCH_FEATHERED", {"mode": "SAFE_HALT", "mw": 0, "sap_lock": True, "sap_ref": "SAP-PM-99482"}),
    ]

    for ts, key, evt_type, meta in lifecycle_events:
        broker.produce(es_topic, key, {"event_type": evt_type, "metadata": meta}, ts)

    # Consume from Offset 0 and fold into current state
    group_id = "cg_cqrs_state_projector"
    msgs = broker.topics[es_topic][0]
    broker.consumer_offsets.setdefault(group_id, {})[f"{es_topic}-0"] = len(msgs)
    (broker.base_dir / "__consumer_offsets.json").write_text(
        json.dumps(broker.consumer_offsets, indent=2)
    )

    audit_rows = []
    state_map: dict[str, dict] = {}

    for m in msgs:
        aid = m["key"]
        etype = m["payload"]["event_type"]
        meta = m["payload"]["metadata"]

        audit_rows.append({
            "offset": m["offset"],
            "event_time": m["event_time"],
            "asset_id": aid,
            "event_type": etype,
            "metadata": json.dumps(meta),
        })

        st = state_map.setdefault(aid, {
            "asset_id": aid,
            "current_mode": "INIT",
            "output_mw": 0,
            "sap_locked": False,
            "sap_order_ref": "NONE",
            "version": 0,
            "state_lineage": [],
        })
        if "mode" in meta:
            st["current_mode"] = meta["mode"]
            st["state_lineage"].append(meta["mode"])
        if "mw" in meta:
            st["output_mw"] = meta["mw"]
        if "sap_lock" in meta:
            st["sap_locked"] = meta["sap_lock"]
        if "sap_ref" in meta:
            st["sap_order_ref"] = meta["sap_ref"]
        st["version"] += 1

    cqrs_rows = [
        {
            "asset_id": v["asset_id"],
            "version": v["version"],
            "current_mode": v["current_mode"],
            "output_mw": v["output_mw"],
            "sap_locked": v["sap_locked"],
            "sap_order_ref": v["sap_order_ref"],
            "audit_lineage": " -> ".join(v["state_lineage"]),
        }
        for v in state_map.values()
    ]
    return pd.DataFrame(audit_rows), pd.DataFrame(cqrs_rows)


def sink_to_duckdb_lakehouse(
    db_path: Path, win_df: pd.DataFrame, audit_df: pd.DataFrame, cqrs_df: pd.DataFrame
):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))

    con.execute("CREATE OR REPLACE TABLE rt_tumbling_window_metrics AS SELECT * FROM win_df")
    con.execute("CREATE OR REPLACE TABLE es_immutable_audit_log AS SELECT * FROM audit_df")
    con.execute("CREATE OR REPLACE TABLE cqrs_current_asset_state AS SELECT * FROM cqrs_df")

    res_cqrs = con.execute("SELECT * FROM cqrs_current_asset_state ORDER BY asset_id").df()
    con.close()
    return res_cqrs


if __name__ == "__main__":
    broker = LocalKafkaBroker(Path("data/broker_logs"))
    raw_topic, _ = seed_stream_with_late_satellite_events(broker)
    win_df = consume_and_compute_tumbling_windows(broker, "cg_tumbling_window_engine", raw_topic)

    audit_df, cqrs_df = produce_and_replay_lifecycle_events(broker)
    db_path = Path("data/lakehouse/streaming_olap.duckdb")
    queried_cqrs = sink_to_duckdb_lakehouse(db_path, win_df, audit_df, cqrs_df)

    t1 = Table(title="Step 4A: Event Sourcing Immutable Journal (Replayed from Offset 0)")
    for c in audit_df.columns:
        t1.add_column(c)
    for _, r in audit_df.iterrows():
        t1.add_row(*[str(x) for x in r.values])
    console.print(t1)

    t2 = Table(title="Step 4B: CQRS Materialized State in DuckDB (Zero SQL UPDATEs)")
    for c in queried_cqrs.columns:
        t2.add_column(c)
    for _, r in queried_cqrs.iterrows():
        t2.add_row(*[str(x) for x in r.values])
    console.print(t2)
    console.print(f"\n[bold green]DuckDB OLAP Lakehouse persisted at:[/bold green] {db_path}")