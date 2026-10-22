import json
import pandas as pd
import sys
from pathlib import Path
from network.simulator import NetworkSimulator
from replicas.replica import Replica
from storage.local_store import load_replica, save_replica
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from network.simulator import NetworkSimulator


REPLICA_IDS = ["r1", "r2", "r3", "r4", "r5"]
STORAGE_DIR = ROOT / "data" / "replicas"

st.set_page_config(
    page_title="PS-39 | Replicated Store",
    page_icon="🔄",
    layout="wide",
)

st.title("PS-39: Offline-First Replicated Store")
st.caption(
    "Explore CRDTs, network partitions, message delivery, "
    "and replica convergence."
)


def reset_network(mode: str) -> None:
    st.session_state.network = NetworkSimulator(
        REPLICA_IDS,
        sync_mode=mode,
    )
    st.session_state.active_mode = mode
    st.session_state.last_action = "Simulation reset."


with st.sidebar:
    st.header("Simulation controls")

    mode = st.selectbox(
        "Synchronisation mode",
        ["full", "delta"],
        format_func=lambda value: (
            "Full-state synchronisation"
            if value == "full"
            else "Delta synchronisation"
        ),
    )

    if (
        "network" not in st.session_state
        or st.session_state.get("active_mode") != mode
    ):
        reset_network(mode)

    if st.button("Reset simulation", width="stretch"):
        reset_network(mode)

network = st.session_state.network

st.sidebar.divider()
st.sidebar.subheader("Make a local change")

replica_id = st.sidebar.selectbox(
    "Choose replica",
    REPLICA_IDS,
)

change_type = st.sidebar.selectbox(
    "Data type",
    [
        "Increment counter",
        "Add task",
        "Remove task",
        "Set setting",
        "Delete setting",
    ],
)

if change_type == "Increment counter":
    amount = st.sidebar.number_input(
        "Increment amount",
        min_value=1,
        max_value=1000,
        value=1,
    )

    if st.sidebar.button("Apply increment", width="stretch"):
        network.replicas[replica_id].counter.increment(int(amount))
        st.session_state.last_action = (
            f"{replica_id} incremented its counter by {amount}."
        )

elif change_type in ("Add task", "Remove task"):
    task = st.sidebar.text_input("Task name", value="Emergency supplies")

    if st.sidebar.button("Apply task change", width="stretch"):
        if not task.strip():
            st.sidebar.error("Enter a task name.")
        else:
            target = network.replicas[replica_id].task_set

            if change_type == "Add task":
                target.add(task)
            else:
                target.remove(task)

            st.session_state.last_action = (
                f"{replica_id}: {change_type.lower()} — {task}"
            )

else:
    key = st.sidebar.text_input("Setting key", value="theme")

    if change_type == "Set setting":
        value = st.sidebar.text_input("Setting value", value="dark")

    if st.sidebar.button("Apply setting change", width="stretch"):
        if not key.strip():
            st.sidebar.error("Enter a setting key.")
        elif change_type == "Set setting":
            network.replicas[replica_id].settings.put(key, value)
            st.session_state.last_action = (
                f"{replica_id} set {key} = {value}."
            )
        else:
            network.replicas[replica_id].settings.remove(key)
            st.session_state.last_action = (
                f"{replica_id} deleted setting '{key}'."
            )


st.divider()
st.subheader("Local replica persistence")
st.caption(
    "Save a replica's local state to disk and restore it later. "
    "After restoring, synchronise again to exchange updates."
)

save_col, restore_col = st.columns(2)

with save_col:
    if st.button("Save selected replica", width="stretch"):
        try:
            saved_path = save_replica(
                network.replicas[replica_id],
                STORAGE_DIR,
            )
            st.session_state.last_action = (
                f"Saved {replica_id} to {saved_path}."
            )
            st.success(f"Saved local state for {replica_id}.")
        except (OSError, ValueError) as error:
            st.error(f"Could not save replica: {error}")

with restore_col:
    if st.button("Restore selected replica", width="stretch"):
        try:
            restored = load_replica(replica_id, STORAGE_DIR)
            network.replicas[replica_id] = restored

            # In delta mode, force fresh synchronisation knowledge
            # for every connection involving this restored replica.
            if network.sync_mode == "delta":
<<<<<<< HEAD
                for sender in REPLICA_IDS:
                    for receiver in REPLICA_IDS:
                        if (
                            sender != receiver
                            and (
                                sender == replica_id
                                or receiver == replica_id
                            )
                        ):
                            known_id = f"__known__{sender}__{receiver}"
                            network.peer_known_states[
                                (sender, receiver)
                            ] = Replica(known_id)
=======
                network.reset_peer_knowledge(replica_id)
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256

            st.session_state.last_action = (
                f"Restored {replica_id}. "
                "Synchronise to exchange updates."
            )
            st.success(
                f"Restored {replica_id}. "
                "Now synchronise the network."
            )

        except FileNotFoundError:
            st.warning(
                f"No saved state exists for {replica_id}. "
                "Save it first."
            )
        except (OSError, ValueError) as error:
            st.error(f"Could not restore replica: {error}")

st.subheader("Network operations")

col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("Broadcast round", width="stretch"):
        count = network.broadcast_round()
        st.session_state.last_action = (
            f"Queued {count} message(s)."
        )

with col2:
    if st.button("Deliver all reachable", width="stretch"):
        count = network.deliver_all(
            random_order=True,
            seed=42,
        )
        st.session_state.last_action = (
            f"Delivered {count} message(s)."
        )

with col3:
    if st.button("Split network", width="stretch"):
        network.set_partitions([
            ["r1", "r2"],
            ["r3", "r4", "r5"],
        ])
        st.session_state.last_action = (
            "Network partitioned into groups of two and three."
        )

with col4:
    if st.button("Heal network", width="stretch"):
        network.heal_partitions()
        st.session_state.last_action = (
            "All network links re-enabled. Broadcast to synchronise."
        )


col1, col2 = st.columns(2)

with col1:
    if st.button("Duplicate first queued message", width="stretch"):
        if network.queue:
            network.duplicate_message(0)
            st.session_state.last_action = (
                "Duplicated the first queued message."
            )
        else:
            st.session_state.last_action = "No queued message to duplicate."

with col2:
    if st.button("Broadcast + deliver", width="stretch"):
        queued = network.broadcast_round()
        delivered = network.deliver_all(
            random_order=True,
            seed=42,
        )
        st.session_state.last_action = (
            f"Queued {queued} message(s); delivered {delivered}."
        )


st.info(st.session_state.get("last_action", "Simulation ready."))

converged = network.all_replicas_converged()

<<<<<<< HEAD
m1, m2, m3, m4 = st.columns(4)
=======
m1, m2, m3, m4, m5 = st.columns(5)
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256

m1.metric("Replicas", len(network.replicas))
m2.metric("Converged", "Yes" if converged else "No")
m3.metric("Queued messages", len(network.queue))
m4.metric("Payload bytes sent", network.bytes_transmitted)
<<<<<<< HEAD
=======
m5.metric("Pending deltas", network.pending_delta_count() if network.sync_mode == "delta" else "N/A")
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256

st.subheader("Replica states")

columns = st.columns(5)

for column, rid in zip(columns, REPLICA_IDS):
    replica = network.replicas[rid]

    with column:
        st.markdown(f"### {rid}")
        st.caption(f"Network group: {network._groups[rid]}")
        st.metric("Counter", replica.counter.value)

        with st.expander("Tasks"):
            st.write(replica.task_set.ordered_values)

        with st.expander("Settings"):
            st.json(replica.settings.items)

        with st.expander("Full state"):
            st.json(replica.snapshot())


st.divider()
st.subheader("Conflict inspector")
st.caption(
    "Compare setting versions across replicas before and after "
    "synchronisation. LWW-Map resolves a key using "
    "(logical timestamp, writer replica ID)."
)

entries_by_key = {}

for rid, replica in network.replicas.items():
    for key, entry in replica.settings.state["entries"].items():
        entries_by_key.setdefault(key, []).append({
            "Current replica": rid,
            "Value": (
                "<DELETED>"
                if entry["deleted"]
                else entry["value"]
            ),
            "Logical timestamp": entry["timestamp"],
            "Writer replica": entry["replica_id"],
            "Deleted": entry["deleted"],
        })

divergent_keys = {}

for key, candidates in entries_by_key.items():
    distinct_versions = {
        (
            row["Logical timestamp"],
            row["Writer replica"],
            row["Value"],
            row["Deleted"],
        )
        for row in candidates
    }

    if len(distinct_versions) > 1:
        winner = max(
            candidates,
            key=lambda row: (
                row["Logical timestamp"],
                row["Writer replica"],
            ),
        )
        divergent_keys[key] = {
            "candidates": candidates,
            "winner": winner,
        }

if not divergent_keys:
    st.success(
        "No divergent setting versions detected across replicas."
    )
else:
    st.warning(
        f"{len(divergent_keys)} setting key(s) have divergent versions."
    )

    for key, details in divergent_keys.items():
        winner = details["winner"]

        with st.expander(f"Setting: {key}", expanded=True):
            st.dataframe(
                details["candidates"],
                width="stretch",
                hide_index=True,
            )

            st.success(
                f"Expected LWW winner: {winner['Value']} — "
                f"timestamp {winner['Logical timestamp']}, "
                f"writer {winner['Writer replica']}."
            )

            st.caption(
                "The winner is calculated from the currently observed "
                "entries. Replicas must still exchange updates before "
                "their stored states actually converge."
            )


st.subheader("Message queue")

if network.queue:
    queue_rows = [
        {
            "Index": index,
<<<<<<< HEAD
            "Sender": message.sender,
            "Receiver": message.receiver,
=======
            "Type": message.msg_type.upper(),
            "Sender": message.sender,
            "Receiver": message.receiver,
            "ID / ACK-For": message.ack_for if message.msg_type == "ack" else message.msg_id[:18],
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
            "Payload bytes": message.size_bytes,
            "Link available": (
                network._groups[message.sender]
                == network._groups[message.receiver]
            ),
        }
        for index, message in enumerate(network.queue)
    ]

    st.dataframe(
        queue_rows,
        width="stretch",
        hide_index=True,
    )

    message_index = st.number_input(
        "Message index to deliver",
        min_value=0,
        max_value=len(network.queue) - 1,
        value=0,
        step=1,
    )

<<<<<<< HEAD
    if st.button("Deliver selected message"):
        delivered = network.deliver(int(message_index))

        st.session_state.last_action = (
            "Message delivered successfully."
            if delivered
            else "Delivery blocked by the network partition."
        )
=======
    del_col1, del_col2, del_col3 = st.columns(3)
    with del_col1:
        if st.button("Deliver selected message", width="stretch"):
            delivered = network.deliver(int(message_index))
            st.session_state.last_action = (
                "Message delivered successfully."
                if delivered
                else "Delivery blocked by the network partition."
            )
    with del_col2:
        if st.button("Drop selected message", width="stretch"):
            dropped = network.drop_message(int(message_index))
            st.session_state.last_action = (
                f"Dropped {dropped.msg_type.upper()} message at index {message_index}."
            )
    with del_col3:
        if st.button("Drop first queued ACK", width="stretch"):
            dropped = network.drop_first_ack()
            st.session_state.last_action = (
                f"Dropped ACK for message '{dropped.ack_for}'."
                if dropped
                else "No ACK message currently waiting in the queue."
            )
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
else:
    st.write("No messages are waiting in the queue.")

st.subheader("Merge performance")

metrics = network.metrics

<<<<<<< HEAD
c1, c2, c3 = st.columns(3)

c1.metric("Messages delivered", metrics["messages_delivered"])
c2.metric("Average merge latency (ms)", f"{metrics['average_merge_latency_ms']:.6f}")
c3.metric("Payload bytes transmitted", network.bytes_transmitted)
=======
c1, c2, c3, c4 = st.columns(4)

c1.metric("Data delivered", metrics["messages_delivered"])
c2.metric("ACKs delivered", metrics["ack_messages_delivered"])
c3.metric("Avg merge latency (ms)", f"{metrics['average_merge_latency_ms']:.6f}")
c4.metric("Data bytes sent", network.bytes_transmitted)

if network.sync_mode == "delta":
    st.caption(
        f"ACK traffic: {metrics['ack_messages_sent']} sent, "
        f"{metrics['ack_messages_delivered']} delivered, "
        f"{metrics['ack_bytes_transmitted']} bytes sent, "
        f"{metrics['ack_bytes_delivered']} bytes delivered."
    )
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256

st.caption(
    "This is a simulator: payload counts cover serialised message data, "
    "and merge latency measures local processing rather than network delay."
)

st.divider()
st.header("Benchmark comparison")
st.caption(
    "Compare full-state and delta synchronisation using saved "
    "results from the same randomised experiment settings."
)

benchmark_path = ROOT / "benchmarks" / "comparison_results.json"

if not benchmark_path.exists():
    st.warning(
        "Benchmark results were not found. Run "
        "'python benchmarks/run_benchmarks.py' first."
    )
else:
    try:
        with benchmark_path.open("r", encoding="utf-8") as file:
            report = json.load(file)

        full = report["full_state"]
        delta = report["delta"]

        full_bytes = full["average_payload_bytes_per_trial"]
        delta_bytes = delta["average_payload_bytes_per_trial"]

        reduction = (
            (full_bytes - delta_bytes) / full_bytes * 100
            if full_bytes > 0
            else 0.0
        )

        c1, c2, c3 = st.columns(3)

        c1.metric(
            "Payload reduction",
            f"{reduction:.2f}%",
            help="Reduction in average serialised payload bytes per trial.",
        )

        c2.metric(
            "Full-state convergence",
            f"{full['convergence_rate_percent']:.1f}%",
        )

        c3.metric(
            "Delta convergence",
            f"{delta['convergence_rate_percent']:.1f}%",
        )

        comparison_rows = [
            {
                "Metric": "Trials",
                "Full-state": full["trials"],
                "Delta": delta["trials"],
            },
            {
                "Metric": "Converged trials",
                "Full-state": full["converged_trials"],
                "Delta": delta["converged_trials"],
            },
            {
                "Metric": "Messages per trial",
                "Full-state": full["average_messages_per_trial"],
                "Delta": delta["average_messages_per_trial"],
            },
            {
                "Metric": "Payload bytes sent per trial",
                "Full-state": full_bytes,
                "Delta": delta_bytes,
            },
            {
                "Metric": "Payload bytes delivered per trial",
                "Full-state": full["average_payload_bytes_delivered_per_trial"],
                "Delta": delta["average_payload_bytes_delivered_per_trial"],
            },
            {
                "Metric": "Average merge latency (ms)",
                "Full-state": full["average_merge_latency_ms"],
                "Delta": delta["average_merge_latency_ms"],
            },
            {
                "Metric": "P95 merge latency (ms)",
                "Full-state": full["p95_merge_latency_ms"],
                "Delta": delta["p95_merge_latency_ms"],
            },
        ]

        st.subheader("Measured results")
        st.dataframe(
            comparison_rows,
            width="stretch",
            hide_index=True,
        )

        chart_data = pd.DataFrame({
            "Mode": ["Full-state", "Delta"],
            "Payload bytes sent": [full_bytes, delta_bytes],
        })

        st.subheader("Synchronisation payload")
        st.bar_chart(
            chart_data,
            x="Mode",
            y="Payload bytes sent",
        )

        st.caption(
            "Payload bytes measure serialised message data, not complete "
            "real-network overhead. Merge latency measures local processing, "
            "not end-to-end synchronisation time. Results apply only to "
            "the tested scenarios."
        )

    except (OSError, ValueError, KeyError, TypeError) as error:
        st.error(f"Unable to load benchmark results: {error}")


# ---------------------------------------------------------
# Steady-state incremental-update benchmark
# ---------------------------------------------------------
st.divider()
st.subheader("Steady-State Incremental-Update Benchmark")

st.caption(
    "Five replicas; one new task added after initial synchronisation. "
    "Compares full-state and delta synchronisation."
)

benchmark_path = ROOT / "benchmarks" / "incremental_update_results.json"

if not benchmark_path.exists():
    st.warning(
        "Benchmark results not found. Run "
        "benchmarks/run_incremental_update_benchmark.py first."
    )
else:
    try:
        with benchmark_path.open("r", encoding="utf-8") as file:
            benchmark_report = json.load(file)

        benchmark_df = pd.DataFrame(benchmark_report.get("summary", []))

        if benchmark_df.empty:
            st.warning("The benchmark report contains no summary results.")
        else:
            display_df = benchmark_df[
                [
                    "item_count",
                    "mode",
                    "trials",
                    "convergence_rate_percent",
                    "average_messages_sent",
                    "average_payload_bytes_transmitted",
                    "average_merge_latency_ms",
                    "p95_trial_average_merge_latency_ms",
                ]
            ].rename(
                columns={
                    "item_count": "Baseline items",
                    "mode": "Sync mode",
                    "trials": "Trials",
                    "convergence_rate_percent": "Convergence (%)",
                    "average_messages_sent": "Messages/update",
                    "average_payload_bytes_transmitted": "Payload bytes/update",
                    "average_merge_latency_ms": "Mean merge latency (ms)",
                    "p95_trial_average_merge_latency_ms":
                        "P95 trial-average latency (ms)",
                }
            )

            st.markdown("#### Benchmark results")
            st.dataframe(
                display_df,
                width="stretch",
                hide_index=True,
            )

            st.markdown("#### Payload bytes transmitted per update")
            payload_chart = benchmark_df.pivot(
                index="item_count",
                columns="mode",
                values="average_payload_bytes_transmitted",
            )
            payload_chart.index.name = "Baseline items"
            st.bar_chart(payload_chart, width="stretch")

            st.markdown("#### Messages sent per update")
            messages_chart = benchmark_df.pivot(
                index="item_count",
                columns="mode",
                values="average_messages_sent",
            )
            messages_chart.index.name = "Baseline items"
            st.bar_chart(messages_chart, width="stretch")

            st.markdown("#### Mean merge latency")
            latency_chart = benchmark_df.pivot(
                index="item_count",
                columns="mode",
                values="average_merge_latency_ms",
            )
            latency_chart.index.name = "Baseline items"
            st.bar_chart(latency_chart, width="stretch")

            reductions = benchmark_report.get(
                "payload_reduction_percent", {}
            )
            reduction_df = pd.DataFrame(
                [
                    {
                        "Baseline items": int(size),
                        "Delta payload reduction (%)": float(percent),
                    }
                    for size, percent in reductions.items()
                ]
            ).sort_values("Baseline items")

            st.markdown("#### Bandwidth saved by delta synchronisation")
            st.dataframe(
                reduction_df,
                width="stretch",
                hide_index=True,
            )

    except (OSError, json.JSONDecodeError, KeyError, ValueError) as error:
        st.error(f"Could not load benchmark results: {error}")
