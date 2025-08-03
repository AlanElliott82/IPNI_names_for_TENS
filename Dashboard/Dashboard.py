import streamlit as st
import pandas as pd
import sqlite3
import altair as alt

# === CONFIG ===
DB_PATH = r"C:\Users\alane\OneDrive\Documents\wfo\GaultheriaQuery\Data\WFOcontent.db"

@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_PATH)
    snapshots = {
        "2022-12": "Dec2022",
        "2023-06": "June2023",
        "2023-12": "Dec2023",
        "2024-06": "June2024",
        "2024-12": "Dec2024",
        "2025-06": "June2025"
    }
    snap_dict = {}
    for label, table in snapshots.items():
        df = pd.read_sql(f"SELECT taxonID, scientificName, taxonomicStatus FROM {table}", conn)
        df["Snapshot"] = label
        snap_dict[label] = df
    conn.close()
    combined = pd.concat(snap_dict.values(), ignore_index=True)
    return combined, snap_dict, list(snapshots.keys())

# === LOAD + LIMIT DATA ===
data, snap_dict, snapshot_labels = load_data()

st.sidebar.title("⚙️ Dashboard Controls")

# Optional dev mode for performance
st.sidebar.header("🧪 Dev Mode")
max_rows = st.sidebar.slider("Limit rows for performance", 0, 50000, 10000, step=5000)
if max_rows > 0:
    data = data.head(max_rows)

# Display options
st.sidebar.header("📊 Display Sections")
show_status_chart = st.sidebar.checkbox("Trends Over Time (Line Chart)", True)
show_transitions = st.sidebar.checkbox("Status Transitions", True)
show_vanished = st.sidebar.checkbox("Vanished Names", True)
show_custom_compare = st.sidebar.checkbox("Custom Snapshot Comparison", True)

# Filters
st.sidebar.header("🔎 Filter Data")
available_statuses = sorted(data["taxonomicStatus"].dropna().unique())
status_filter = st.sidebar.multiselect("Taxonomic Status", available_statuses, default=available_statuses)
name_filter = st.sidebar.text_input("Scientific Name Contains").strip().lower()

# === TITLE ===
st.title("🧬 Taxonomic Trends Dashboard")
st.caption("Efficiently tracking names, statuses, transitions, and snapshot comparisons")

# === APPLY FILTERS ===
filtered_data = data[
    data["taxonomicStatus"].isin(status_filter) &
    data["scientificName"].str.lower().str.contains(name_filter if name_filter else "", na=False)
]

# === STATUS SUMMARY ===
st.subheader("📊 Taxonomic Status per Snapshot")
summary = filtered_data.groupby(["Snapshot", "taxonomicStatus"]).size().unstack(fill_value=0)
st.dataframe(summary)

# === KPI METRICS FOR NEW NAMES ===
st.subheader("📈 Newly Added Names")
for i in range(1, len(snapshot_labels)):
    prev_ids = set().union(*[set(snap_dict[snapshot_labels[j]]["taxonID"]) for j in range(i)])
    curr_snap = snapshot_labels[i]
    curr = snap_dict[curr_snap]
    new = curr[~curr["taxonID"].isin(prev_ids)]
    count = new["taxonomicStatus"].str.lower().value_counts()

    st.markdown(f"### 🕒 Snapshot: {curr_snap}")
    col1, col2, col3 = st.columns(3)
    col1.metric("New Names", len(new))
    col2.metric("✔️ Accepted", count.get("accepted", 0))
    col3.metric("🔁 Synonymised", count.get("synonym", 0))
    st.markdown("---")

# === LINE CHART ===
if show_status_chart:
    st.subheader("📉 Trends Over Time")
    trend_df = filtered_data.groupby(["Snapshot", "taxonomicStatus"])["taxonID"].count().reset_index()
    chart = alt.Chart(trend_df).mark_line(point=True).encode(
        x="Snapshot:N",
        y="taxonID:Q",
        color="taxonomicStatus:N",
        strokeDash="taxonomicStatus:N",
        tooltip=["Snapshot", "taxonomicStatus", "taxonID"]
    ).properties(width=700)
    st.altair_chart(chart, use_container_width=True)

# === TRANSITION DETECTION ===
if show_transitions:
    st.subheader("🔁 Taxonomic Status Transitions")
    pivot = filtered_data.pivot_table(index="taxonID", columns="Snapshot", values="taxonomicStatus", aggfunc="first")

    for i in range(len(snapshot_labels) - 1):
        prev, curr = snapshot_labels[i], snapshot_labels[i+1]
        label = f"{prev} → {curr}"

        if prev in pivot.columns and curr in pivot.columns:
            transition = pivot[[prev, curr]].dropna()
            transition["StatusChange"] = transition[curr].str.lower() + " ← " + transition[prev].str.lower()
            counts = transition["StatusChange"].value_counts()

            with st.expander(f"🔄 {label} — Status Changes"):
                st.dataframe(counts.rename("Count").to_frame())

# === VANISHED TAXA ===
if show_vanished:
    st.subheader("💨 Vanished Taxon Records")
    for i in range(len(snapshot_labels) - 1):
        from_snap, to_snap = snapshot_labels[i], snapshot_labels[i+1]
        from_ids = set(snap_dict[from_snap]["taxonID"])
        to_ids = set(snap_dict[to_snap]["taxonID"])
        vanished_ids = from_ids - to_ids

        if vanished_ids:
            vanished_df = snap_dict[from_snap][snap_dict[from_snap]["taxonID"].isin(vanished_ids)]
            with st.expander(f"❌ Vanished between {from_snap} → {to_snap} ({len(vanished_df)} records)"):
                st.dataframe(vanished_df[["taxonID", "scientificName", "taxonomicStatus"]])

# === CUSTOM COMPARISON ===
if show_custom_compare:
    st.subheader("🪄 Custom Snapshot Comparison")
    from_snap = st.selectbox("📍 Select 'From' Snapshot", snapshot_labels[:-1])
    to_snap_options = snapshot_labels[snapshot_labels.index(from_snap)+1:]
    to_snap = st.selectbox("🎯 Select 'To' Snapshot", to_snap_options)

    if from_snap == to_snap:
        st.warning("Please choose two different snapshots to compare.")
    else:
        st.markdown(f"### 🔁 Status Transitions: {from_snap} → {to_snap}")

        pivot_pair = filtered_data[filtered_data["Snapshot"].isin([from_snap, to_snap])]
        pivot_comp = pivot_pair.pivot_table(index="taxonID", columns="Snapshot", values="taxonomicStatus", aggfunc="first").dropna()

        if from_snap in pivot_comp.columns and to_snap in pivot_comp.columns:
            pivot_comp["Transition"] = pivot_comp[to_snap].str.lower() + " ← " + pivot_comp[from_snap].str.lower()
            trans_counts = pivot_comp["Transition"].value_counts()
            st.dataframe(trans_counts.rename("Count").to_frame())

        # Vanished & New
        ids_from = set(snap_dict[from_snap]["taxonID"])
        ids_to = set(snap_dict[to_snap]["taxonID"])

        st.markdown(f"### 💨 Vanished Names (in {from_snap} but not in {to_snap})")
        vanished_ids = ids_from - ids_to
        if vanished_ids:
            vanished_df = snap_dict[from_snap][snap_dict[from_snap]["taxonID"].isin(vanished_ids)]
            st.dataframe(vanished_df[["taxonID", "scientificName", "taxonomicStatus"]])
        else:
            st.success("No vanished names between selected snapshots.")

        st.markdown(f"### 🌱 New Names (in {to_snap} but not in {from_snap})")
        new_ids = ids_to - ids_from
        if new_ids:
            new_df = snap_dict[to_snap][snap_dict[to_snap]["taxonID"].isin(new_ids)]
            st.dataframe(new_df[["taxonID", "scientificName", "taxonomicStatus"]])
        else:
            st.info("No new names added in selected snapshot.")