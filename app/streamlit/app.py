import streamlit as st
import pandas as pd
from pymongo import MongoClient


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Citi Bike Analytics",
    page_icon="🚲",
    layout="wide"
)


# ============================================================
# MONGODB CONNECTION
# ============================================================

MONGO_URI = (
    "mongodb://admin:citibikemongo123"
    "@citibike-mongodb:27017/citibike"
    "?authSource=admin"
)


@st.cache_resource
def get_database():
    client = MongoClient(MONGO_URI)
    return client["citibike"]


db = get_database()


# ============================================================
# HISTORICAL DATA
# ============================================================

@st.cache_data
def load_user_type():
    data = list(
        db.historical_user_type_stats.find(
            {},
            {"_id": 0}
        )
    )

    return pd.DataFrame(data)


@st.cache_data
def load_hourly():
    data = list(
        db.historical_hourly_stats.find(
            {},
            {"_id": 0}
        )
    )

    return pd.DataFrame(data)


@st.cache_data
def load_stations():
    data = list(
        db.historical_station_stats.find(
            {},
            {"_id": 0}
        )
        .sort("total_rides", -1)
        .limit(20)
    )

    return pd.DataFrame(data)


user_type_df = load_user_type()
hourly_df = load_hourly()
station_df = load_stations()


# ============================================================
# PAGE TITLE
# ============================================================

st.title("Citi Bike NYC Analytics")
st.caption(
    "Historical Analysis — January 2026 | "
    "Real-Time Station Monitoring"
)


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.header("Historical Filter")

user_filter = st.sidebar.selectbox(
    "User Type",
    ["All", "member", "casual"]
)


# ============================================================
# HISTORICAL DASHBOARD
# ============================================================

st.header("Historical Analysis")

filtered_hourly = hourly_df.copy()

if user_filter != "All":
    filtered_hourly = filtered_hourly[
        filtered_hourly["member_casual"] == user_filter
    ]


total_rides = filtered_hourly["total_rides"].sum()

avg_duration = (
    filtered_hourly["avg_duration_minutes"].mean()
)


col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "Total Rides",
        f"{total_rides:,.0f}"
    )

with col2:
    st.metric(
        "Average Duration",
        f"{avg_duration:.2f} min"
    )

with col3:
    if not station_df.empty:
        st.metric(
            "Top Station",
            station_df.iloc[0]["start_station_name"]
        )
    else:
        st.metric(
            "Top Station",
            "-"
        )


# ============================================================
# HISTORICAL HOURLY CHART
# ============================================================

st.subheader("Ride Activity by Hour")

hourly_chart = (
    filtered_hourly
    .groupby("ride_hour")["total_rides"]
    .sum()
    .sort_index()
)

st.bar_chart(hourly_chart)


# ============================================================
# HISTORICAL TOP STATIONS
# ============================================================

st.subheader("Top 20 Start Stations")

station_display = station_df[
    [
        "start_station_name",
        "total_rides",
        "avg_duration_minutes"
    ]
].copy()

station_display.columns = [
    "Station",
    "Total Rides",
    "Avg Duration (min)"
]

st.dataframe(
    station_display,
    use_container_width=True,
    hide_index=True
)


# ============================================================
# HISTORICAL HOURLY DATA
# ============================================================

st.subheader("Hourly Data")

display_hourly = filtered_hourly.sort_values(
    ["ride_date", "ride_hour"]
)

st.dataframe(
    display_hourly,
    use_container_width=True,
    hide_index=True
)


# ============================================================
# REAL-TIME DASHBOARD
# ============================================================

st.divider()

st.header("Real-Time Station Monitoring")

st.caption(
    "Live station availability from Citi Bike GBFS "
    "processed by Spark Structured Streaming."
)


@st.fragment(run_every="30s")
def realtime_dashboard():
    st.subheader("Real-Time Station Monitoring")
    st.caption(
        "Live station availability from Citi Bike GBFS "
        "processed by Spark Structured Streaming."
    )

    # Ambil waktu snapshot terbaru
    latest_doc = db["realtime_station_status"].find_one(
        {},
        sort=[("snapshot_time", -1)]
    )

    if not latest_doc:
        st.info("Belum ada data real-time.")
        return

    latest_snapshot = latest_doc["snapshot_time"]

    # Ambil seluruh data station pada snapshot terbaru
    cursor = db["realtime_station_status"].find(
        {"snapshot_time": latest_snapshot},
        {
            "_id": 0,
            "station_id": 1,
            "num_bikes_available": 1,
            "num_ebikes_available": 1,
            "num_docks_available": 1,
            "is_installed": 1,
            "is_renting": 1,
            "is_returning": 1
        }
    )

    realtime_df = pd.DataFrame(list(cursor))

    if realtime_df.empty:
        st.info("Data real-time belum tersedia.")
        return

    # Pastikan kolom numerik
    numeric_columns = [
        "num_bikes_available",
        "num_ebikes_available",
        "num_docks_available",
        "is_installed",
        "is_renting",
        "is_returning"
    ]

    for column in numeric_columns:
        if column in realtime_df.columns:
            realtime_df[column] = pd.to_numeric(
                realtime_df[column],
                errors="coerce"
            ).fillna(0)

    # Hanya station yang ter-install
    active_df = realtime_df[
        realtime_df["is_installed"] == 1
    ].copy()

    # KPI
    active_stations = len(active_df)

    available_bikes = int(
        active_df["num_bikes_available"].sum()
    )

    available_ebikes = int(
        active_df["num_ebikes_available"].sum()
    )

    available_docks = int(
        active_df["num_docks_available"].sum()
    )

    renting_stations = int(
        (active_df["is_renting"] == 1).sum()
    )

    # =========================
    # KPI CARDS
    # =========================

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "Active Stations",
            f"{active_stations:,}"
        )

    with col2:
        st.metric(
            "Available Bikes",
            f"{available_bikes:,}"
        )

    with col3:
        st.metric(
            "Available E-Bikes",
            f"{available_ebikes:,}"
        )

    with col4:
        st.metric(
            "Available Docks",
            f"{available_docks:,}"
        )

    with col5:
        st.metric(
            "Currently Renting",
            f"{renting_stations:,}",
            help="Jumlah station yang sedang berstatus is_renting = 1."
        )

    # Informasi update
    st.caption(
        f"Live | Last update: {latest_snapshot}"
    )

    st.divider()

    # =========================
    # STATION AVAILABILITY
    # =========================

    st.markdown("### Station Availability")

    display_df = active_df[
        [
            "station_id",
            "num_bikes_available",
            "num_ebikes_available",
            "num_docks_available",
            "is_renting",
            "is_returning"
        ]
    ].copy()

    display_df.columns = [
        "Station ID",
        "Available Bikes",
        "Available E-Bikes",
        "Available Docks",
        "Renting",
        "Returning"
    ]

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )

    # =========================
    # LOW AVAILABILITY
    # =========================

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Stations with Low Bikes")

        low_bikes = active_df[
            active_df["num_bikes_available"] <= 2
        ][
            [
                "station_id",
                "num_bikes_available",
                "num_docks_available"
            ]
        ].sort_values(
            "num_bikes_available"
        ).head(10)

        low_bikes.columns = [
            "Station ID",
            "Available Bikes",
            "Available Docks"
        ]

        st.dataframe(
            low_bikes,
            use_container_width=True,
            hide_index=True
        )

    with col2:
        st.markdown("### Stations with Low Docks")

        low_docks = active_df[
            active_df["num_docks_available"] <= 2
        ][
            [
                "station_id",
                "num_bikes_available",
                "num_docks_available"
            ]
        ].sort_values(
            "num_docks_available"
        ).head(10)

        low_docks.columns = [
            "Station ID",
            "Available Bikes",
            "Available Docks"
        ]

        st.dataframe(
            low_docks,
            use_container_width=True,
            hide_index=True
        )

# ============================================================
# RUN REAL-TIME DASHBOARD
# ============================================================

realtime_dashboard()
