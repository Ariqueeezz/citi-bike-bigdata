import os
from urllib.parse import quote

import pandas as pd
import streamlit as st
from pymongo import MongoClient


# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Citi Bike Analytics",
    page_icon="🚲",
    layout="wide",
)


# ============================================================
# MONGODB CONNECTION
# ============================================================
# Recommended: set MONGO_URI in the Streamlit container environment.
# For backward compatibility, username/password can also be supplied
# through MONGO_USER and MONGO_PASSWORD. No credentials are hardcoded.
def build_mongo_uri():
    configured_uri = os.getenv("MONGO_URI")
    if configured_uri:
        return configured_uri

    username = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASSWORD")
    if username and password:
        username = quote(username, safe="")
        password = quote(password, safe="")
        return (
            f"mongodb://{username}:{password}"
            "@citibike-mongodb:27017/citibike?authSource=admin"
        )

    # Allow unauthenticated local MongoDB only if explicitly configured.
    # For this project, the authenticated URI/env vars should be used.
    raise RuntimeError(
        "Kredensial MongoDB belum dikonfigurasi. Atur MONGO_URI atau "
        "MONGO_USER dan MONGO_PASSWORD pada environment container Streamlit."
    )


@st.cache_resource
def get_database():
    client = MongoClient(build_mongo_uri(), serverSelectionTimeoutMS=5000)
    # Fail early with a readable error if MongoDB cannot be reached.
    client.admin.command("ping")
    return client["citibike"]


try:
    db = get_database()
except Exception as exc:
    st.error(f"Tidak dapat terhubung ke MongoDB: {exc}")
    st.stop()


# ============================================================
# DATA LOADERS — EXPERIMENT 1
# ============================================================
@st.cache_data(ttl=60)
def load_exp1_user_type():
    return pd.DataFrame(
        list(db.historical_user_type_stats.find({}, {"_id": 0}))
    )


@st.cache_data(ttl=60)
def load_exp1_hourly():
    return pd.DataFrame(
        list(db.historical_hourly_stats.find({}, {"_id": 0}))
    )


@st.cache_data(ttl=60)
def load_exp1_stations():
    return pd.DataFrame(
        list(
            db.historical_station_stats.find({}, {"_id": 0})
            .sort("total_rides", -1)
            .limit(20)
        )
    )


# ============================================================
# DATA LOADERS — EXPERIMENT 2
# ============================================================
@st.cache_data(ttl=60)
def load_exp2_hourly():
    return pd.DataFrame(
        list(db.exp2_hourly_stats.find({}, {"_id": 0}))
    )


@st.cache_data(ttl=60)
def load_exp2_user_type():
    return pd.DataFrame(
        list(db.exp2_user_type_stats.find({}, {"_id": 0}))
    )


@st.cache_data(ttl=60)
def load_exp2_stations():
    return pd.DataFrame(
        list(
            db.exp2_station_stats.find({}, {"_id": 0})
            .sort("total_rides", -1)
            .limit(20)
        )
    )


# ============================================================
# PAGE HEADER
# ============================================================
st.title("Citi Bike NYC Analytics")
st.caption("January 2026 trip analysis · Experiment 1 and Experiment 2")

exp1_tab, exp2_tab, realtime_tab = st.tabs(
    ["Experiment 1 · Historical", "Experiment 2 · HDFS + Spark", "Real-Time Monitoring"]
)


# ============================================================
# EXPERIMENT 1 — EXISTING HISTORICAL DASHBOARD
# ============================================================
with exp1_tab:
    st.header("Historical Analysis — Experiment 1")
    st.caption("Data dari collection historical_*; fitur dashboard lama dipertahankan.")

    user_type_df = load_exp1_user_type()
    hourly_df = load_exp1_hourly()
    station_df = load_exp1_stations()

    if hourly_df.empty:
        st.warning("Collection historical_hourly_stats belum memiliki data.")
    else:
        filter_options = ["All", "member", "casual"]
        user_filter = st.selectbox(
            "Filter User Type",
            filter_options,
            key="exp1_user_filter",
        )

        filtered_hourly = hourly_df.copy()
        if user_filter != "All" and "member_casual" in filtered_hourly.columns:
            filtered_hourly = filtered_hourly[
                filtered_hourly["member_casual"] == user_filter
            ]
        elif user_filter != "All" and "member_casual" not in filtered_hourly.columns:
            st.info(
                "Filter tipe pengguna tidak tersedia pada data hourly Experiment 1; "
                "menampilkan seluruh tipe pengguna."
            )

        total_rides = pd.to_numeric(
            filtered_hourly.get("total_rides", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0).sum()

        # Weighted mean is more accurate than taking the simple mean of hourly averages.
        avg_duration = None
        if {"avg_duration_minutes", "total_rides"}.issubset(filtered_hourly.columns):
            durations = pd.to_numeric(filtered_hourly["avg_duration_minutes"], errors="coerce")
            counts = pd.to_numeric(filtered_hourly["total_rides"], errors="coerce")
            valid = durations.notna() & counts.notna() & (counts > 0)
            if valid.any():
                avg_duration = (durations[valid] * counts[valid]).sum() / counts[valid].sum()

        top_station = "-"
        if not station_df.empty and "start_station_name" in station_df.columns:
            top_station = str(station_df.iloc[0]["start_station_name"])

        k1, k2, k3 = st.columns(3)
        k1.metric("Total Rides", f"{total_rides:,.0f}")
        k2.metric(
            "Weighted Avg. Duration",
            f"{avg_duration:.2f} min" if avg_duration is not None else "N/A",
        )
        k3.metric("Top Station", top_station)

        if "ride_hour" in filtered_hourly.columns and "total_rides" in filtered_hourly.columns:
            st.subheader("Ride Activity by Hour")
            hourly_chart = (
                filtered_hourly.assign(
                    ride_hour=pd.to_numeric(filtered_hourly["ride_hour"], errors="coerce"),
                    total_rides=pd.to_numeric(filtered_hourly["total_rides"], errors="coerce"),
                )
                .dropna(subset=["ride_hour", "total_rides"])
                .groupby("ride_hour")["total_rides"]
                .sum()
                .sort_index()
            )
            st.bar_chart(hourly_chart)

        st.subheader("Top 20 Start Stations")
        station_columns = [
            column for column in ["start_station_name", "total_rides", "avg_duration_minutes"]
            if column in station_df.columns
        ]
        if not station_df.empty and station_columns:
            station_display = station_df[station_columns].copy()
            station_display = station_display.rename(
                columns={
                    "start_station_name": "Station",
                    "total_rides": "Total Rides",
                    "avg_duration_minutes": "Avg Duration (min)",
                }
            )
            st.dataframe(station_display, use_container_width=True, hide_index=True)
        else:
            st.info("Data stasiun Experiment 1 tidak tersedia atau strukturnya berbeda.")

        st.subheader("Hourly Data")
        sort_columns = [c for c in ["ride_date", "ride_hour"] if c in filtered_hourly.columns]
        display_hourly = filtered_hourly.sort_values(sort_columns) if sort_columns else filtered_hourly
        st.dataframe(display_hourly, use_container_width=True, hide_index=True)


# ============================================================
# EXPERIMENT 2 — HDFS + SPARK BATCH RESULTS IN MONGODB
# ============================================================
with exp2_tab:
    st.header("Experiment 2 — HDFS + Spark Batch Analytics")
    st.caption(
        "Hasil batch processing dari data mentah di HDFS, diproses menggunakan Spark, "
        "lalu disajikan melalui collection exp2_* di MongoDB."
    )

    exp2_hourly_df = load_exp2_hourly()
    exp2_user_type_df = load_exp2_user_type()
    exp2_station_df = load_exp2_stations()

    if exp2_hourly_df.empty and exp2_user_type_df.empty and exp2_station_df.empty:
        st.warning(
            "Belum ada data Experiment 2 yang terbaca. Periksa collection "
            "exp2_hourly_stats, exp2_user_type_stats, dan exp2_station_stats."
        )
    else:
        total_rides_exp2 = 0
        if not exp2_user_type_df.empty and "total_rides" in exp2_user_type_df.columns:
            total_rides_exp2 = pd.to_numeric(
                exp2_user_type_df["total_rides"], errors="coerce"
            ).fillna(0).sum()
        elif not exp2_hourly_df.empty and "total_rides" in exp2_hourly_df.columns:
            # Hourly collection contains date-hour aggregates; this is a fallback.
            total_rides_exp2 = pd.to_numeric(
                exp2_hourly_df["total_rides"], errors="coerce"
            ).fillna(0).sum()

        weighted_avg_exp2 = None
        if not exp2_user_type_df.empty and {
            "avg_duration_minutes", "total_rides"
        }.issubset(exp2_user_type_df.columns):
            durations = pd.to_numeric(exp2_user_type_df["avg_duration_minutes"], errors="coerce")
            counts = pd.to_numeric(exp2_user_type_df["total_rides"], errors="coerce")
            valid = durations.notna() & counts.notna() & (counts > 0)
            if valid.any():
                weighted_avg_exp2 = (durations[valid] * counts[valid]).sum() / counts[valid].sum()

        top_station_exp2 = "-"
        top_station_rides = 0
        if not exp2_station_df.empty:
            first = exp2_station_df.iloc[0]
            top_station_exp2 = str(first.get("start_station_name", "-"))
            top_station_rides = int(pd.to_numeric(
                pd.Series([first.get("total_rides", 0)]), errors="coerce"
            ).fillna(0).iloc[0])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Valid Trips", f"{total_rides_exp2:,.0f}")
        k2.metric(
            "Weighted Avg. Duration",
            f"{weighted_avg_exp2:.2f} min" if weighted_avg_exp2 is not None else "N/A",
        )
        k3.metric("Hourly Groups", f"{len(exp2_hourly_df):,}")
        k4.metric("Stations Aggregated", f"{len(exp2_station_df):,}")

        st.divider()
        left, right = st.columns(2)

        with left:
            st.subheader("Ride Activity by Date and Hour")
            required_hourly = {"ride_hour", "total_rides"}
            if not exp2_hourly_df.empty and required_hourly.issubset(exp2_hourly_df.columns):
                chart_df = exp2_hourly_df.copy()
                chart_df["ride_hour"] = pd.to_numeric(chart_df["ride_hour"], errors="coerce")
                chart_df["total_rides"] = pd.to_numeric(chart_df["total_rides"], errors="coerce")
                chart_df = chart_df.dropna(subset=["ride_hour", "total_rides"])
                by_hour = chart_df.groupby("ride_hour", as_index=True)["total_rides"].sum().sort_index()
                st.bar_chart(by_hour)
                st.caption("Grafik menggabungkan jumlah perjalanan dari seluruh tanggal berdasarkan jam.")
            else:
                st.info("Kolom ride_hour dan total_rides belum tersedia pada collection hourly.")

        with right:
            st.subheader("Trips by User Type")
            if not exp2_user_type_df.empty and "member_casual" in exp2_user_type_df.columns:
                user_display = exp2_user_type_df.copy()
                rename_map = {
                    "member_casual": "User Type",
                    "total_rides": "Total Rides",
                    "avg_duration_minutes": "Avg Duration (min)",
                }
                user_display = user_display.rename(columns=rename_map)
                st.bar_chart(user_display.set_index("User Type")["Total Rides"])
                st.dataframe(user_display, use_container_width=True, hide_index=True)
            else:
                st.info("Data agregasi tipe pengguna belum tersedia atau strukturnya berbeda.")

        st.subheader("Top 20 Start Stations — Experiment 2")
        if not exp2_station_df.empty:
            station_cols = [
                c for c in ["start_station_name", "start_station_id", "total_rides"]
                if c in exp2_station_df.columns
            ]
            station_display = exp2_station_df[station_cols].copy().rename(
                columns={
                    "start_station_name": "Station",
                    "start_station_id": "Station ID",
                    "total_rides": "Total Rides",
                }
            )
            st.dataframe(station_display, use_container_width=True, hide_index=True)
            if "start_station_name" in exp2_station_df.columns and "total_rides" in exp2_station_df.columns:
                plot_df = exp2_station_df.head(10).copy()
                plot_df["total_rides"] = pd.to_numeric(plot_df["total_rides"], errors="coerce")
                plot_df = plot_df.dropna(subset=["total_rides"]).set_index("start_station_name")
                st.subheader("Top 10 Stations by Trips")
                st.bar_chart(plot_df["total_rides"])
        else:
            st.info("Data stasiun Experiment 2 belum tersedia.")

        with st.expander("Lihat data hourly lengkap"):
            if not exp2_hourly_df.empty:
                sort_columns = [c for c in ["ride_date", "ride_hour"] if c in exp2_hourly_df.columns]
                hourly_display = exp2_hourly_df.sort_values(sort_columns) if sort_columns else exp2_hourly_df
                st.dataframe(hourly_display, use_container_width=True, hide_index=True)
            else:
                st.info("Data hourly belum tersedia.")


# ============================================================
# REAL-TIME DASHBOARD — EXISTING MONITORING
# ============================================================
with realtime_tab:
    st.header("Real-Time Station Monitoring")
    st.caption(
        "Data berasal dari Citi Bike GBFS dan diproses oleh Spark Structured Streaming. "
        "Jika collector sedang dihentikan, tampilan menunjukkan snapshot terakhir yang tersimpan."
    )

    @st.fragment(run_every="30s")
    def realtime_dashboard():
        latest_doc = db["realtime_station_status"].find_one(
            {}, sort=[("snapshot_time", -1)]
        )

        if not latest_doc:
            st.info("Belum ada data real-time.")
            return

        latest_snapshot = latest_doc.get("snapshot_time")
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
                "is_returning": 1,
            },
        )
        realtime_df = pd.DataFrame(list(cursor))

        if realtime_df.empty:
            st.info("Data real-time belum tersedia.")
            return

        numeric_columns = [
            "num_bikes_available", "num_ebikes_available", "num_docks_available",
            "is_installed", "is_renting", "is_returning",
        ]
        for column in numeric_columns:
            if column in realtime_df.columns:
                realtime_df[column] = pd.to_numeric(
                    realtime_df[column], errors="coerce"
                ).fillna(0)

        if "is_installed" in realtime_df.columns:
            active_df = realtime_df[realtime_df["is_installed"] == 1].copy()
        else:
            active_df = realtime_df.copy()

        active_stations = len(active_df)
        available_bikes = int(active_df.get("num_bikes_available", pd.Series(dtype=float)).sum())
        available_ebikes = int(active_df.get("num_ebikes_available", pd.Series(dtype=float)).sum())
        available_docks = int(active_df.get("num_docks_available", pd.Series(dtype=float)).sum())
        renting_stations = int((active_df.get("is_renting", pd.Series(dtype=float)) == 1).sum())

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Active Stations", f"{active_stations:,}")
        c2.metric("Available Bikes", f"{available_bikes:,}")
        c3.metric("Available E-Bikes", f"{available_ebikes:,}")
        c4.metric("Available Docks", f"{available_docks:,}")
        c5.metric("Currently Renting", f"{renting_stations:,}")
        st.caption(f"Snapshot terakhir tersimpan: {latest_snapshot}")

        st.subheader("Station Availability")
        display_columns = [
            c for c in [
                "station_id", "num_bikes_available", "num_ebikes_available",
                "num_docks_available", "is_renting", "is_returning",
            ] if c in active_df.columns
        ]
        display_df = active_df[display_columns].copy().rename(
            columns={
                "station_id": "Station ID",
                "num_bikes_available": "Available Bikes",
                "num_ebikes_available": "Available E-Bikes",
                "num_docks_available": "Available Docks",
                "is_renting": "Renting",
                "is_returning": "Returning",
            }
        )
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Stations with Low Bikes")
            if "num_bikes_available" in active_df.columns:
                low_bikes = active_df[active_df["num_bikes_available"] <= 2]
                cols = [c for c in ["station_id", "num_bikes_available", "num_docks_available"] if c in low_bikes.columns]
                st.dataframe(
                    low_bikes[cols].sort_values("num_bikes_available").head(10).rename(
                        columns={
                            "station_id": "Station ID",
                            "num_bikes_available": "Available Bikes",
                            "num_docks_available": "Available Docks",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
        with c2:
            st.subheader("Stations with Low Docks")
            if "num_docks_available" in active_df.columns:
                low_docks = active_df[active_df["num_docks_available"] <= 2]
                cols = [c for c in ["station_id", "num_bikes_available", "num_docks_available"] if c in low_docks.columns]
                st.dataframe(
                    low_docks[cols].sort_values("num_docks_available").head(10).rename(
                        columns={
                            "station_id": "Station ID",
                            "num_bikes_available": "Available Bikes",
                            "num_docks_available": "Available Docks",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

    realtime_dashboard()
