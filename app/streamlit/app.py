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

    # Fallback to default if not specified
    return "mongodb://admin:citibikemongo123@citibike-mongodb:27017/citibike?authSource=admin"


@st.cache_resource
def get_database():
    client = MongoClient(build_mongo_uri(), serverSelectionTimeoutMS=5000)
    # Ping database
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
# DATA LOADERS — REAL-TIME METADATA & TRENDS
# ============================================================
@st.cache_data(ttl=3600)
def load_station_metadata():
    """Mengambil metadata nama stasiun dan koordinat dari GBFS Lyft."""
    url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
    try:
        import requests
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            stations = resp.json().get("data", {}).get("stations", [])
            data = [
                {
                    "station_id": str(s.get("station_id")),
                    "station_name": str(s.get("name")),
                    "lat": float(s.get("lat")),
                    "lon": float(s.get("lon")),
                    "capacity": int(s.get("capacity", 0))
                }
                for s in stations if s.get("lat") and s.get("lon")
            ]
            return pd.DataFrame(data)
    except Exception:
        pass
    return pd.DataFrame(columns=["station_id", "station_name", "lat", "lon", "capacity"])


def load_realtime_trend():
    """Mengambil riwayat tren ketersediaan dari 20 snapshot terakhir di MongoDB."""
    pipeline = [
        {
            "$group": {
                "_id": "$snapshot_time",
                "Total Bikes": {"$sum": "$num_bikes_available"},
                "E-Bikes": {"$sum": "$num_ebikes_available"},
                "Available Docks": {"$sum": "$num_docks_available"},
            }
        },
        {"$sort": {"_id": -1}},
        {"$limit": 20}
    ]
    try:
        data = list(db.realtime_station_status.aggregate(pipeline))
        if data:
            df_trend = pd.DataFrame(data).rename(columns={"_id": "Snapshot Time"})
            df_trend["Snapshot Time"] = pd.to_datetime(df_trend["Snapshot Time"])
            return df_trend.sort_values("Snapshot Time")
    except Exception:
        pass
    return pd.DataFrame()


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
        if not exp2_station_df.empty:
            first = exp2_station_df.iloc[0]
            top_station_exp2 = str(first.get("start_station_name", "-"))

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Valid Trips (Jan 2026)", f"{total_rides_exp2:,.0f}")
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
# REAL-TIME DASHBOARD — MONITORING, GRAFIK, TOP/BOTTOM & MAP
# ============================================================
with realtime_tab:
    st.header("Real-Time Station Monitoring")
    st.caption(
        "Data ketersediaan live dari Citi Bike GBFS diproses oleh Spark Structured Streaming. "
        "Dashboard auto-refresh setiap 30 detik."
    )

    @st.fragment(run_every="30s")
    def realtime_dashboard():
        latest_doc = db["realtime_station_status"].find_one(
            {}, sort=[("snapshot_time", -1)]
        )

        if not latest_doc:
            st.info("Belum ada data real-time di MongoDB. Pastikan streaming & collector aktif.")
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
                "last_reported": 1,
                "last_reported_time": 1,
                "is_stale": 1,
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

        # Gabungkan dengan metadata stasiun (nama dan koordinat peta)
        meta_df = load_station_metadata()
        if not meta_df.empty and "station_id" in realtime_df.columns:
            realtime_df["station_id"] = realtime_df["station_id"].astype(str)
            meta_df["station_id"] = meta_df["station_id"].astype(str)
            merged_df = pd.merge(realtime_df, meta_df, on="station_id", how="left")
            merged_df["station_name"] = merged_df["station_name"].fillna(merged_df["station_id"])
        else:
            merged_df = realtime_df.copy()
            merged_df["station_name"] = merged_df["station_id"]

        # Validasi Definisi Active Stations
        installed_mask = (merged_df["is_installed"] == 1) if "is_installed" in merged_df.columns else pd.Series(True, index=merged_df.index)
        renting_mask = (merged_df["is_renting"] == 1) if "is_renting" in merged_df.columns else pd.Series(True, index=merged_df.index)
        returning_mask = (merged_df["is_returning"] == 1) if "is_returning" in merged_df.columns else pd.Series(True, index=merged_df.index)

        installed_count = int(installed_mask.sum())
        active_renting_count = int((installed_mask & renting_mask).sum())
        return_only_count = int((installed_mask & ~renting_mask & returning_mask).sum())
        out_of_service_count = int((installed_mask & ~renting_mask & ~returning_mask).sum())

        active_df = merged_df[installed_mask].copy()

        available_bikes = int(active_df.get("num_bikes_available", pd.Series(dtype=float)).sum())
        available_ebikes = int(active_df.get("num_ebikes_available", pd.Series(dtype=float)).sum())
        available_docks = int(active_df.get("num_docks_available", pd.Series(dtype=float)).sum())

        empty_bikes_count = int((active_df["num_bikes_available"] == 0).sum())
        full_docks_count = int((active_df["num_docks_available"] == 0).sum())
        stale_count = int((active_df["is_stale"] == True).sum()) if "is_stale" in active_df.columns else 0

        # ============================================================
        # 1. KPI CARDS & INDIKATOR LOW AVAILABILITY
        # ============================================================
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric(
            "Active (Renting)",
            f"{active_renting_count:,}",
            help="Stasiun fisik terpasang & melayani sewa (is_installed=1 & is_renting=1)."
        )
        k2.metric(
            "Installed Stations",
            f"{installed_count:,}",
            delta=f"-{out_of_service_count + return_only_count} Non-Renting" if (out_of_service_count + return_only_count) > 0 else None,
            delta_color="inverse",
            help="Total stasiun fisik terpasang."
        )
        k3.metric("Available Bikes", f"{available_bikes:,}")
        k4.metric("Available E-Bikes", f"{available_ebikes:,}")
        k5.metric("Available Docks", f"{available_docks:,}")

        # Baris Indikator Kritis
        alert1, alert2, alert3 = st.columns(3)
        with alert1:
            if empty_bikes_count > 0:
                st.error(f"🚨 **{empty_bikes_count} Stasiun Kosong** (0 sepeda tersedia)")
            else:
                st.success("✅ Tidak ada stasiun yang kehabisan sepeda.")
        with alert2:
            if full_docks_count > 0:
                st.warning(f"⚠️ **{full_docks_count} Stasiun Penuh Dock** (0 dock kosong)")
            else:
                st.success("✅ Tidak ada stasiun yang dock-nya penuh.")
        with alert3:
            if stale_count > 0:
                st.info(f"⏱️ **{stale_count} Stasiun Stale** (>1 jam tidak mengirim status)")
            else:
                st.success("✅ Sinyal pelaporan seluruh stasiun mutakhir.")

        st.caption(f"Live Update · Terakhir diperbarui: {latest_snapshot}")
        st.divider()

        # ============================================================
        # 2. GRAFIK REAL-TIME TREN KETERSEDIAAN SEPEDAH
        # ============================================================
        st.subheader("📈 Real-Time Availability Trend")
        df_trend = load_realtime_trend()
        if not df_trend.empty and len(df_trend) > 1:
            trend_chart_data = df_trend.set_index("Snapshot Time")[["Total Bikes", "E-Bikes", "Available Docks"]]
            st.line_chart(trend_chart_data)
            st.caption("Grafik memantau dinamika total unit sepeda dan dock dari riwayat micro-batch streaming.")
        else:
            st.info("Grafik tren riwayat real-time akan terbentuk seiring berjalannya micro-batch.")

        st.divider()

        # ============================================================
        # 3. TOP / BOTTOM STATIONS (SURPLUS VS DEFISIT)
        # ============================================================
        st.subheader("📊 Top & Bottom Stations Analysis")
        col_top, col_bot, col_docks = st.columns(3)

        with col_top:
            st.markdown("##### 🟢 Top 5 Surplus (Sepeda Terbanyak)")
            top_surplus = active_df.sort_values("num_bikes_available", ascending=False).head(5)[
                ["station_name", "num_bikes_available", "num_docks_available"]
            ].rename(columns={
                "station_name": "Stasiun",
                "num_bikes_available": "Sepeda",
                "num_docks_available": "Docks",
            })
            st.dataframe(top_surplus, use_container_width=True, hide_index=True)

        with col_bot:
            st.markdown("##### 🔴 Top 5 Defisit (Sepeda Paling Sedikit)")
            bottom_deficit = active_df.sort_values(["num_bikes_available", "num_docks_available"], ascending=[True, False]).head(5)[
                ["station_name", "num_bikes_available", "num_docks_available"]
            ].rename(columns={
                "station_name": "Stasiun",
                "num_bikes_available": "Sepeda",
                "num_docks_available": "Docks",
            })
            st.dataframe(bottom_deficit, use_container_width=True, hide_index=True)

        with col_docks:
            st.markdown("##### 🟡 Top 5 Dock Kritis (Dock Tersisa Paling Sedikit)")
            bottom_docks = active_df.sort_values("num_docks_available", ascending=True).head(5)[
                ["station_name", "num_docks_available", "num_bikes_available"]
            ].rename(columns={
                "station_name": "Stasiun",
                "num_docks_available": "Docks Tersisa",
                "num_bikes_available": "Sepeda",
            })
            st.dataframe(bottom_docks, use_container_width=True, hide_index=True)

        st.divider()

        # ============================================================
        # 4. INTERACTIVE STATION MAP (PETA KETERSEDIAAN GEOGRAFIS)
        # ============================================================
        st.subheader("🗺️ Geographic Station Map")
        if "lat" in active_df.columns and "lon" in active_df.columns and active_df["lat"].notna().any():
            map_data = active_df.dropna(subset=["lat", "lon"]).copy()

            # Filter opsi tampilan peta
            map_filter = st.radio(
                "Filter Tampilan Peta:",
                ["Semua Stasiun", "Hanya Stasiun Kritis (Sepeda <= 2)", "Hanya Stasiun Surplus (Sepeda >= 10)"],
                horizontal=True
            )

            if map_filter == "Hanya Stasiun Kritis (Sepeda <= 2)":
                map_data = map_data[map_data["num_bikes_available"] <= 2]
            elif map_filter == "Hanya Stasiun Surplus (Sepeda >= 10)":
                map_data = map_data[map_data["num_bikes_available"] >= 10]

            if not map_data.empty:
                st.map(map_data[["lat", "lon"]], size=20)
                st.caption(f"Menampilkan {len(map_data):,} stasiun di peta New York City.")
            else:
                st.info("Tidak ada stasiun yang memenuhi kriteria filter peta saat ini.")
        else:
            st.info("Koordinat peta stasiun sedang disinkronkan dari metadata GBFS.")

        # ============================================================
        # 5. TABEL LENGKAP KETERSEDIAAN STASIUN
        # ============================================================
        with st.expander("Lihat Tabel Lengkap Status Seluruh Stasiun"):
            only_critical = st.checkbox("Hanya tampilkan stasiun kritis (Sepeda <= 2 atau Dock <= 2)")
            table_df = active_df.copy()
            if only_critical:
                table_df = table_df[(table_df["num_bikes_available"] <= 2) | (table_df["num_docks_available"] <= 2)]

            display_cols = [
                c for c in [
                    "station_name", "station_id", "num_bikes_available", "num_ebikes_available",
                    "num_docks_available", "is_renting", "is_returning", "last_reported_time"
                ] if c in table_df.columns
            ]
            st.dataframe(
                table_df[display_cols].rename(columns={
                    "station_name": "Nama Stasiun",
                    "station_id": "Station ID",
                    "num_bikes_available": "Sepeda",
                    "num_ebikes_available": "E-Bikes",
                    "num_docks_available": "Docks",
                    "is_renting": "Renting",
                    "is_returning": "Returning",
                    "last_reported_time": "Last Reported",
                }),
                use_container_width=True,
                hide_index=True
            )

    realtime_dashboard()
