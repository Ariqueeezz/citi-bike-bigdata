import os
from urllib.parse import quote

import pandas as pd
import streamlit as st
from pymongo import MongoClient


# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Citi Bike NYC Big Data Analytics",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="expanded",
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

    return "mongodb://admin:citibikemongo123@citibike-mongodb:27017/citibike?authSource=admin"


@st.cache_resource
def get_database():
    client = MongoClient(build_mongo_uri(), serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client["citibike"]


try:
    db = get_database()
except Exception as exc:
    st.error(f"Tidak dapat terhubung ke MongoDB: {exc}")
    st.stop()


# ============================================================
# DATA LOADERS — EXPERIMENT 1 (HISTORICAL LOCAL)
# ============================================================
@st.cache_data(ttl=60)
def load_exp1_user_type():
    return pd.DataFrame(list(db.historical_user_type_stats.find({}, {"_id": 0})))


@st.cache_data(ttl=60)
def load_exp1_hourly():
    return pd.DataFrame(list(db.historical_hourly_stats.find({}, {"_id": 0})))


@st.cache_data(ttl=60)
def load_exp1_stations():
    return pd.DataFrame(
        list(db.historical_station_stats.find({}, {"_id": 0}).sort("total_rides", -1).limit(20))
    )


# ============================================================
# DATA LOADERS — EXPERIMENT 2 (HDFS + SPARK)
# ============================================================
@st.cache_data(ttl=60)
def load_exp2_user_type():
    return pd.DataFrame(list(db.exp2_user_type_stats.find({}, {"_id": 0})))


@st.cache_data(ttl=60)
def load_exp2_hourly():
    return pd.DataFrame(list(db.exp2_hourly_stats.find({}, {"_id": 0})))


@st.cache_data(ttl=60)
def load_exp2_stations():
    return pd.DataFrame(
        list(db.exp2_station_stats.find({}, {"_id": 0}).sort("total_rides", -1).limit(20))
    )


# ============================================================
# DATA LOADERS — BATCH DATA LAKEHOUSE (MINIO S3)
# ============================================================
@st.cache_data(ttl=60)
def load_batch_hourly():
    return pd.DataFrame(list(db.minio_hourly_stats.find({}, {"_id": 0})))


@st.cache_data(ttl=60)
def load_batch_user_type():
    return pd.DataFrame(list(db.minio_user_type_stats.find({}, {"_id": 0})))


@st.cache_data(ttl=60)
def load_batch_stations():
    return pd.DataFrame(
        list(db.minio_station_stats.find({}, {"_id": 0}).sort("total_rides", -1))
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
            # Konversi UTC ke WIB (+7 jam)
            df_trend["Snapshot Time"] = pd.to_datetime(df_trend["Snapshot Time"]) + pd.Timedelta(hours=7)
            return df_trend.sort_values("Snapshot Time")
    except Exception:
        pass
    return pd.DataFrame()


# ============================================================
# SIDEBAR CONFIGURATION
# ============================================================
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/0/09/Citi_Bike_logo.svg/320px-Citi_Bike_logo.svg.png", width=180)
    st.markdown("### ⚙️ Konfigurasi Dashboard")
    
    view_mode = st.radio(
        "Pilih Mode Tampilan:",
        [
            "🚀 Mode Ringkas (2 Tab: Batch & Real-Time)",
            "📚 Mode Lengkap (4 Tab: Progres Eksperimen)",
        ],
        index=0,
        help="Pilih Mode Ringkas untuk presentasi bisnis yang bersih, atau Mode Lengkap untuk melihat seluruh evolusi eksperimen (Local -> HDFS -> MinIO S3)."
    )
    
    st.divider()
    st.markdown("#### 📡 Status Arsitektur Big Data")
    st.markdown("• **Object Storage**: `MinIO S3` (Active)")
    st.markdown("• **Compute Engine**: `Apache Spark 3.5`")
    st.markdown("• **Serving Store**: `MongoDB 7.0` (Active)")
    st.markdown("• **Streaming Source**: `GBFS Live Lyft API`")
    st.divider()
    st.caption("Citi Bike Big Data Analytics Platform · Tugas Besar Big Data")


# ============================================================
# MAIN HEADER
# ============================================================
st.title("🚲 Citi Bike NYC Big Data Analytics Platform")
st.caption(
    "Arsitektur Analitik Hibrida: **Batch Processing Data Lakehouse (MinIO S3 + Apache Spark)** & "
    "**Real-Time Station Monitoring (GBFS Streaming)**"
)


# ============================================================
# RENDER FUNCTION: BATCH TRIP ANALYTICS (MINIO S3 LAKEHOUSE)
# ============================================================
def render_batch_tab(tab_context, is_sub_tab=False):
    with tab_context:
        title = "Batch Historical Trip Analytics (MinIO S3 Lakehouse)" if not is_sub_tab else "Experiment 2.1 — MinIO S3 Lakehouse Analytics"
        st.header(title)
        st.caption(
            "Hasil batch processing dari data mentah di MinIO S3 Object Storage (`s3a://citibike-raw/`), "
            "dibersihkan dan dikurasi menggunakan Apache Spark Batch terdistribusi (`s3a://citibike-curated/`), "
            "dan disajikan melalui koleksi MongoDB `minio_*`."
        )

        raw_hourly_df = load_batch_hourly()
        raw_user_type_df = load_batch_user_type()
        raw_station_df = load_batch_stations()

        if raw_hourly_df.empty and raw_user_type_df.empty and raw_station_df.empty:
            st.warning("Belum ada data Batch MinIO di MongoDB. Jalankan pipeline batch terlebih dahulu.")
            return

        # ----------------------------------------------------
        # FILTER PERIODE BULAN & TIPE PENGGUNA
        # ----------------------------------------------------
        filter_col1, filter_col2 = st.columns([2, 2])

        available_months = ["Semua Bulan (Januari & Oktober)"]
        if not raw_user_type_df.empty and "month_label" in raw_user_type_df.columns:
            unique_months = sorted(raw_user_type_df["month_label"].dropna().unique().tolist(), reverse=True)
            available_months.extend(unique_months)

        with filter_col1:
            selected_month = st.selectbox(
                "📅 Pilih Periode Bulan Data:",
                available_months,
                key=f"batch_month_select_{'sub' if is_sub_tab else 'main'}",
                help="Bandingkan data antara Musim Gugur (Oktober ~5 Juta Perjalanan) vs Musim Dingin (Januari)."
            )

        with filter_col2:
            selected_user_type = st.selectbox(
                "👥 Filter Tipe Pengguna:",
                ["Semua (Member & Casual)", "Member (Tahunan)", "Casual (Harian/Turis)"],
                key=f"batch_user_select_{'sub' if is_sub_tab else 'main'}",
            )

        # Terapkan Filter Bulan
        hourly_df = raw_hourly_df.copy()
        user_type_df = raw_user_type_df.copy()
        station_df = raw_station_df.copy()

        if selected_month != "Semua Bulan (Januari & Oktober)":
            if "month_label" in hourly_df.columns:
                hourly_df = hourly_df[hourly_df["month_label"] == selected_month]
            if "month_label" in user_type_df.columns:
                user_type_df = user_type_df[user_type_df["month_label"] == selected_month]
            if "month_label" in station_df.columns:
                station_df = station_df[station_df["month_label"] == selected_month]

        # Terapkan Filter Tipe Pengguna pada hourly
        if selected_user_type == "Member (Tahunan)" and "member_casual" in hourly_df.columns:
            hourly_df = hourly_df[hourly_df["member_casual"] == "member"]
        elif selected_user_type == "Casual (Harian/Turis)" and "member_casual" in hourly_df.columns:
            hourly_df = hourly_df[hourly_df["member_casual"] == "casual"]

        # ----------------------------------------------------
        # KPI SUMMARY CARDS
        # ----------------------------------------------------
        total_rides = 0
        if not user_type_df.empty and "total_rides" in user_type_df.columns:
            total_rides = int(pd.to_numeric(user_type_df["total_rides"], errors="coerce").fillna(0).sum())
        elif not hourly_df.empty and "total_rides" in hourly_df.columns:
            total_rides = int(pd.to_numeric(hourly_df["total_rides"], errors="coerce").fillna(0).sum())

        weighted_avg_duration = None
        if not user_type_df.empty and {"avg_duration_minutes", "total_rides"}.issubset(user_type_df.columns):
            durations = pd.to_numeric(user_type_df["avg_duration_minutes"], errors="coerce")
            counts = pd.to_numeric(user_type_df["total_rides"], errors="coerce")
            valid = durations.notna() & counts.notna() & (counts > 0)
            if valid.any():
                weighted_avg_duration = (durations[valid] * counts[valid]).sum() / counts[valid].sum()

        top_station_name = "-"
        if not station_df.empty and "start_station_name" in station_df.columns:
            # Group by station name in case multiple months are present
            top_st = (
                station_df.groupby("start_station_name")["total_rides"]
                .sum()
                .sort_values(ascending=False)
            )
            if not top_st.empty:
                top_station_name = top_st.index[0]

        member_count = 0
        casual_count = 0
        if not user_type_df.empty and "member_casual" in user_type_df.columns:
            m_row = user_type_df[user_type_df["member_casual"] == "member"]
            c_row = user_type_df[user_type_df["member_casual"] == "casual"]
            if not m_row.empty:
                member_count = int(pd.to_numeric(m_row["total_rides"], errors="coerce").fillna(0).sum())
            if not c_row.empty:
                casual_count = int(pd.to_numeric(c_row["total_rides"], errors="coerce").fillna(0).sum())

        member_ratio = (member_count / total_rides * 100) if total_rides > 0 else 0

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Valid Trips", f"{total_rides:,}")
        k2.metric(
            "Rata-rata Durasi Perjalanan",
            f"{weighted_avg_duration:.2f} menit" if weighted_avg_duration is not None else "N/A"
        )
        k3.metric("Rasio Member (Tahunan)", f"{member_ratio:.1f}%", f"{member_count:,} perjalanan")
        k4.metric("Stasiun Terpopuler", top_station_name[:24] + "..." if len(top_station_name) > 24 else top_station_name)

        # Context Insight Box
        if "Oktober" in selected_month or selected_month == "Semua Bulan (Januari & Oktober)":
            st.info(
                "💡 **Business Insight (Analisis Musiman):** Data batch Oktober 2026 (Musim Gugur / Fall) mencatat **4.981.785 perjalanan valid**, "
                "menunjukkan lonjakan permintaan sangat tinggi dibandingkan musim dingin. Pengguna Member menyumbang **81,2% perjalanan** "
                "dengan durasi rata-rata **11,89 menit** (komuter harian jam kerja), sedangkan Casual bersepeda rata-rata **18,47 menit** (rekreasi akhir pekan)."
            )

        st.divider()

        # ----------------------------------------------------
        # GRAFIK HOURLY DEMAND & TIPE PENGGUNA
        # ----------------------------------------------------
        col_chart_left, col_chart_right = st.columns(2)

        with col_chart_left:
            st.subheader("📈 Distribusi Jam Sibuk (Hourly Demand Curve)")
            if not hourly_df.empty and {"ride_hour", "total_rides"}.issubset(hourly_df.columns):
                h_chart = hourly_df.copy()
                h_chart["ride_hour"] = pd.to_numeric(h_chart["ride_hour"], errors="coerce")
                h_chart["total_rides"] = pd.to_numeric(h_chart["total_rides"], errors="coerce")
                h_grouped = h_chart.groupby("ride_hour")["total_rides"].sum()
                st.bar_chart(h_grouped)
                st.caption("Puncak perjalanan (rush hour) terdeteksi pada pukul **08:00 pagi** (berangkat kerja) dan pukul **17:00–18:00 sore** (pulang kantor).")
            else:
                st.info("Data jam sibuk tidak tersedia untuk filter yang dipilih.")

        with col_chart_right:
            st.subheader("👥 Komparasi Tipe Pengguna (Member vs Casual)")
            if not user_type_df.empty and "member_casual" in user_type_df.columns:
                u_chart = user_type_df.groupby("member_casual").agg(
                    {"total_rides": "sum", "avg_duration_minutes": "mean"}
                ).reset_index()
                u_chart["total_rides"] = pd.to_numeric(u_chart["total_rides"], errors="coerce").fillna(0)
                u_display = u_chart.rename(
                    columns={
                        "member_casual": "Tipe Pengguna",
                        "total_rides": "Total Perjalanan",
                        "avg_duration_minutes": "Rata-rata Durasi (menit)"
                    }
                )
                st.bar_chart(u_display.set_index("Tipe Pengguna")["Total Perjalanan"])
                st.dataframe(u_display, use_container_width=True, hide_index=True)
            else:
                st.info("Data tipe pengguna tidak tersedia.")

        st.divider()

        # ----------------------------------------------------
        # TOP 10 BUSIEST STATIONS
        # ----------------------------------------------------
        st.subheader("🏆 Top 10 Stasiun Keberangkatan Terpopuler di NYC")
        if not station_df.empty and "start_station_name" in station_df.columns:
            agg_stations = (
                station_df.groupby(["start_station_id", "start_station_name"], as_index=False)
                .agg({"total_rides": "sum", "avg_duration_minutes": "mean"})
                .sort_values("total_rides", ascending=False)
                .head(10)
            )

            top10_plot = agg_stations.set_index("start_station_name")
            st.bar_chart(top10_plot["total_rides"])

            station_table = agg_stations.rename(
                columns={
                    "start_station_name": "Nama Stasiun",
                    "start_station_id": "Station ID",
                    "total_rides": "Total Perjalanan",
                    "avg_duration_minutes": "Rata-rata Durasi (menit)"
                }
            )
            st.dataframe(station_table, use_container_width=True, hide_index=True)
        else:
            st.info("Data stasiun belum tersedia.")

        with st.expander("🔍 Lihat Ringkasan Agregasi Hourly Lengkap"):
            if not hourly_df.empty:
                sort_cols = [c for c in ["ride_date", "ride_hour"] if c in hourly_df.columns]
                display_h = hourly_df.sort_values(sort_cols) if sort_cols else hourly_df
                st.dataframe(display_h, use_container_width=True, hide_index=True)


# ============================================================
# RENDER FUNCTION: REAL-TIME STATION MONITORING
# ============================================================
def render_realtime_tab(tab_context):
    with tab_context:
        st.header("Real-Time Station Monitoring (Live GBFS)")
        st.caption(
            "Monitoring ketersediaan sepeda dan dock secara live dari feed GBFS Citi Bike NYC, "
            "diproses secara streaming dan disinkronkan ke MongoDB setiap 30 detik."
        )

        @st.fragment(run_every="30s")
        def realtime_fragment():
            latest_doc = db["realtime_station_status"].find_one(
                {}, sort=[("snapshot_time", -1)]
            )

            if not latest_doc:
                st.info("Belum ada data real-time di MongoDB. Pastikan service collector dan streaming aktif.")
                return

            latest_time = latest_doc["snapshot_time"]
            cursor = db["realtime_station_status"].find({"snapshot_time": latest_time})
            df = pd.DataFrame(list(cursor))

            meta_df = load_station_metadata()
            if not meta_df.empty and "station_id" in df.columns:
                df = df.merge(meta_df, on="station_id", how="left")
                if "station_name_y" in df.columns:
                    df["station_name"] = df["station_name_y"].fillna(df.get("station_name_x", df["station_id"]))

            active_df = df[df["is_renting"] == 1].copy() if "is_renting" in df.columns else df.copy()

            if active_df.empty:
                st.warning("Data snapshot ada namun tidak ada stasiun yang berstatus aktif saat ini.")
                return

            # Waktu WIB (+7 jam)
            local_time = pd.to_datetime(latest_time) + pd.Timedelta(hours=7)
            latest_snapshot = local_time.strftime("%d %B %Y, %H:%M:%S WIB")

            refresh_col1, refresh_col2 = st.columns([3, 1])
            with refresh_col1:
                st.success(f"🟢 **Live Connection**: Terakhir diperbarui pada **{latest_snapshot}** (Auto-refresh 30s)")
            with refresh_col2:
                if st.button("🔄 Segarkan Data Sekarang", use_container_width=True):
                    st.rerun()

            # ----------------------------------------------------
            # 1. KPI SUMMARY CARDS
            # ----------------------------------------------------
            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Active Stations", f"{len(active_df):,}")
            k2.metric("Total Sepeda", f"{int(active_df['num_bikes_available'].sum()):,}")
            k3.metric("Total E-Bikes", f"{int(active_df['num_ebikes_available'].sum()):,}")
            k4.metric("Docks Tersedia", f"{int(active_df['num_docks_available'].sum()):,}")

            empty_count = int((active_df["num_bikes_available"] == 0).sum())
            full_count = int((active_df["num_docks_available"] == 0).sum())
            k5.metric(
                "Stasiun Kosong / Penuh",
                f"{empty_count} / {full_count}",
                delta=f"{empty_count} tanpa sepeda",
                delta_color="inverse" if empty_count > 0 else "normal",
            )

            # Notifikasi Kritis
            if empty_count > 0 or full_count > 0:
                st.warning(
                    f"⚠️ **Peringatan Rebalancing**: Terdapat **{empty_count} stasiun kehabisan sepeda** (butuh suplai unit) "
                    f"dan **{full_count} stasiun dengan dock penuh** (pengguna tidak bisa mengembalikan sepeda)."
                )

            st.divider()

            # ----------------------------------------------------
            # 2. GRAFIK TREN KETERSEDIAAN
            # ----------------------------------------------------
            st.subheader("📈 Real-Time Availability Trend")
            df_trend = load_realtime_trend()
            if not df_trend.empty and len(df_trend) > 1:
                st.line_chart(df_trend.set_index("Snapshot Time")[["Total Bikes", "E-Bikes", "Available Docks"]])
                st.caption("Tren agregat ketersediaan sepeda mekanik, e-bike, dan dock kosong di seluruh stasiun New York City.")
            else:
                st.info("Mengumpulkan riwayat snapshot untuk grafik tren...")

            st.divider()

            # ----------------------------------------------------
            # 3. TOP SURPLUS, DEFISIT, & DOCK PENUH
            # ----------------------------------------------------
            st.subheader("📊 Analisis Surplus vs Defisit Stasiun")
            col_top, col_bot, col_docks = st.columns(3)

            with col_top:
                st.markdown("##### 🟢 Top 5 Surplus (Sepeda Terbanyak)")
                st.caption("Stasiun dengan ketersediaan armada paling melimpah.")
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
                st.caption("Stasiun kehabisan sepeda (**Sepeda = 0**, butuh pasokan armada).")
                bottom_deficit = active_df.sort_values(["num_bikes_available", "num_docks_available"], ascending=[True, False]).head(5)[
                    ["station_name", "num_bikes_available", "num_docks_available"]
                ].rename(columns={
                    "station_name": "Stasiun",
                    "num_bikes_available": "Sepeda",
                    "num_docks_available": "Docks",
                })
                st.dataframe(bottom_deficit, use_container_width=True, hide_index=True)

            with col_docks:
                st.markdown("##### 🟡 Top 5 Dock Penuh (Dock Kosong Paling Sedikit)")
                st.caption("Stasiun penuh total (**Docks Tersisa = 0**, tidak bisa parkir).")
                bottom_docks = active_df.sort_values("num_docks_available", ascending=True).head(5)[
                    ["station_name", "num_docks_available", "num_bikes_available"]
                ].rename(columns={
                    "station_name": "Stasiun",
                    "num_docks_available": "Docks Tersisa",
                    "num_bikes_available": "Sepeda",
                })
                st.dataframe(bottom_docks, use_container_width=True, hide_index=True)

            st.divider()

            # ----------------------------------------------------
            # 4. INTERACTIVE NYC MAP
            # ----------------------------------------------------
            st.subheader("🗺️ Geographic Station Map")
            if "lat" in active_df.columns and "lon" in active_df.columns and active_df["lat"].notna().any():
                map_data = active_df.dropna(subset=["lat", "lon"]).copy()

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
                    st.caption(f"Menampilkan **{len(map_data):,} stasiun** di peta New York City sesuai filter.")
                else:
                    st.info("Tidak ada stasiun yang memenuhi kriteria filter peta saat ini.")
            else:
                st.info("Koordinat peta stasiun sedang disinkronkan dari metadata GBFS.")

            # ----------------------------------------------------
            # 5. TABEL LENGKAP STASIUN
            # ----------------------------------------------------
            with st.expander("📋 Lihat Tabel Lengkap Status Seluruh Stasiun"):
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

        realtime_fragment()


# ============================================================
# RENDER FUNCTION: EXPERIMENT 1 (LOCAL CSV BASELINE)
# ============================================================
def render_exp1_tab(tab_context):
    with tab_context:
        st.header("Experiment 1 · Historical Baseline (Local Files)")
        st.caption("Data historis baseline yang diproses langsung dari file lokal Januari 2026 ke collection `historical_*`.")

        user_type_df = load_exp1_user_type()
        hourly_df = load_exp1_hourly()
        station_df = load_exp1_stations()

        if hourly_df.empty:
            st.warning("Collection historical_hourly_stats belum memiliki data.")
            return

        filter_options = ["All", "member", "casual"]
        user_filter = st.selectbox("Filter User Type:", filter_options, key="exp1_user_filter")

        filtered_hourly = hourly_df.copy()
        if user_filter != "All" and "member_casual" in filtered_hourly.columns:
            filtered_hourly = filtered_hourly[filtered_hourly["member_casual"] == user_filter]

        total_rides = pd.to_numeric(filtered_hourly.get("total_rides", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
        k1, k2 = st.columns(2)
        k1.metric("Total Trips (Exp 1)", f"{total_rides:,.0f}")
        k2.metric("Station Records", f"{len(station_df):,}")

        st.subheader("Hourly Trip Distribution (Exp 1)")
        if not filtered_hourly.empty and {"ride_hour", "total_rides"}.issubset(filtered_hourly.columns):
            st.bar_chart(filtered_hourly.groupby("ride_hour")["total_rides"].sum())


# ============================================================
# RENDER FUNCTION: EXPERIMENT 2 (HDFS + SPARK)
# ============================================================
def render_exp2_tab(tab_context):
    with tab_context:
        st.header("Experiment 2 · HDFS + Apache Spark Batch")
        st.caption("Hasil pemrosesan data historis dari Hadoop HDFS menggunakan Apache Spark Batch (`exp2_*`).")

        exp2_hourly_df = load_exp2_hourly()
        exp2_user_type_df = load_exp2_user_type()
        exp2_station_df = load_exp2_stations()

        if exp2_hourly_df.empty and exp2_user_type_df.empty:
            st.warning("Belum ada data Experiment 2 di MongoDB.")
            return

        total_rides = 0
        if not exp2_user_type_df.empty and "total_rides" in exp2_user_type_df.columns:
            total_rides = pd.to_numeric(exp2_user_type_df["total_rides"], errors="coerce").fillna(0).sum()

        k1, k2 = st.columns(2)
        k1.metric("Total Trips (HDFS + Spark)", f"{total_rides:,.0f}")
        k2.metric("Total Stations (Exp 2)", f"{len(exp2_station_df):,}")

        st.subheader("Hourly Trip Distribution (HDFS)")
        if not exp2_hourly_df.empty and {"ride_hour", "total_rides"}.issubset(exp2_hourly_df.columns):
            st.bar_chart(exp2_hourly_df.groupby("ride_hour")["total_rides"].sum())


# ============================================================
# TAB DISPATCHER BASED ON SELECTED MODE
# ============================================================
if "Mode Ringkas" in view_mode:
    tab1, tab2 = st.tabs(
        [
            "📊 Batch Trip Analytics (MinIO S3 Lakehouse)",
            "⚡ Real-Time Station Monitoring (Live GBFS)",
        ]
    )
    render_batch_tab(tab1, is_sub_tab=False)
    render_realtime_tab(tab2)
else:
    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "Experiment 1 · Historical",
            "Experiment 2 · HDFS + Spark",
            "Experiment 2.1 · MinIO S3",
            "Real-Time Monitoring",
        ]
    )
    render_exp1_tab(tab1)
    render_exp2_tab(tab2)
    render_batch_tab(tab3, is_sub_tab=True)
    render_realtime_tab(tab4)
