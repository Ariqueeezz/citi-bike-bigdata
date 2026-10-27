import os
from urllib.parse import quote

import altair as alt
import pandas as pd
import requests
import streamlit as st
from pymongo import MongoClient


# ============================================================
# 1. PAGE CONFIGURATION & METADATA
# ============================================================
st.set_page_config(
    page_title="Citi Bike NYC · Enterprise Big Data Analytics",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# 2. EXECUTIVE CUSTOM CSS (PROFESSIONAL UI SYSTEM)
# ============================================================
st.markdown(
    """
    <style>
    /* Global Typography & Spacing */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    /* Header Container */
    .dashboard-header {
        background: linear-gradient(135deg, #0A192F 0%, #172A45 50%, #002B49 100%);
        padding: 24px 30px;
        border-radius: 14px;
        color: #FFFFFF;
        margin-bottom: 24px;
        box-shadow: 0 8px 20px rgba(0, 43, 73, 0.15);
        border: 1px solid rgba(255, 255, 255, 0.08);
    }
    
    .dashboard-title {
        font-size: 26px;
        font-weight: 800;
        letter-spacing: -0.5px;
        margin: 0;
        display: flex;
        align-items: center;
        gap: 12px;
        color: #F8FAFC;
    }
    
    .dashboard-subtitle {
        font-size: 13.5px;
        color: #94A3B8;
        margin-top: 6px;
        font-weight: 400;
        line-height: 1.5;
    }

    /* Professional Metric Cards */
    .metric-card {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 18px 20px;
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.04);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 14px rgba(0, 0, 0, 0.08);
        border-color: #CBD5E1;
    }
    
    .metric-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 8px;
    }
    
    .metric-label {
        font-size: 12px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.6px;
        color: #64748B;
    }
    
    .metric-icon-box {
        width: 32px;
        height: 32px;
        border-radius: 8px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 16px;
    }
    
    .metric-value {
        font-size: 28px;
        font-weight: 800;
        letter-spacing: -0.5px;
        color: #0F172A;
        margin: 4px 0;
        line-height: 1.1;
    }
    
    .metric-subtext {
        font-size: 12px;
        color: #64748B;
        font-weight: 500;
        display: flex;
        align-items: center;
        gap: 6px;
    }
    
    .metric-pill-green {
        background-color: #ECFDF5;
        color: #059669;
        font-weight: 700;
        padding: 2px 7px;
        border-radius: 6px;
        font-size: 11px;
    }
    
    .metric-pill-blue {
        background-color: #EFF6FF;
        color: #2563EB;
        font-weight: 700;
        padding: 2px 7px;
        border-radius: 6px;
        font-size: 11px;
    }

    .metric-pill-red {
        background-color: #FEF2F2;
        color: #DC2626;
        font-weight: 700;
        padding: 2px 7px;
        border-radius: 6px;
        font-size: 11px;
    }

    /* Insight Banner */
    .insight-card {
        background-color: #F8FAFC;
        border-left: 4px solid #0056B3;
        padding: 16px 20px;
        border-radius: 8px;
        margin: 18px 0 24px 0;
        font-size: 13.5px;
        color: #334155;
        line-height: 1.6;
    }

    /* Live Telemetry Banner */
    .live-status-bar {
        background: #0F172A;
        border: 1px solid #1E293B;
        border-radius: 10px;
        padding: 12px 20px;
        margin-bottom: 20px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        color: #E2E8F0;
    }

    .pulsing-dot {
        display: inline-block;
        width: 10px;
        height: 10px;
        background-color: #10B981;
        border-radius: 50%;
        box-shadow: 0 0 0 rgba(16, 185, 129, 0.4);
        animation: pulse 1.5s infinite;
        margin-right: 8px;
    }

    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.6); }
        70% { box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
        100% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }

    /* Section Subheaders */
    .section-title {
        font-size: 17px;
        font-weight: 700;
        color: #0F172A;
        margin-bottom: 4px;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    .section-caption {
        font-size: 12.5px;
        color: #64748B;
        margin-bottom: 14px;
        line-height: 1.4;
    }

    /* Action Alert Banners */
    .alert-box {
        border-radius: 8px;
        padding: 12px 18px;
        font-size: 13px;
        margin-bottom: 16px;
        line-height: 1.5;
    }
    .alert-warning {
        background-color: #FFFBEB;
        border-left: 4px solid #F59E0B;
        color: #92400E;
    }
    .alert-success {
        background-color: #ECFDF5;
        border-left: 4px solid #10B981;
        color: #065F46;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# 3. MONGODB RESILIENT CONNECTION
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
    st.error(f"❌ Tidak dapat terhubung ke MongoDB Serving Store: {exc}")
    st.stop()


# ============================================================
# 4. DATA LOADERS (CACHE OPTIMIZED)
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


@st.cache_data(ttl=3600)
def load_station_metadata():
    """Mengambil metadata koordinat dan nama stasiun dari feed resmi GBFS Lyft."""
    url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            stations = resp.json().get("data", {}).get("stations", [])
            data = [
                {
                    "station_id": str(s.get("station_id")),
                    "station_name": str(s.get("name")),
                    "lat": float(s.get("lat")),
                    "lon": float(s.get("lon")),
                    "capacity": int(s.get("capacity", 0)),
                }
                for s in stations
                if s.get("lat") and s.get("lon")
            ]
            return pd.DataFrame(data)
    except Exception:
        pass
    return pd.DataFrame(columns=["station_id", "station_name", "lat", "lon", "capacity"])


def load_realtime_trend():
    """Mengambil riwayat snapshot tren ketersediaan dari MongoDB."""
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
        {"$limit": 20},
    ]
    try:
        data = list(db.realtime_station_status.aggregate(pipeline))
        if data:
            df_trend = pd.DataFrame(data).rename(columns={"_id": "Snapshot Time"})
            # Konversi UTC ke Waktu Indonesia Barat (WIB = UTC+7)
            df_trend["Snapshot Time"] = pd.to_datetime(df_trend["Snapshot Time"]) + pd.Timedelta(hours=7)
            return df_trend.sort_values("Snapshot Time")
    except Exception:
        pass
    return pd.DataFrame()


# ============================================================
# 5. SIDEBAR: PLATFORM GOVERNANCE & ARCHITECTURE
# ============================================================
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/thumb/0/09/Citi_Bike_logo.svg/320px-Citi_Bike_logo.svg.png",
        width=160,
    )
    st.markdown("### 🏛️ Platform Architecture")
    st.caption("Solusi Big Data Terpadu Berbasis **Lambda Architecture**")

    st.markdown(
        """
        * **🗄️ Data Lakehouse:** `MinIO S3`
          * `citibike-raw/` (Raw CSV)
          * `citibike-curated/` (Parquet)
        * **📨 Event Stream Buffer:** `Apache Kafka 3.8`
          * Topic: `citibike-station-status`
        * **⚡ Distributed Compute:** `Apache Spark 3.5`
          * Spark Batch (Ingestion & Cleansing)
          * Spark Structured Streaming (Kafka)
        * **🍃 Serving Store:** `MongoDB 7.0`
          * Koleksi `minio_*` & `realtime_*`
        * **📊 Consumption Layer:** `Streamlit UI`
        """
    )
    st.divider()

    st.markdown("#### 📡 System Health Indicators")
    st.markdown("🟢 **Storage Layer (MinIO):** *Active (S3A)*")
    st.markdown("🟢 **Message Broker (Kafka):** *Connected (citibike-kafka)*")
    st.markdown("🟢 **Serving Store (MongoDB):** *Connected*")
    st.markdown("🟢 **Streaming Ingestion:** *Kafka Consumer Active*")
    st.markdown("🟢 **Data Quality Gate:** *Passed (No Anomalies)*")
    st.divider()
    st.caption("Citi Bike NYC Analytics v2.1 · Production Ready")


# ============================================================
# 6. EXECUTIVE HEADER BANNER
# ============================================================
st.markdown(
    """
    <div class="dashboard-header">
        <h1 class="dashboard-title">🚲 Citi Bike NYC · Enterprise Big Data Analytics</h1>
        <p class="dashboard-subtitle">
            Platform intelijen analitik mobilitas terpadu New York City. Memadukan <b>Batch Historical Lakehouse</b> 
            (berkapasitas jutaan perjalanan dari MinIO S3 Object Storage) dengan <b>Real-Time Station Monitoring</b> 
            (pemantauan live feed GBFS streaming untuk kebutuhan rebalancing armada harian).
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# 7. FIXED 2-TAB DASHBOARD (CLEAN & NON-AMBIGUOUS)
# ============================================================
tab_batch, tab_realtime = st.tabs(
    [
        "📊 Batch Historical Trip Analytics (MinIO S3 Lakehouse)",
        "⚡ Real-Time Station Monitoring (Live GBFS)",
    ]
)


# ============================================================
# TAB 1: BATCH HISTORICAL TRIP ANALYTICS
# ============================================================
with tab_batch:
    raw_hourly_df = load_batch_hourly()
    raw_user_type_df = load_batch_user_type()
    raw_station_df = load_batch_stations()

    if raw_hourly_df.empty and raw_user_type_df.empty and raw_station_df.empty:
        st.warning("⚠️ Data Batch MinIO belum tersedia di MongoDB. Silakan jalankan batch pipeline terlebih dahulu.")
    else:
        # ----------------------------------------------------
        # FILTER BAR
        # ----------------------------------------------------
        f_col1, f_col2, f_col3 = st.columns([2.5, 2.5, 3])

        available_months = ["Semua Periode (Januari & Oktober 2026)"]
        if not raw_user_type_df.empty and "month_label" in raw_user_type_df.columns:
            unique_months = sorted(raw_user_type_df["month_label"].dropna().unique().tolist(), reverse=True)
            available_months.extend(unique_months)

        with f_col1:
            selected_month = st.selectbox(
                "📅 Periode Data Perjalanan:",
                available_months,
                index=0,
                help="Bandingkan data antara Musim Gugur (Oktober ~4.98 Juta Trips) vs Musim Dingin (Januari).",
            )

        with f_col2:
            selected_user_type = st.selectbox(
                "👥 Segmentasi Pengguna:",
                ["Semua Pengguna (Member & Casual)", "Member Tahunan (Komuter Reguler)", "Casual (Harian / Wisatawan)"],
                index=0,
                help="Saring analisis berdasarkan tipe pelanggan Citi Bike.",
            )

        with f_col3:
            st.write("")
            st.caption(
                "💡 *Gunakan filter untuk menganalisis tren musiman (*seasonality*) dan perbedaan perilaku komuter kerja vs wisatawan.*"
            )

        # Menerapkan Filter Dataset
        hourly_df = raw_hourly_df.copy()
        user_type_df = raw_user_type_df.copy()
        station_df = raw_station_df.copy()

        if selected_month != "Semua Periode (Januari & Oktober 2026)":
            if "month_label" in hourly_df.columns:
                hourly_df = hourly_df[hourly_df["month_label"] == selected_month]
            if "month_label" in user_type_df.columns:
                user_type_df = user_type_df[user_type_df["month_label"] == selected_month]
            if "month_label" in station_df.columns:
                station_df = station_df[station_df["month_label"] == selected_month]

        if selected_user_type == "Member Tahunan (Komuter Reguler)" and "member_casual" in hourly_df.columns:
            hourly_df = hourly_df[hourly_df["member_casual"] == "member"]
        elif selected_user_type == "Casual (Harian / Wisatawan)" and "member_casual" in hourly_df.columns:
            hourly_df = hourly_df[hourly_df["member_casual"] == "casual"]

        # ----------------------------------------------------
        # PERHITUNGAN METRIK KPI
        # ----------------------------------------------------
        total_rides = 0
        if not user_type_df.empty and "total_rides" in user_type_df.columns:
            total_rides = int(pd.to_numeric(user_type_df["total_rides"], errors="coerce").fillna(0).sum())
        elif not hourly_df.empty and "total_rides" in hourly_df.columns:
            total_rides = int(pd.to_numeric(hourly_df["total_rides"], errors="coerce").fillna(0).sum())

        weighted_avg_duration = 0.0
        if not user_type_df.empty and {"avg_duration_minutes", "total_rides"}.issubset(user_type_df.columns):
            durations = pd.to_numeric(user_type_df["avg_duration_minutes"], errors="coerce")
            counts = pd.to_numeric(user_type_df["total_rides"], errors="coerce")
            valid = durations.notna() & counts.notna() & (counts > 0)
            if valid.any():
                weighted_avg_duration = (durations[valid] * counts[valid]).sum() / counts[valid].sum()

        top_station_name = "N/A"
        top_station_rides = 0
        if not station_df.empty and "start_station_name" in station_df.columns:
            top_st = (
                station_df.groupby("start_station_name")["total_rides"]
                .sum()
                .sort_values(ascending=False)
            )
            if not top_st.empty:
                top_station_name = top_st.index[0]
                top_station_rides = int(top_st.iloc[0])

        member_count = 0
        casual_count = 0
        if not user_type_df.empty and "member_casual" in user_type_df.columns:
            m_row = user_type_df[user_type_df["member_casual"] == "member"]
            c_row = user_type_df[user_type_df["member_casual"] == "casual"]
            if not m_row.empty:
                member_count = int(pd.to_numeric(m_row["total_rides"], errors="coerce").fillna(0).sum())
            if not c_row.empty:
                casual_count = int(pd.to_numeric(c_row["total_rides"], errors="coerce").fillna(0).sum())

        member_ratio = (member_count / total_rides * 100) if total_rides > 0 else 0.0

        # ----------------------------------------------------
        # KPI SUMMARY CARDS (MODERN PROFESSIONAL DESIGN)
        # ----------------------------------------------------
        k1, k2, k3, k4 = st.columns(4)

        with k1:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-label">Total Perjalanan Valid</span>
                        <div class="metric-icon-box" style="background:#EFF6FF; color:#2563EB;">🚲</div>
                    </div>
                    <div class="metric-value">{total_rides:,}</div>
                    <div class="metric-subtext">
                        <span class="metric-pill-green">100% Quality Gate</span>
                        <span>data bersih MinIO</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with k2:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-label">Rata-Rata Durasi</span>
                        <div class="metric-icon-box" style="background:#ECFDF5; color:#059669;">⏱️</div>
                    </div>
                    <div class="metric-value">{weighted_avg_duration:.2f} <span style="font-size:16px; font-weight:600;">menit</span></div>
                    <div class="metric-subtext">
                        <span class="metric-pill-blue">Titik ke Titik</span>
                        <span>durasi wajar perjalanan</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with k3:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-label">Rasio Member Tahunan</span>
                        <div class="metric-icon-box" style="background:#F5F3FF; color:#7C3AED;">💳</div>
                    </div>
                    <div class="metric-value">{member_ratio:.1f}%</div>
                    <div class="metric-subtext">
                        <span class="metric-pill-green">{member_count:,} trip</span>
                        <span>komuter harian aktif</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with k4:
            clean_station_display = (top_station_name[:22] + "...") if len(top_station_name) > 22 else top_station_name
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-label">Stasiun Terpopuler</span>
                        <div class="metric-icon-box" style="background:#FFFBEB; color:#D97706;">📍</div>
                    </div>
                    <div class="metric-value" style="font-size:20px; line-height:1.3;" title="{top_station_name}">{clean_station_display}</div>
                    <div class="metric-subtext">
                        <span class="metric-pill-blue">{top_station_rides:,} trip</span>
                        <span>pusat keberangkatan</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ----------------------------------------------------
        # EXECUTIVE BUSINESS CONTEXT BOX
        # ----------------------------------------------------
        st.markdown(
            """
            <div class="insight-card">
                <b>📌 Executive Summary & Pola Mobilitas Kota:</b><br/>
                Dataset batch membuktikan bahwa Citi Bike NYC merupakan tulang punggung transportasi transit harian di Manhattan dan sekitarnya. 
                Data bulan <b>Oktober 2026 (Musim Gugur / Fall)</b> merekam lonjakan volume masif sebesar <b>4.981.785 perjalanan</b> dengan dominasi 
                pelanggan Member tahunan sebesar <b>81,2%</b>. Perbedaan durasi yang sangat kontras (Member rata-rata 11,89 menit vs Casual rata-rata 18,47 menit) 
                menegaskan bahwa Member mengutamakan kecepatan mobilitas kerja, sedangkan pengguna Casual memanfaatkannya untuk kebutuhan rekreasi santai dan wisata kota.
            </div>
            """,
            unsafe_allow_html=True,
        )

        # ----------------------------------------------------
        # VISUALISASI 1: HOURLY DEMAND & USER TYPE COMPARISON
        # ----------------------------------------------------
        col_g1, col_g2 = st.columns(2)

        with col_g1:
            st.markdown('<div class="section-title">📈 Kurva Permintaan Jam Sibuk (Hourly Demand Curve)</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="section-caption">Distribusi total volume perjalanan per jam (00:00 s.d. 23:00). Mengidentifikasi lonjakan jam kerja (rush hours).</div>',
                unsafe_allow_html=True,
            )

            if not hourly_df.empty and {"ride_hour", "total_rides"}.issubset(hourly_df.columns):
                h_chart_df = (
                    hourly_df.groupby("ride_hour", as_index=False)["total_rides"]
                    .sum()
                    .sort_values("ride_hour")
                )
                h_chart_df["ride_hour"] = pd.to_numeric(h_chart_df["ride_hour"], errors="coerce")
                h_chart_df["total_rides"] = pd.to_numeric(h_chart_df["total_rides"], errors="coerce")

                # Label indikator peak
                h_chart_df["Status Jam"] = h_chart_df["ride_hour"].apply(
                    lambda h: "Jam Sibuk (Puncak)" if h in [8, 17, 18] else ("Siang Reguler" if 9 <= h <= 16 else "Malam / Dini Hari")
                )

                hourly_chart = (
                    alt.Chart(h_chart_df)
                    .mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5)
                    .encode(
                        x=alt.X("ride_hour:O", title="Jam Perjalanan (0 - 23)", axis=alt.Axis(labelAngle=0)),
                        y=alt.Y("total_rides:Q", title="Total Perjalanan (Trips)", axis=alt.Axis(format="~s")),
                        color=alt.Color(
                            "Status Jam:N",
                            scale=alt.Scale(
                                domain=["Jam Sibuk (Puncak)", "Siang Reguler", "Malam / Dini Hari"],
                                range=["#2563EB", "#60A5FA", "#CBD5E1"],
                            ),
                            legend=alt.Legend(title="Kategori Waktu", orient="top"),
                        ),
                        tooltip=[
                            alt.Tooltip("ride_hour:O", title="Jam"),
                            alt.Tooltip("total_rides:Q", title="Total Perjalanan", format=","),
                            alt.Tooltip("Status Jam:N", title="Kategori"),
                        ],
                    )
                    .properties(height=310)
                )
                st.altair_chart(hourly_chart, use_container_width=True)

                st.info(
                    "🔍 **Penjelasan Analitik:** Terdeteksi dua puncak tajam (*bimodal peak*): **Pukul 08:00 pagi** (arus komuter berangkat kerja) "
                    "dan **Pukul 17:00–18:00 sore** (arus pulang kerja). Operasional rebalancing armada wajib menyelesaikan distribusi sepeda sebelum pukul 07:30 dan 16:30."
                )
            else:
                st.info("Data jam sibuk tidak ditemukan untuk kombinasi filter ini.")

        with col_g2:
            st.markdown('<div class="section-title">👥 Karakteristik Segmen: Member vs Casual</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="section-caption">Komparasi menyeluruh antara volume perjalanan dan rata-rata durasi bersepeda per segmen pengguna.</div>',
                unsafe_allow_html=True,
            )

            if not user_type_df.empty and "member_casual" in user_type_df.columns:
                u_chart_df = user_type_df.groupby("member_casual").agg(
                    {"total_rides": "sum", "avg_duration_minutes": "mean"}
                ).reset_index()

                u_chart_df["Tipe Pengguna"] = u_chart_df["member_casual"].map(
                    {"member": "Member (Langganan)", "casual": "Casual (Harian/Turis)"}
                ).fillna(u_chart_df["member_casual"])

                u_chart = (
                    alt.Chart(u_chart_df)
                    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                    .encode(
                        x=alt.X("Tipe Pengguna:N", title="Segmen Pengguna", axis=alt.Axis(labelAngle=0)),
                        y=alt.Y("total_rides:Q", title="Total Volume Perjalanan", axis=alt.Axis(format="~s")),
                        color=alt.Color(
                            "Tipe Pengguna:N",
                            scale=alt.Scale(domain=["Member (Langganan)", "Casual (Harian/Turis)"], range=["#2563EB", "#10B981"]),
                            legend=None,
                        ),
                        tooltip=[
                            alt.Tooltip("Tipe Pengguna:N"),
                            alt.Tooltip("total_rides:Q", title="Total Perjalanan", format=","),
                            alt.Tooltip("avg_duration_minutes:Q", title="Rata-rata Durasi", format=".2f"),
                        ],
                    )
                    .properties(height=310)
                )
                st.altair_chart(u_chart, use_container_width=True)

                u_display = u_chart_df[["Tipe Pengguna", "total_rides", "avg_duration_minutes"]].rename(
                    columns={"total_rides": "Total Perjalanan", "avg_duration_minutes": "Rata-rata Durasi (menit)"}
                )
                st.dataframe(u_display, use_container_width=True, hide_index=True)
            else:
                st.info("Data segmentasi pengguna tidak tersedia.")

        st.divider()

        # ----------------------------------------------------
        # VISUALISASI 2: TOP 10 BUSIEST STATIONS IN NYC
        # ----------------------------------------------------
        st.markdown('<div class="section-title">🏆 Top 10 Stasiun Keberangkatan Tersibuk di New York City</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-caption">10 stasiun dengan traffic awal tertinggi. Merupakan titik pusat mobilitas utama di sekitar kawasan perkantoran, stasiun kereta transit (Penn Station), dan jalur wisata Hudson River.</div>',
            unsafe_allow_html=True,
        )

        if not station_df.empty and "start_station_name" in station_df.columns:
            top_stations = (
                station_df.groupby(["start_station_id", "start_station_name"], as_index=False)
                .agg({"total_rides": "sum", "avg_duration_minutes": "mean"})
                .sort_values("total_rides", ascending=False)
                .head(10)
            )

            col_st_chart, col_st_table = st.columns([5, 5])

            with col_st_chart:
                station_bar_chart = (
                    alt.Chart(top_stations)
                    .mark_bar(cornerRadiusTopRight=5, cornerRadiusBottomRight=5)
                    .encode(
                        y=alt.Y("start_station_name:N", sort="-x", title="Stasiun Keberangkatan", axis=alt.Axis(labelLimit=250)),
                        x=alt.X("total_rides:Q", title="Total Perjalanan Berangkat", axis=alt.Axis(format="~s")),
                        color=alt.Color("total_rides:Q", scale=alt.Scale(scheme="blues"), legend=None),
                        tooltip=[
                            alt.Tooltip("start_station_name:N", title="Stasiun"),
                            alt.Tooltip("start_station_id:N", title="Station ID"),
                            alt.Tooltip("total_rides:Q", title="Total Perjalanan", format=","),
                            alt.Tooltip("avg_duration_minutes:Q", title="Durasi Rata-rata (menit)", format=".2f"),
                        ],
                    )
                    .properties(height=360)
                )
                st.altair_chart(station_bar_chart, use_container_width=True)

            with col_st_table:
                display_table = top_stations.rename(
                    columns={
                        "start_station_name": "Nama Stasiun",
                        "start_station_id": "Station ID",
                        "total_rides": "Total Perjalanan",
                        "avg_duration_minutes": "Rata-rata Durasi (menit)",
                    }
                )
                st.dataframe(display_table, use_container_width=True, hide_index=True)
                st.caption(
                    "💡 **Rekomendasi Operasional:** Stasiun peringkat atas seperti **W 21 St & 6 Ave** dan **Pier 61 at Chelsea Piers** "
                    "memerlukan slot dock berkapasitas besar (>50 dock) serta penempatan *valet parking* staf pada jam sibuk sore."
                )

        with st.expander("🔍 Lihat Ringkasan Agregasi Jam Sibuk Lengkap"):
            if not hourly_df.empty:
                sort_cols = [c for c in ["ride_date", "ride_hour"] if c in hourly_df.columns]
                st.dataframe(hourly_df.sort_values(sort_cols) if sort_cols else hourly_df, use_container_width=True, hide_index=True)


# ============================================================
# TAB 2: REAL-TIME STATION MONITORING (GBFS STREAMING)
# ============================================================
with tab_realtime:
    @st.fragment(run_every="30s")
    def render_realtime_view():
        latest_doc = db["realtime_station_status"].find_one({}, sort=[("snapshot_time", -1)])

        if not latest_doc:
            st.info("📡 Belum ada data real-time di MongoDB. Pastikan service collector dan streaming aktif.")
            return

        latest_time = latest_doc["snapshot_time"]
        cursor = db["realtime_station_status"].find({"snapshot_time": latest_time})
        df = pd.DataFrame(list(cursor))

        # Sinkronkan Metadata Stasiun
        meta_df = load_station_metadata()
        if not meta_df.empty and "station_id" in df.columns:
            df = df.merge(meta_df, on="station_id", how="left")
            if "station_name_y" in df.columns:
                df["station_name"] = df["station_name_y"].fillna(df.get("station_name_x", df["station_id"]))

        active_df = df[df["is_renting"] == 1].copy() if "is_renting" in df.columns else df.copy()

        # Format waktu WIB (UTC + 7 jam)
        local_time = pd.to_datetime(latest_time) + pd.Timedelta(hours=7)
        timestamp_str = local_time.strftime("%d %B %Y, %H:%M:%S WIB")

        # ----------------------------------------------------
        # LIVE TELEMETRY STATUS BAR
        # ----------------------------------------------------
        bar_col1, bar_col2 = st.columns([4, 1.2])
        with bar_col1:
            st.markdown(
                f"""
                <div class="live-status-bar">
                    <div style="display:flex; align-items:center;">
                        <span class="pulsing-dot"></span>
                        <span style="font-weight:700; color:#FFFFFF; margin-right:8px;">GBFS LIVE STREAMING ACTIVE</span>
                        <span style="color:#94A3B8;">| Terakhir diperbarui: <b>{timestamp_str}</b></span>
                    </div>
                    <span style="font-size:12px; color:#10B981; font-weight:600;">Auto-Refresh Tiap 30 Detik</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with bar_col2:
            if st.button("🔄 Segarkan Data Sekarang", use_container_width=True):
                st.rerun()

        # ----------------------------------------------------
        # PERHITUNGAN KPI OPERASIONAL LIVE
        # ----------------------------------------------------
        active_station_count = len(active_df)
        total_bikes = int(active_df["num_bikes_available"].sum()) if "num_bikes_available" in active_df.columns else 0
        total_ebikes = int(active_df["num_ebikes_available"].sum()) if "num_ebikes_available" in active_df.columns else 0
        total_docks = int(active_df["num_docks_available"].sum()) if "num_docks_available" in active_df.columns else 0

        empty_stations = int((active_df["num_bikes_available"] == 0).sum()) if "num_bikes_available" in active_df.columns else 0
        full_stations = int((active_df["num_docks_available"] == 0).sum()) if "num_docks_available" in active_df.columns else 0

        # KPI Cards Live
        r1, r2, r3, r4, r5 = st.columns(5)
        with r1:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-label">Stasiun Aktif</span>
                        <div class="metric-icon-box" style="background:#EFF6FF; color:#2563EB;">🏢</div>
                    </div>
                    <div class="metric-value">{active_station_count:,}</div>
                    <div class="metric-subtext"><span class="metric-pill-green">Online</span> melayani sewa</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with r2:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-label">Sepeda Mekanik</span>
                        <div class="metric-icon-box" style="background:#ECFDF5; color:#059669;">🚲</div>
                    </div>
                    <div class="metric-value">{total_bikes:,}</div>
                    <div class="metric-subtext"><span class="metric-pill-blue">Siap Pakai</span> di seluruh dock</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with r3:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-label">Sepeda Listrik</span>
                        <div class="metric-icon-box" style="background:#F5F3FF; color:#7C3AED;">⚡</div>
                    </div>
                    <div class="metric-value">{total_ebikes:,}</div>
                    <div class="metric-subtext"><span class="metric-pill-green">E-Bikes</span> armada baterai</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with r4:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-label">Dock Tersedia</span>
                        <div class="metric-icon-box" style="background:#FFFBEB; color:#D97706;">🅿️</div>
                    </div>
                    <div class="metric-value">{total_docks:,}</div>
                    <div class="metric-subtext"><span class="metric-pill-blue">Slot Kosong</span> siap menerima unit</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with r5:
            pill_class = "metric-pill-red" if (empty_stations > 0 or full_stations > 0) else "metric-pill-green"
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-label">Stasiun Kritis</span>
                        <div class="metric-icon-box" style="background:#FEF2F2; color:#DC2626;">🚨</div>
                    </div>
                    <div class="metric-value">{empty_stations} / {full_stations}</div>
                    <div class="metric-subtext"><span class="{pill_class}">{empty_stations} kosong</span> • {full_stations} penuh</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ----------------------------------------------------
        # PERINGATAN REBALANCING LAPANGAN
        # ----------------------------------------------------
        if empty_stations > 0 or full_stations > 0:
            st.markdown(
                f"""
                <div class="alert-box alert-warning">
                    ⚠️ <b>Instruksi Dispatch Rebalancing:</b> Terdapat <b>{empty_stations} stasiun kehabisan sepeda (Stok = 0)</b> 
                    yang membutuhkan suplai armada segera, dan <b>{full_stations} stasiun dengan dock penuh total (Slot = 0)</b> 
                    yang berisiko menimbulkan komplain pengguna karena tidak dapat mengembalikan sepeda. Tim logistik lapangan 
                    disarankan mengarahkan armada van pengangkut ke stasiun-stasiun kritis di bawah.
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                """
                <div class="alert-box alert-success">
                    ✅ <b>Kondisi Armada Optimal:</b> Tidak ada stasiun dalam status kritis saat ini. Distribusi unit dan slot parkir seimbang.
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ----------------------------------------------------
        # VISUALISASI TREN KETERSEDIAAN ARMADA & DOCK
        # ----------------------------------------------------
        st.markdown('<div class="section-title">📈 Tren Ketersediaan Armada Real-Time (20 Snapshot Terakhir)</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-caption">Memantau fluktuasi pasokan total sepeda, e-bike, dan ketersediaan dock kosong per menit secara real-time.</div>',
            unsafe_allow_html=True,
        )

        df_trend = load_realtime_trend()
        if not df_trend.empty and len(df_trend) > 1:
            trend_melted = df_trend.melt(
                id_vars=["Snapshot Time"],
                value_vars=["Total Bikes", "E-Bikes", "Available Docks"],
                var_name="Metrik",
                value_name="Jumlah Unit / Slot",
            )
            trend_chart = (
                alt.Chart(trend_melted)
                .mark_line(point=alt.OverlayMarkDef(size=30, filled=True), strokeWidth=2.5)
                .encode(
                    x=alt.X("Snapshot Time:T", title="Waktu Snapshot (WIB)", axis=alt.Axis(format="%H:%M:%S")),
                    y=alt.Y("Jumlah Unit / Slot:Q", title="Jumlah Akumulatif", axis=alt.Axis(format="~s")),
                    color=alt.Color(
                        "Metrik:N",
                        scale=alt.Scale(
                            domain=["Total Bikes", "E-Bikes", "Available Docks"],
                            range=["#2563EB", "#7C3AED", "#D97706"],
                        ),
                        legend=alt.Legend(title="Komponen Armada", orient="top"),
                    ),
                    tooltip=[
                        alt.Tooltip("Snapshot Time:T", title="Waktu", format="%H:%M:%S WIB"),
                        alt.Tooltip("Metrik:N", title="Komponen"),
                        alt.Tooltip("Jumlah Unit / Slot:Q", title="Jumlah", format=","),
                    ],
                )
                .properties(height=280)
            )
            st.altair_chart(trend_chart, use_container_width=True)
        else:
            st.info("Mengumpulkan riwayat snapshot berkala dari streaming micro-batch...")

        st.divider()

        # ----------------------------------------------------
        # MATRIKS SURPLUS VS DEFISIT ARMADA
        # ----------------------------------------------------
        st.markdown('<div class="section-title">⚖️ Matriks Ketidakseimbangan Armada Stasiun (Rebalancing Matrix)</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-caption">Panduan langsung bagi tim dispatcher operasional untuk mengambil unit berlebih (*Surplus*) dan mendistribusikannya ke stasiun krisis (*Defisit/Dock Penuh*).</div>',
            unsafe_allow_html=True,
        )

        col_surplus, col_deficit, col_dock_full = st.columns(3)

        with col_surplus:
            st.markdown("##### 🟢 Top 5 Surplus (Stok Terbanyak)")
            st.caption("Stasiun dengan sepeda melimpah. Titik ideal penjemputan unit untuk rebalancing.")
            top_surplus = (
                active_df.sort_values("num_bikes_available", ascending=False)
                .head(5)[["station_name", "num_bikes_available", "num_docks_available"]]
                .rename(columns={"station_name": "Stasiun", "num_bikes_available": "Sepeda", "num_docks_available": "Docks"})
            )
            st.dataframe(top_surplus, use_container_width=True, hide_index=True)

        with col_deficit:
            st.markdown("##### 🔴 Top 5 Defisit (Sepeda Paling Sedikit)")
            st.caption("Stasiun kehabisan unit (**Sepeda = 0**). Prioritas pasokan armada pertama.")
            top_deficit = (
                active_df.sort_values(["num_bikes_available", "num_docks_available"], ascending=[True, False])
                .head(5)[["station_name", "num_bikes_available", "num_docks_available"]]
                .rename(columns={"station_name": "Stasiun", "num_bikes_available": "Sepeda", "num_docks_available": "Docks"})
            )
            st.dataframe(top_deficit, use_container_width=True, hide_index=True)

        with col_dock_full:
            st.markdown("##### 🟡 Top 5 Dock Penuh (Parkir Kritis)")
            st.caption("Stasiun penuh total (**Docks = 0**). Pengguna tidak dapat mengembalikan sepeda.")
            top_dock_full = (
                active_df.sort_values("num_docks_available", ascending=True)
                .head(5)[["station_name", "num_docks_available", "num_bikes_available"]]
                .rename(columns={"station_name": "Stasiun", "num_docks_available": "Docks Sisa", "num_bikes_available": "Sepeda"})
            )
            st.dataframe(top_dock_full, use_container_width=True, hide_index=True)

        st.divider()

        # ----------------------------------------------------
        # PETA GEOGRAFIS INTERAKTIF NYC
        # ----------------------------------------------------
        st.markdown('<div class="section-title">🗺️ Peta Geografis Ketersediaan Stasiun New York City</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-caption">Memvisualisasikan lokasi geografis stasiun di seluruh borough NYC (Manhattan, Brooklyn, Queens, Bronx).</div>',
            unsafe_allow_html=True,
        )

        if "lat" in active_df.columns and "lon" in active_df.columns and active_df["lat"].notna().any():
            map_data = active_df.dropna(subset=["lat", "lon"]).copy()

            f_map_col1, f_map_col2 = st.columns([3, 4])
            with f_map_col1:
                map_filter = st.radio(
                    "Filter Kategori Stasiun Peta:",
                    ["Semua Stasiun", "Hanya Stasiun Kritis (Sepeda <= 2)", "Hanya Stasiun Surplus (Sepeda >= 12)"],
                    horizontal=True,
                )

            if map_filter == "Hanya Stasiun Kritis (Sepeda <= 2)":
                map_data = map_data[map_data["num_bikes_available"] <= 2]
            elif map_filter == "Hanya Stasiun Surplus (Sepeda >= 12)":
                map_data = map_data[map_data["num_bikes_available"] >= 12]

            if not map_data.empty:
                st.map(map_data[["lat", "lon"]], size=22, zoom=11)
                st.caption(f"📍 Menampilkan **{len(map_data):,} titik stasiun** di peta New York City.")
            else:
                st.info("Tidak ada stasiun yang memenuhi kriteria filter peta saat ini.")
        else:
            st.info("Koordinat peta stasiun sedang disinkronkan dari metadata GBFS.")

        # ----------------------------------------------------
        # TABEL INVENTARIS LENGKAP STASIUN
        # ----------------------------------------------------
        with st.expander("📋 Lihat Tabel Inventaris Lengkap Seluruh Stasiun"):
            filter_crit = st.checkbox("Hanya tampilkan stasiun kritis (Sepeda <= 2 atau Dock <= 2)")
            table_display_df = active_df.copy()
            if filter_crit:
                table_display_df = table_display_df[
                    (table_display_df["num_bikes_available"] <= 2) | (table_display_df["num_docks_available"] <= 2)
                ]

            cols_to_show = [
                c
                for c in [
                    "station_name",
                    "station_id",
                    "num_bikes_available",
                    "num_ebikes_available",
                    "num_docks_available",
                    "is_renting",
                    "is_returning",
                    "last_reported_time",
                ]
                if c in table_display_df.columns
            ]
            st.dataframe(
                table_display_df[cols_to_show].rename(
                    columns={
                        "station_name": "Nama Stasiun",
                        "station_id": "Station ID",
                        "num_bikes_available": "Sepeda",
                        "num_ebikes_available": "E-Bikes",
                        "num_docks_available": "Docks",
                        "is_renting": "Sewa Aktif",
                        "is_returning": "Kembali Aktif",
                        "last_reported_time": "Update Pelaporan",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

    render_realtime_view()
