import os
from urllib.parse import quote

import altair as alt
import pandas as pd
import requests
import streamlit as st
from pymongo import MongoClient


# ============================================================
# 1. PAGE CONFIGURATION
# ============================================================
st.set_page_config(
    page_title="Citi Bike NYC · Enterprise Mobility Analytics",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# 2. DESIGN SYSTEM & MODERN EXECUTIVE CSS
# ============================================================
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Inter:wght@400;500;600;700&display=swap');

    /* Global Typography */
    html, body, [class*="css"], .stApp {
        font-family: 'Plus Jakarta Sans', 'Inter', -apple-system, sans-serif;
        color: #0F172A;
    }

    /* Top Hero Header */
    .hero-container {
        background: linear-gradient(135deg, #0F172A 0%, #1E293B 60%, #0F52BA 100%);
        border-radius: 16px;
        padding: 24px 32px;
        color: #FFFFFF;
        margin-bottom: 24px;
        box-shadow: 0 10px 25px -5px rgba(15, 23, 42, 0.15);
        border: 1px solid rgba(255, 255, 255, 0.08);
        display: flex;
        justify-content: space-between;
        align-items: center;
    }

    .hero-title {
        font-size: 26px;
        font-weight: 800;
        letter-spacing: -0.6px;
        margin: 0;
        display: flex;
        align-items: center;
        gap: 12px;
        color: #F8FAFC;
    }

    .hero-desc {
        font-size: 13.5px;
        color: #94A3B8;
        margin: 6px 0 0 0;
        font-weight: 400;
        max-width: 820px;
        line-height: 1.5;
    }

    .hero-tag {
        background: rgba(255, 255, 255, 0.12);
        border: 1px solid rgba(255, 255, 255, 0.18);
        border-radius: 20px;
        padding: 6px 14px;
        font-size: 11.5px;
        font-weight: 600;
        letter-spacing: 0.3px;
        color: #E2E8F0;
        white-space: nowrap;
    }

    /* Executive Metric Cards */
    .metric-box {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 14px;
        padding: 20px 22px;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.04);
        transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1);
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }

    .metric-box:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 18px -3px rgba(15, 23, 42, 0.08);
        border-color: #CBD5E1;
    }

    .metric-top {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 10px;
    }

    .metric-title {
        font-size: 11px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: #64748B;
    }

    .metric-badge-icon {
        width: 32px;
        height: 32px;
        border-radius: 9px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 15px;
    }

    .metric-num {
        font-size: 27px;
        font-weight: 800;
        letter-spacing: -0.6px;
        color: #0F172A;
        line-height: 1.1;
        margin-bottom: 6px;
    }

    .metric-footer {
        font-size: 12px;
        color: #64748B;
        font-weight: 500;
        display: flex;
        align-items: center;
        gap: 6px;
    }

    /* Pill Badges */
    .pill-green {
        background: #ECFDF5;
        color: #047857;
        font-weight: 700;
        padding: 2px 7px;
        border-radius: 6px;
        font-size: 11px;
    }

    .pill-blue {
        background: #EFF6FF;
        color: #1D4ED8;
        font-weight: 700;
        padding: 2px 7px;
        border-radius: 6px;
        font-size: 11px;
    }

    .pill-purple {
        background: #FAF5FF;
        color: #6B21A8;
        font-weight: 700;
        padding: 2px 7px;
        border-radius: 6px;
        font-size: 11px;
    }

    .pill-amber {
        background: #FFFBEB;
        color: #B45309;
        font-weight: 700;
        padding: 2px 7px;
        border-radius: 6px;
        font-size: 11px;
    }

    .pill-red {
        background: #FEF2F2;
        color: #B91C1C;
        font-weight: 700;
        padding: 2px 7px;
        border-radius: 6px;
        font-size: 11px;
    }

    /* Executive Callout Note */
    .executive-card {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-left: 4px solid #2563EB;
        padding: 16px 20px;
        border-radius: 10px;
        margin: 20px 0 24px 0;
        font-size: 13.5px;
        color: #334155;
        line-height: 1.6;
    }

    /* Live Telemetry Status Bar */
    .telemetry-bar {
        background: #0F172A;
        border: 1px solid #1E293B;
        border-radius: 12px;
        padding: 12px 20px;
        margin-bottom: 20px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        color: #F8FAFC;
    }

    .pulse-dot {
        display: inline-block;
        width: 9px;
        height: 9px;
        background-color: #10B981;
        border-radius: 50%;
        margin-right: 8px;
        box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
        animation: pulseAnimation 1.6s infinite;
    }

    @keyframes pulseAnimation {
        0% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
        70% { box-shadow: 0 0 0 7px rgba(16, 185, 129, 0); }
        100% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }

    /* Sidebar Clean Card */
    .sidebar-card {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 14px 16px;
        margin-bottom: 16px;
    }

    .sidebar-header {
        font-size: 11px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.6px;
        color: #64748B;
        margin-bottom: 10px;
    }

    .sidebar-item {
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 12px;
        padding: 5px 0;
        border-bottom: 1px solid #EDF2F7;
    }

    .sidebar-item:last-child {
        border-bottom: none;
    }

    .sidebar-name {
        font-weight: 500;
        color: #334155;
    }

    .sidebar-status {
        font-weight: 600;
        font-size: 11px;
    }

    /* Section Subheaders */
    .chart-header {
        font-size: 16.5px;
        font-weight: 700;
        color: #0F172A;
        letter-spacing: -0.3px;
        margin-bottom: 3px;
    }

    .chart-caption {
        font-size: 12.5px;
        color: #64748B;
        margin-bottom: 12px;
        line-height: 1.4;
    }

    /* Alert Boxes */
    .callout-alert {
        border-radius: 10px;
        padding: 13px 18px;
        font-size: 13px;
        margin-bottom: 18px;
        line-height: 1.5;
    }
    .callout-warning {
        background-color: #FFFBEB;
        border: 1px solid #FDE68A;
        border-left: 4px solid #F59E0B;
        color: #92400E;
    }
    .callout-success {
        background-color: #ECFDF5;
        border: 1px solid #A7F3D0;
        border-left: 4px solid #10B981;
        color: #065F46;
    }

    /* Clean Streamlit Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #F1F5F9;
        padding: 5px 6px;
        border-radius: 12px;
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 18px;
        font-weight: 600;
        font-size: 13.5px;
        color: #64748B;
        border: none;
        background-color: transparent;
    }

    .stTabs [aria-selected="true"] {
        background-color: #FFFFFF !important;
        color: #0F172A !important;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
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
# 4. CACHED DATA LOADERS
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
    """Mengambil koordinat dan nama stasiun dari feed resmi GBFS Lyft."""
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
    """Mengambil riwayat snapshot tren ketersediaan armada dari MongoDB."""
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
            # Konversi UTC ke WIB (+7 jam)
            df_trend["Snapshot Time"] = pd.to_datetime(df_trend["Snapshot Time"]) + pd.Timedelta(hours=7)
            return df_trend.sort_values("Snapshot Time")
    except Exception:
        pass
    return pd.DataFrame()


# ============================================================
# 5. SIDEBAR: MINIMALIST & PROFESSIONAL
# ============================================================
with st.sidebar:
    st.markdown(
        """
        <div style="text-align: center; padding: 10px 0 16px 0;">
            <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/0/09/Citi_Bike_logo.svg/320px-Citi_Bike_logo.svg.png" width="130" style="margin-bottom: 8px;">
            <div style="font-size: 15px; font-weight: 800; color: #0F172A; letter-spacing: -0.3px;">Citi Bike Big Data</div>
            <div style="font-size: 11px; font-weight: 600; color: #64748B;">Enterprise Analytics Platform</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="sidebar-card">
            <div class="sidebar-header">Arsitektur Pipeline</div>
            <div class="sidebar-item">
                <span class="sidebar-name">Lakehouse</span>
                <span class="sidebar-status" style="color:#059669;">MinIO S3</span>
            </div>
            <div class="sidebar-item">
                <span class="sidebar-name">Event Buffer</span>
                <span class="sidebar-status" style="color:#059669;">Kafka 3.8</span>
            </div>
            <div class="sidebar-item">
                <span class="sidebar-name">Compute Engine</span>
                <span class="sidebar-status" style="color:#2563EB;">Spark 3.5</span>
            </div>
            <div class="sidebar-item">
                <span class="sidebar-name">Serving Store</span>
                <span class="sidebar-status" style="color:#059669;">MongoDB 7</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="sidebar-card">
            <div class="sidebar-header">Status Operasional</div>
            <div class="sidebar-item">
                <span class="sidebar-name">Stream Topic</span>
                <span class="sidebar-status" style="color:#2563EB;">gbfs-status</span>
            </div>
            <div class="sidebar-item">
                <span class="sidebar-name">Batch Scale</span>
                <span class="sidebar-status" style="color:#6B21A8;">4.98M Trips</span>
            </div>
            <div class="sidebar-item">
                <span class="sidebar-name">Serving Latency</span>
                <span class="sidebar-status" style="color:#059669;">Sub-detik</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.caption("Citi Bike NYC Analytics · Production v2.1")


# ============================================================
# 6. EXECUTIVE HERO BANNER
# ============================================================
st.markdown(
    """
    <div class="hero-container">
        <div>
            <h1 class="hero-title">🚲 Citi Bike NYC · Enterprise Mobility Analytics</h1>
            <p class="hero-desc">
                Platform intelijen data terpadu untuk mobilitas perkotaan New York City. Mengintegrasikan 
                <b>Batch Historical Lakehouse</b> (skala jutaan perjalanan MinIO S3 + Apache Spark) dengan 
                <b>Real-Time Stream Monitoring</b> (event buffer Apache Kafka + GBFS streaming telemetri armada).
            </p>
        </div>
        <div class="hero-tag">
            ⚡ Spark 3.5 + Kafka + MinIO S3
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# 7. FIXED 2-TAB DASHBOARD
# ============================================================
tab_batch, tab_realtime = st.tabs(
    [
        "📊 Batch Historical Lakehouse Analytics",
        "⚡ Real-Time Station Monitoring (Kafka Stream)",
    ]
)


# ============================================================
# TAB 1: BATCH HISTORICAL LAKEHOUSE ANALYTICS
# ============================================================
with tab_batch:
    raw_hourly_df = load_batch_hourly()
    raw_user_type_df = load_batch_user_type()
    raw_station_df = load_batch_stations()

    if raw_hourly_df.empty and raw_user_type_df.empty and raw_station_df.empty:
        st.warning("⚠️ Data batch MinIO belum tersedia di MongoDB. Silakan jalankan batch pipeline terlebih dahulu.")
    else:
        # ----------------------------------------------------
        # FILTER CONTAINER
        # ----------------------------------------------------
        col_f1, col_f2, col_f3 = st.columns([2.5, 2.5, 3])

        available_months = ["Semua Periode (Januari & Oktober 2026)"]
        if not raw_user_type_df.empty and "month_label" in raw_user_type_df.columns:
            unique_months = sorted(raw_user_type_df["month_label"].dropna().unique().tolist(), reverse=True)
            available_months.extend(unique_months)

        with col_f1:
            selected_month = st.selectbox(
                "📅 Periode Data Perjalanan:",
                available_months,
                index=0,
                help="Bandingkan data antara Musim Gugur (Oktober ~4.98 Juta Trips) vs Musim Dingin (Januari).",
            )

        with col_f2:
            selected_user_type = st.selectbox(
                "👥 Segmentasi Pengguna:",
                ["Semua Pengguna (Member & Casual)", "Member Tahunan (Komuter Reguler)", "Casual (Harian / Wisatawan)"],
                index=0,
                help="Saring analisis berdasarkan tipe pelanggan Citi Bike.",
            )

        with col_f3:
            st.write("")
            st.caption("💡 *Filter secara dinamis memutakhirkan metrik KPI, kurva jam sibuk, dan top 10 stasiun NYC.*")

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

        # Perhitungan Metrik KPI
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
        # 4 KPI EXECUTIVE CARDS
        # ----------------------------------------------------
        k1, k2, k3, k4 = st.columns(4)

        with k1:
            st.markdown(
                f"""
                <div class="metric-box">
                    <div class="metric-top">
                        <span class="metric-title">Total Perjalanan Valid</span>
                        <div class="metric-badge-icon" style="background:#EFF6FF; color:#2563EB;">🚲</div>
                    </div>
                    <div class="metric-num">{total_rides:,}</div>
                    <div class="metric-footer">
                        <span class="pill-green">Quality Gate Lolos</span>
                        <span>MinIO Curated</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with k2:
            st.markdown(
                f"""
                <div class="metric-box">
                    <div class="metric-top">
                        <span class="metric-title">Rata-Rata Durasi</span>
                        <div class="metric-badge-icon" style="background:#ECFDF5; color:#059669;">⏱️</div>
                    </div>
                    <div class="metric-num">{weighted_avg_duration:.2f} <span style="font-size:16px; font-weight:600; color:#64748B;">menit</span></div>
                    <div class="metric-footer">
                        <span class="pill-blue">Perjalanan Efisien</span>
                        <span>waktu tempuh riil</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with k3:
            st.markdown(
                f"""
                <div class="metric-box">
                    <div class="metric-top">
                        <span class="metric-title">Rasio Member Tahunan</span>
                        <div class="metric-badge-icon" style="background:#FAF5FF; color:#7C3AED;">💳</div>
                    </div>
                    <div class="metric-num">{member_ratio:.1f}%</div>
                    <div class="metric-footer">
                        <span class="pill-purple">{member_count:,} trips</span>
                        <span>komuter harian aktif</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with k4:
            clean_station_display = (top_station_name[:20] + "...") if len(top_station_name) > 20 else top_station_name
            st.markdown(
                f"""
                <div class="metric-box">
                    <div class="metric-top">
                        <span class="metric-title">Stasiun Terpopuler</span>
                        <div class="metric-badge-icon" style="background:#FFFBEB; color:#D97706;">📍</div>
                    </div>
                    <div class="metric-num" style="font-size:19px; line-height:1.25;" title="{top_station_name}">{clean_station_display}</div>
                    <div class="metric-footer">
                        <span class="pill-amber">{top_station_rides:,} trips</span>
                        <span>titik awal tersibuk</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # Executive Insight Note
        st.markdown(
            """
            <div class="executive-card">
                <b>📌 Wawasan Strategis Mobilitas Perkotaan (Seasonal & Behavioral Insight):</b><br/>
                Data Lakehouse membuktikan bahwa sistem Citi Bike adalah urat nadi transportasi New York City. 
                Data bulan <b>Oktober 2026 (Musim Gugur / Fall)</b> mencatat rekor mobilitas masif sebesar <b>4.981.785 perjalanan</b>, 
                di mana <b>81,2% didominasi pengguna Member</b> dengan durasi rata-rata singkat <b>11,89 menit</b> (komuter kerja point-to-point). 
                Sebaliknya, pengguna Casual bersepeda 55% lebih lama dengan rata-rata <b>18,47 menit</b> (rekreasi akhir pekan di Central Park & Hudson River Greenway).
            </div>
            """,
            unsafe_allow_html=True,
        )

        # ----------------------------------------------------
        # VISUALISASI: HOURLY DEMAND & USER TYPE
        # ----------------------------------------------------
        g_col1, g_col2 = st.columns(2)

        with g_col1:
            st.markdown('<div class="chart-header">📈 Kurva Distribusi Jam Sibuk (Rush Hour Curve)</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-caption">Volume perjalanan dalam format 24 jam. Mengidentifikasi lonjakan jam berangkat dan pulang kantor.</div>',
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

                h_chart_df["Kategori Jam"] = h_chart_df["ride_hour"].apply(
                    lambda h: "Jam Sibuk (Puncak)" if h in [8, 17, 18] else ("Jam Kerja Reguler" if 9 <= h <= 16 else "Malam / Dini Hari")
                )

                hourly_chart = (
                    alt.Chart(h_chart_df)
                    .mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5)
                    .encode(
                        x=alt.X("ride_hour:O", title="Jam Perjalanan (00:00 - 23:00)", axis=alt.Axis(labelAngle=0)),
                        y=alt.Y("total_rides:Q", title="Total Perjalanan", axis=alt.Axis(format="~s")),
                        color=alt.Color(
                            "Kategori Jam:N",
                            scale=alt.Scale(
                                domain=["Jam Sibuk (Puncak)", "Jam Kerja Reguler", "Malam / Dini Hari"],
                                range=["#2563EB", "#93C5FD", "#E2E8F0"],
                            ),
                            legend=alt.Legend(title=None, orient="top"),
                        ),
                        tooltip=[
                            alt.Tooltip("ride_hour:O", title="Jam"),
                            alt.Tooltip("total_rides:Q", title="Total Trips", format=","),
                            alt.Tooltip("Kategori Jam:N", title="Kategori"),
                        ],
                    )
                    .properties(height=290)
                )
                st.altair_chart(hourly_chart, use_container_width=True)

                st.caption(
                    "🔍 **Pola Bimodal:** Terlihat 2 lonjakan tajam: **Pukul 08:00 pagi** (arus komuter pagi) "
                    "dan **Pukul 17:00–18:00 sore** (arus pulang kerja). Tim operasional wajib menyelesaikan rebalancing armada sebelum pukul 07:30 dan 16:30."
                )
            else:
                st.info("Data jam sibuk tidak tersedia untuk filter ini.")

        with g_col2:
            st.markdown('<div class="chart-header">👥 Segmentasi Pengguna (Member vs Casual)</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-caption">Komparasi menyeluruh antara volume perjalanan dan rata-rata durasi bersepeda per segmen.</div>',
                unsafe_allow_html=True,
            )

            if not user_type_df.empty and "member_casual" in user_type_df.columns:
                u_chart_df = user_type_df.groupby("member_casual").agg(
                    {"total_rides": "sum", "avg_duration_minutes": "mean"}
                ).reset_index()

                u_chart_df["Segmen"] = u_chart_df["member_casual"].map(
                    {"member": "Member (Langganan Tahunan)", "casual": "Casual (Harian / Turis)"}
                ).fillna(u_chart_df["member_casual"])

                u_chart = (
                    alt.Chart(u_chart_df)
                    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                    .encode(
                        x=alt.X("Segmen:N", title=None, axis=alt.Axis(labelAngle=0)),
                        y=alt.Y("total_rides:Q", title="Total Perjalanan", axis=alt.Axis(format="~s")),
                        color=alt.Color(
                            "Segmen:N",
                            scale=alt.Scale(
                                domain=["Member (Langganan Tahunan)", "Casual (Harian / Turis)"],
                                range=["#2563EB", "#10B981"],
                            ),
                            legend=None,
                        ),
                        tooltip=[
                            alt.Tooltip("Segmen:N"),
                            alt.Tooltip("total_rides:Q", title="Total Perjalanan", format=","),
                            alt.Tooltip("avg_duration_minutes:Q", title="Durasi Rata-rata (menit)", format=".2f"),
                        ],
                    )
                    .properties(height=290)
                )
                st.altair_chart(u_chart, use_container_width=True)

                u_clean = u_chart_df[["Segmen", "total_rides", "avg_duration_minutes"]].rename(
                    columns={"total_rides": "Total Trips", "avg_duration_minutes": "Rata-rata Durasi (menit)"}
                )
                st.dataframe(u_clean, use_container_width=True, hide_index=True)
            else:
                st.info("Data segmentasi pengguna tidak tersedia.")

        st.divider()

        # ----------------------------------------------------
        # TOP 10 BUSIEST STATIONS IN NYC
        # ----------------------------------------------------
        st.markdown('<div class="chart-header">🏆 Top 10 Stasiun Keberangkatan Tersibuk di New York City</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="chart-caption">Stasiun awal dengan volume keberangkatan tertinggi di sekitar pusat komersial Manhattan, hub transit kereta (Penn Station), dan Hudson River.</div>',
            unsafe_allow_html=True,
        )

        if not station_df.empty and "start_station_name" in station_df.columns:
            top_stations = (
                station_df.groupby(["start_station_id", "start_station_name"], as_index=False)
                .agg({"total_rides": "sum", "avg_duration_minutes": "mean"})
                .sort_values("total_rides", ascending=False)
                .head(10)
            )

            col_s_left, col_s_right = st.columns([5, 5])

            with col_s_left:
                station_chart = (
                    alt.Chart(top_stations)
                    .mark_bar(cornerRadiusTopRight=5, cornerRadiusBottomRight=5)
                    .encode(
                        y=alt.Y("start_station_name:N", sort="-x", title=None, axis=alt.Axis(labelLimit=250)),
                        x=alt.X("total_rides:Q", title="Total Perjalanan", axis=alt.Axis(format="~s")),
                        color=alt.Color("total_rides:Q", scale=alt.Scale(scheme="blues"), legend=None),
                        tooltip=[
                            alt.Tooltip("start_station_name:N", title="Stasiun"),
                            alt.Tooltip("start_station_id:N", title="Station ID"),
                            alt.Tooltip("total_rides:Q", title="Total Trips", format=","),
                            alt.Tooltip("avg_duration_minutes:Q", title="Rata-rata Durasi", format=".2f"),
                        ],
                    )
                    .properties(height=340)
                )
                st.altair_chart(station_chart, use_container_width=True)

            with col_s_right:
                display_s = top_stations.rename(
                    columns={
                        "start_station_name": "Nama Stasiun",
                        "start_station_id": "Station ID",
                        "total_rides": "Total Perjalanan",
                        "avg_duration_minutes": "Rata-rata Durasi (menit)",
                    }
                )
                st.dataframe(display_s, use_container_width=True, hide_index=True)
                st.caption(
                    "💡 **Rekomendasi Operasional:** Stasiun peringkat atas seperti **W 21 St & 6 Ave** dan **Pier 61 at Chelsea Piers** "
                    "membutuhkan alokasi dock ekstra (>50 slots) dan tim *valet staging* pada jam sibuk sore."
                )

        with st.expander("🔍 Lihat Ringkasan Agregasi Data Mentah"):
            if not hourly_df.empty:
                sort_c = [c for c in ["ride_date", "ride_hour"] if c in hourly_df.columns]
                st.dataframe(hourly_df.sort_values(sort_c) if sort_c else hourly_df, use_container_width=True, hide_index=True)


# ============================================================
# TAB 2: REAL-TIME STATION MONITORING (GBFS VIA KAFKA)
# ============================================================
with tab_realtime:
    @st.fragment(run_every="30s")
    def render_realtime_monitor():
        latest_doc = db["realtime_station_status"].find_one({}, sort=[("snapshot_time", -1)])

        if not latest_doc:
            st.info("📡 Menunggu aliran data real-time di MongoDB. Pastikan Kafka Producer dan Spark Streaming aktif.")
            return

        latest_time = latest_doc["snapshot_time"]
        cursor = db["realtime_station_status"].find({"snapshot_time": latest_time})
        df = pd.DataFrame(list(cursor))

        # Metadata Stasiun (GBFS Lyft)
        meta_df = load_station_metadata()
        if not meta_df.empty and "station_id" in df.columns:
            df = df.merge(meta_df, on="station_id", how="left")
            if "station_name_y" in df.columns:
                df["station_name"] = df["station_name_y"].fillna(df.get("station_name_x", df["station_id"]))

        active_df = df[df["is_renting"] == 1].copy() if "is_renting" in df.columns else df.copy()

        # Format Waktu Indonesia Barat (WIB = UTC+7)
        local_time = pd.to_datetime(latest_time) + pd.Timedelta(hours=7)
        timestamp_str = local_time.strftime("%d %B %Y, %H:%M:%S WIB")

        # ----------------------------------------------------
        # LIVE TELEMETRY BAR
        # ----------------------------------------------------
        t_col1, t_col2 = st.columns([4, 1.2])
        with t_col1:
            st.markdown(
                f"""
                <div class="telemetry-bar">
                    <div style="display:flex; align-items:center;">
                        <span class="pulse-dot"></span>
                        <span style="font-weight:700; color:#FFFFFF; margin-right:8px;">KAFKA LIVE TELEMETRY ACTIVE</span>
                        <span style="color:#94A3B8;">| Snapshot Terakhir: <b style="color:#F8FAFC;">{timestamp_str}</b></span>
                    </div>
                    <span style="font-size:11.5px; color:#10B981; font-weight:600; background:rgba(16,185,129,0.15); padding:3px 8px; border-radius:5px;">
                        Auto-Refresh 30s
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with t_col2:
            if st.button("🔄 Segarkan Sekarang", use_container_width=True):
                st.rerun()

        # Metrik Operasional Real-Time
        active_count = len(active_df)
        total_bikes = int(active_df["num_bikes_available"].sum()) if "num_bikes_available" in active_df.columns else 0
        total_ebikes = int(active_df["num_ebikes_available"].sum()) if "num_ebikes_available" in active_df.columns else 0
        total_docks = int(active_df["num_docks_available"].sum()) if "num_docks_available" in active_df.columns else 0

        empty_stations = int((active_df["num_bikes_available"] == 0).sum()) if "num_bikes_available" in active_df.columns else 0
        full_stations = int((active_df["num_docks_available"] == 0).sum()) if "num_docks_available" in active_df.columns else 0

        # ----------------------------------------------------
        # 5 OPERATIONAL KPI CARDS
        # ----------------------------------------------------
        r1, r2, r3, r4, r5 = st.columns(5)

        with r1:
            st.markdown(
                f"""
                <div class="metric-box">
                    <div class="metric-top">
                        <span class="metric-title">Stasiun Aktif</span>
                        <div class="metric-badge-icon" style="background:#EFF6FF; color:#2563EB;">🏢</div>
                    </div>
                    <div class="metric-num">{active_count:,}</div>
                    <div class="metric-footer"><span class="pill-green">Online</span> melayani sewa</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with r2:
            st.markdown(
                f"""
                <div class="metric-box">
                    <div class="metric-top">
                        <span class="metric-title">Sepeda Mekanik</span>
                        <div class="metric-badge-icon" style="background:#ECFDF5; color:#059669;">🚲</div>
                    </div>
                    <div class="metric-num">{total_bikes:,}</div>
                    <div class="metric-footer"><span class="pill-blue">Siap Pakai</span> armada reguler</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with r3:
            st.markdown(
                f"""
                <div class="metric-box">
                    <div class="metric-top">
                        <span class="metric-title">Sepeda Listrik</span>
                        <div class="metric-badge-icon" style="background:#FAF5FF; color:#7C3AED;">⚡</div>
                    </div>
                    <div class="metric-num">{total_ebikes:,}</div>
                    <div class="metric-footer"><span class="pill-purple">E-Bikes</span> armada baterai</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with r4:
            st.markdown(
                f"""
                <div class="metric-box">
                    <div class="metric-top">
                        <span class="metric-title">Dock Tersedia</span>
                        <div class="metric-badge-icon" style="background:#FFFBEB; color:#D97706;">🅿️</div>
                    </div>
                    <div class="metric-num">{total_docks:,}</div>
                    <div class="metric-footer"><span class="pill-amber">Slot Kosong</span> siap parkir</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with r5:
            alert_pill = "pill-red" if (empty_stations > 0 or full_stations > 0) else "pill-green"
            st.markdown(
                f"""
                <div class="metric-box">
                    <div class="metric-top">
                        <span class="metric-title">Stasiun Kritis</span>
                        <div class="metric-badge-icon" style="background:#FEF2F2; color:#DC2626;">🚨</div>
                    </div>
                    <div class="metric-num">{empty_stations} / {full_stations}</div>
                    <div class="metric-footer"><span class="{alert_pill}">{empty_stations} kosong</span> • {full_stations} penuh</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # Alert Banner
        if empty_stations > 0 or full_stations > 0:
            st.markdown(
                f"""
                <div class="callout-alert callout-warning">
                    ⚠️ <b>Instruksi Dispatch Rebalancing:</b> Terdeteksi <b>{empty_stations} stasiun kehabisan unit (Stok = 0)</b> 
                    yang memerlukan pengiriman armada, serta <b>{full_stations} stasiun dengan dock penuh total (Slot = 0)</b> 
                    yang berisiko menimbulkan keluhan pengguna karena tidak bisa mengunci sepeda.
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                """
                <div class="callout-alert callout-success">
                    ✅ <b>Kondisi Armada Optimal:</b> Distribusi sepeda dan slot dock parkir di seluruh New York City berada dalam status seimbang.
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ----------------------------------------------------
        # TREN KETERSEDIAAN ARMADA REAL-TIME
        # ----------------------------------------------------
        st.markdown('<div class="chart-header">📈 Tren Ketersediaan Armada Real-Time (20 Snapshot Terakhir)</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="chart-caption">Dinamika fluktuasi total sepeda mekanik, e-bike, dan ketersediaan dock kosong per menit dari feed Kafka.</div>',
            unsafe_allow_html=True,
        )

        df_trend = load_realtime_trend()
        if not df_trend.empty and len(df_trend) > 1:
            trend_melt = df_trend.melt(
                id_vars=["Snapshot Time"],
                value_vars=["Total Bikes", "E-Bikes", "Available Docks"],
                var_name="Metrik",
                value_name="Jumlah",
            )
            line_trend = (
                alt.Chart(trend_melt)
                .mark_line(point=alt.OverlayMarkDef(size=26, filled=True), strokeWidth=2.4)
                .encode(
                    x=alt.X("Snapshot Time:T", title="Waktu Snapshot (WIB)", axis=alt.Axis(format="%H:%M:%S")),
                    y=alt.Y("Jumlah:Q", title="Jumlah Unit / Slot", axis=alt.Axis(format="~s")),
                    color=alt.Color(
                        "Metrik:N",
                        scale=alt.Scale(
                            domain=["Total Bikes", "E-Bikes", "Available Docks"],
                            range=["#2563EB", "#7C3AED", "#D97706"],
                        ),
                        legend=alt.Legend(title=None, orient="top"),
                    ),
                    tooltip=[
                        alt.Tooltip("Snapshot Time:T", title="Waktu", format="%H:%M:%S WIB"),
                        alt.Tooltip("Metrik:N", title="Komponen"),
                        alt.Tooltip("Jumlah:Q", title="Total", format=","),
                    ],
                )
                .properties(height=270)
            )
            st.altair_chart(line_trend, use_container_width=True)
        else:
            st.info("Mengumpulkan riwayat snapshot berkala dari streaming micro-batch...")

        st.divider()

        # ----------------------------------------------------
        # MATRIKS KETIDAKSEIMBANGAN ARMADA (REBALANCING MATRIX)
        # ----------------------------------------------------
        st.markdown('<div class="chart-header">⚖️ Matriks Ketidakseimbangan Armada Stasiun (Rebalancing Matrix)</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="chart-caption">Panduan langsung bagi tim dispatcher operasional untuk mengambil unit berlebih (*Surplus*) dan mendistribusikannya ke stasiun krisis (*Defisit/Dock Penuh*).</div>',
            unsafe_allow_html=True,
        )

        c_surplus, c_deficit, c_dock = st.columns(3)

        with c_surplus:
            st.markdown("##### 🟢 Top 5 Surplus (Stok Terbanyak)")
            st.caption("Stasiun ketersediaan melimpah. Titik ideal penjemputan unit untuk rebalancing.")
            top_surplus = (
                active_df.sort_values("num_bikes_available", ascending=False)
                .head(5)[["station_name", "num_bikes_available", "num_docks_available"]]
                .rename(columns={"station_name": "Stasiun", "num_bikes_available": "Sepeda", "num_docks_available": "Docks"})
            )
            st.dataframe(top_surplus, use_container_width=True, hide_index=True)

        with c_deficit:
            st.markdown("##### 🔴 Top 5 Defisit (Sepeda Paling Sedikit)")
            st.caption("Stasiun kehabisan unit (**Sepeda = 0**). Prioritas pasokan armada pertama.")
            top_deficit = (
                active_df.sort_values(["num_bikes_available", "num_docks_available"], ascending=[True, False])
                .head(5)[["station_name", "num_bikes_available", "num_docks_available"]]
                .rename(columns={"station_name": "Stasiun", "num_bikes_available": "Sepeda", "num_docks_available": "Docks"})
            )
            st.dataframe(top_deficit, use_container_width=True, hide_index=True)

        with c_dock:
            st.markdown("##### 🟡 Top 5 Dock Penuh (Parkir Kritis)")
            st.caption("Stasiun penuh total (**Docks = 0**). Pengguna tidak dapat mengembalikan sepeda.")
            top_dock = (
                active_df.sort_values("num_docks_available", ascending=True)
                .head(5)[["station_name", "num_docks_available", "num_bikes_available"]]
                .rename(columns={"station_name": "Stasiun", "num_docks_available": "Docks Sisa", "num_bikes_available": "Sepeda"})
            )
            st.dataframe(top_dock, use_container_width=True, hide_index=True)

        st.divider()

        # ----------------------------------------------------
        # PETA GEOGRAFIS INTERAKTIF NYC
        # ----------------------------------------------------
        st.markdown('<div class="chart-header">🗺️ Peta Geografis Ketersediaan Stasiun New York City</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="chart-caption">Memvisualisasikan lokasi geografis stasiun di seluruh borough NYC (Manhattan, Brooklyn, Queens, Bronx).</div>',
            unsafe_allow_html=True,
        )

        if "lat" in active_df.columns and "lon" in active_df.columns and active_df["lat"].notna().any():
            map_data = active_df.dropna(subset=["lat", "lon"]).copy()

            m_col1, m_col2 = st.columns([3, 4])
            with m_col1:
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
                st.map(map_data[["lat", "lon"]], size=20, zoom=11)
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
            table_display = active_df.copy()
            if filter_crit:
                table_display = table_display[
                    (table_display["num_bikes_available"] <= 2) | (table_display["num_docks_available"] <= 2)
                ]

            cols_show = [
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
                if c in table_display.columns
            ]
            st.dataframe(
                table_display[cols_show].rename(
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

    render_realtime_monitor()
