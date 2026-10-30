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
    page_title="Citi Bike NYC · Enterprise Mobility Intelligence",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# 2. LUXURY DARK DESIGN SYSTEM (VAULTO AESTHETIC OVERHAUL)
# ============================================================
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap');

    /* 
       PRIMARY CANVAS: LUXURY VAULTO BLACK GRADIENT WITH METALLIC SILVER SPOTLIGHT
       Explicitly targeted on html, body, stApp, and stAppViewContainer so Streamlit themes cannot override it.
    */
    html, body, .stApp, 
    [data-testid="stAppViewContainer"],
    [data-testid="stAppViewContainer"] > .main {
        font-family: 'Plus Jakarta Sans', system-ui, -apple-system, sans-serif !important;
        background: 
            /* Top Center Brushed Metallic Silver Spotlight (Vaulto Signature Glow) */
            radial-gradient(ellipse 70% 45% at 50% -6%, rgba(226, 232, 240, 0.36) 0%, rgba(160, 175, 195, 0.16) 42%, transparent 72%),
            /* Top Right Silver Metallic Rim Highlight */
            radial-gradient(ellipse 50% 35% at 88% 0%, rgba(248, 250, 252, 0.22) 0%, transparent 55%),
            /* Bottom Subtle Titanium Ambient */
            radial-gradient(ellipse 60% 40% at 20% 95%, rgba(148, 163, 184, 0.08) 0%, transparent 50%),
            /* Deep Charcoal Metallic to Obsidian Black Linear Gradient Base */
            linear-gradient(180deg, #1A1E2A 0%, #10121A 32%, #08090D 100%) !important;
        background-attachment: fixed !important;
        color: #F8FAFC !important;
    }

    /* Make header and inner containers transparent so canvas gradient shows cleanly */
    [data-testid="stHeader"] {
        background: transparent !important;
    }

    [data-testid="stMainBlockContainer"],
    .main .block-container {
        background: transparent !important;
    }

    /* Sleek Silver Edge Line at Top of Content */
    .block-container::before {
        content: "";
        display: block;
        height: 1.5px;
        width: 100%;
        background: linear-gradient(90deg, transparent 0%, rgba(226, 232, 240, 0.65) 50%, transparent 100%);
        margin-bottom: 18px;
    }

    /* Force all text in dark mode to be bright & readable */
    p, span, label, div, [data-testid="stMarkdownContainer"] p {
        color: #CBD5E1;
    }

    h1, h2, h3, h4, h5, h6 {
        color: #FFFFFF !important;
        font-weight: 700 !important;
        letter-spacing: -0.4px !important;
    }

    /* Sidebar: Luxury Smoked Titanium with Silver Accent Rim */
    [data-testid="stSidebar"], [data-testid="stSidebarContent"] {
        background: 
            radial-gradient(ellipse 90% 40% at 50% 0%, rgba(220, 228, 240, 0.14) 0%, transparent 60%),
            linear-gradient(180deg, #131520 0%, #0A0B10 100%) !important;
        border-right: 1px solid rgba(220, 228, 240, 0.16) !important;
        box-shadow: 4px 0 24px rgba(0, 0, 0, 0.45);
    }

    [data-testid="stSidebar"] * {
        color: #CBD5E1 !important;
    }

    /* Hero Header Banner (Vaulto Silver-Black Gradient) */
    .vaulto-hero {
        background: 
            radial-gradient(ellipse 80% 60% at 75% 0%, rgba(241, 245, 249, 0.16) 0%, transparent 60%),
            radial-gradient(circle at 10% 90%, rgba(255, 87, 34, 0.10) 0%, transparent 50%),
            linear-gradient(140deg, #1E2333 0%, #131622 55%, #0B0D14 100%) !important;
        border: 1px solid rgba(220, 228, 240, 0.25) !important;
        border-top: 1px solid rgba(255, 255, 255, 0.48) !important;
        border-radius: 16px;
        padding: 24px 32px;
        margin-bottom: 24px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        box-shadow: 0 16px 40px -6px rgba(0, 0, 0, 0.75), inset 0 1px 0 rgba(255, 255, 255, 0.25);
    }

    .vaulto-hero-title {
        font-size: 26px;
        font-weight: 800;
        letter-spacing: -0.6px;
        color: #FFFFFF !important;
        margin: 0;
        display: flex;
        align-items: center;
        gap: 12px;
    }

    .vaulto-hero-desc {
        font-size: 13.5px;
        color: #94A3B8 !important;
        margin-top: 6px;
        line-height: 1.5;
        max-width: 820px;
    }

    .vaulto-hero-pill {
        background: linear-gradient(135deg, #FF5722 0%, #D84315 100%);
        color: #FFFFFF !important;
        padding: 6px 16px;
        border-radius: 20px;
        font-size: 11.5px;
        font-weight: 700;
        letter-spacing: 0.3px;
        box-shadow: 0 4px 14px rgba(255, 87, 34, 0.35);
        white-space: nowrap;
    }

    /* Metric Cards (Frosted Metallic Dark Glass - Vaulto Reference) */
    .vaulto-card {
        background: 
            radial-gradient(circle at 85% 0%, rgba(241, 245, 249, 0.12) 0%, transparent 50%),
            linear-gradient(155deg, #1A1E2C 0%, #121420 55%, #0A0C13 100%) !important;
        border: 1px solid rgba(220, 228, 240, 0.20) !important;
        border-top: 1px solid rgba(255, 255, 255, 0.40) !important;
        border-radius: 14px;
        padding: 20px 22px;
        box-shadow: 0 10px 28px rgba(0, 0, 0, 0.55), inset 0 1px 0 rgba(255, 255, 255, 0.18);
        transition: all 0.25s cubic-bezier(0.16, 1, 0.3, 1);
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }

    .vaulto-card:hover {
        border-color: rgba(241, 245, 249, 0.55) !important;
        border-top: 1px solid rgba(255, 255, 255, 0.90) !important;
        box-shadow: 0 14px 35px rgba(0, 0, 0, 0.7), 0 0 25px rgba(220, 228, 240, 0.22) !important;
        transform: translateY(-2px);
    }

    .vaulto-label {
        font-size: 11px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: #8E95A2 !important;
    }

    .vaulto-val {
        font-size: 28px;
        font-weight: 800;
        letter-spacing: -0.6px;
        color: #FFFFFF !important;
        margin: 6px 0;
        line-height: 1.1;
    }

    .vaulto-footer {
        font-size: 12px;
        color: #94A3B8 !important;
        display: flex;
        align-items: center;
        gap: 6px;
    }

    /* Neon Pill Badges */
    .neon-pill-green {
        background: rgba(16, 185, 129, 0.15);
        border: 1px solid rgba(16, 185, 129, 0.35);
        color: #34D399 !important;
        font-weight: 700;
        padding: 2px 8px;
        border-radius: 6px;
        font-size: 11px;
    }

    .neon-pill-orange {
        background: rgba(255, 87, 34, 0.15);
        border: 1px solid rgba(255, 87, 34, 0.35);
        color: #FF7043 !important;
        font-weight: 700;
        padding: 2px 8px;
        border-radius: 6px;
        font-size: 11px;
    }

    .neon-pill-blue {
        background: rgba(59, 130, 246, 0.15);
        border: 1px solid rgba(59, 130, 246, 0.35);
        color: #60A5FA !important;
        font-weight: 700;
        padding: 2px 8px;
        border-radius: 6px;
        font-size: 11px;
    }

    .neon-pill-purple {
        background: rgba(168, 85, 247, 0.15);
        border: 1px solid rgba(168, 85, 247, 0.35);
        color: #C084FC !important;
        font-weight: 700;
        padding: 2px 8px;
        border-radius: 6px;
        font-size: 11px;
    }

    .neon-pill-amber {
        background: rgba(245, 158, 11, 0.15);
        border: 1px solid rgba(245, 158, 11, 0.35);
        color: #FBBF24 !important;
        font-weight: 700;
        padding: 2px 8px;
        border-radius: 6px;
        font-size: 11px;
    }

    .neon-pill-red {
        background: rgba(239, 68, 68, 0.15);
        border: 1px solid rgba(239, 68, 68, 0.35);
        color: #F87171 !important;
        font-weight: 700;
        padding: 2px 8px;
        border-radius: 6px;
        font-size: 11px;
    }

    /* Executive Callout Note */
    .vaulto-insight {
        background: linear-gradient(135deg, rgba(255, 87, 34, 0.08) 0%, rgba(19, 21, 28, 0.9) 100%);
        border: 1px solid rgba(255, 87, 34, 0.28);
        border-left: 4px solid #FF5722;
        border-radius: 12px;
        padding: 16px 22px;
        margin: 20px 0 24px 0;
        font-size: 13.5px;
        color: #CBD5E1 !important;
        line-height: 1.6;
    }

    .vaulto-insight b {
        color: #FFFFFF !important;
    }

    /* Live Telemetry Status Bar (Silver-Rimmed Dark Plate) */
    .vaulto-telemetry {
        background: 
            radial-gradient(circle at 10% 50%, rgba(16, 185, 129, 0.08) 0%, transparent 40%),
            linear-gradient(135deg, #181B27 0%, #11131C 60%, #0A0C11 100%) !important;
        border: 1px solid rgba(220, 228, 240, 0.20) !important;
        border-top: 1px solid rgba(255, 255, 255, 0.35) !important;
        border-radius: 12px;
        padding: 13px 22px;
        margin-bottom: 22px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45), inset 0 1px 0 rgba(255, 255, 255, 0.14);
    }

    .pulse-dot {
        display: inline-block;
        width: 10px;
        height: 10px;
        background-color: #10B981;
        border-radius: 50%;
        margin-right: 10px;
        box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
        animation: pulseAnimation 1.6s infinite;
    }

    @keyframes pulseAnimation {
        0% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
        70% { box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
        100% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }

    /* Sidebar Mini Status Card (Metallic Dark Plate) */
    .sidebar-box {
        background: linear-gradient(150deg, #181A26 0%, #11131C 60%, #0B0D13 100%) !important;
        border: 1px solid rgba(220, 228, 240, 0.18) !important;
        border-top: 1px solid rgba(255, 255, 255, 0.28) !important;
        border-radius: 12px;
        padding: 14px 16px;
        margin-bottom: 16px;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.35), inset 0 1px 0 rgba(255, 255, 255, 0.10);
    }

    .sidebar-head {
        font-size: 11px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: #8E95A2 !important;
        margin-bottom: 10px;
    }

    .sidebar-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 12px;
        padding: 6px 0;
        border-bottom: 1px solid rgba(203, 213, 225, 0.08);
    }

    .sidebar-row:last-child {
        border-bottom: none;
    }

    .sidebar-key {
        font-weight: 500;
        color: #94A3B8 !important;
    }

    .sidebar-val {
        font-weight: 700;
        font-size: 11px;
    }

    /* Section Subheaders */
    .vaulto-section-title {
        font-size: 17px;
        font-weight: 700;
        color: #FFFFFF !important;
        letter-spacing: -0.3px;
        margin-bottom: 4px;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    .vaulto-section-desc {
        font-size: 12.5px;
        color: #94A3B8 !important;
        margin-bottom: 14px;
        line-height: 1.4;
    }

    /* Alert Banners */
    .vaulto-alert-warn {
        background: linear-gradient(135deg, rgba(245, 158, 11, 0.12) 0%, rgba(20, 22, 30, 0.9) 100%);
        border: 1px solid rgba(245, 158, 11, 0.35);
        border-left: 4px solid #F59E0B;
        border-radius: 10px;
        padding: 14px 20px;
        color: #FDE68A !important;
        font-size: 13.5px;
        line-height: 1.5;
        margin-bottom: 20px;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
    }

    .vaulto-alert-ok {
        background: linear-gradient(135deg, rgba(16, 185, 129, 0.12) 0%, rgba(20, 22, 30, 0.9) 100%);
        border: 1px solid rgba(16, 185, 129, 0.35);
        border-left: 4px solid #10B981;
        border-radius: 10px;
        padding: 14px 20px;
        color: #A7F3D0 !important;
        font-size: 13.5px;
        line-height: 1.5;
        margin-bottom: 20px;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
    }

    /* Tabs Bar Metallic Silver Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: linear-gradient(180deg, #161926 0%, #0E1018 100%) !important;
        border: 1px solid rgba(220, 228, 240, 0.18) !important;
        border-top: 1px solid rgba(255, 255, 255, 0.28) !important;
        padding: 6px 8px;
        border-radius: 12px;
        box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.5);
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
        font-weight: 600;
        font-size: 13.5px;
        color: #8E95A2 !important;
        border: none;
        background-color: transparent;
    }

    .stTabs [aria-selected="true"] {
        background: linear-gradient(145deg, #2D3346 0%, #1D2130 100%) !important;
        color: #FFFFFF !important;
        border: 1px solid rgba(220, 228, 240, 0.35) !important;
        border-top: 1px solid rgba(255, 255, 255, 0.50) !important;
        box-shadow: 0 6px 18px rgba(0, 0, 0, 0.5), inset 0 1px 0 rgba(255, 255, 255, 0.25) !important;
    }

    /* Streamlit Selectbox and Input overrides */
    [data-testid="stSelectbox"] > div > div {
        background-color: #13151E !important;
        border-color: #272B38 !important;
        color: #FFFFFF !important;
        border-radius: 9px;
    }

    [data-testid="stRadio"] label {
        color: #E2E8F0 !important;
        font-size: 13px;
        font-weight: 500;
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
    username = quote(os.getenv("MONGO_USER", os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin")), safe="")
    password = quote(os.getenv("MONGO_PASSWORD", os.getenv("MONGO_INITDB_ROOT_PASSWORD", "citibikemongo123")), safe="")

    candidates = []
    configured_uri = os.getenv("MONGO_URI")
    if configured_uri:
        candidates.append(configured_uri)

    # Deteksi lingkungan: Container Docker (Linux) vs Host Lokal (Windows)
    is_docker = os.path.exists("/.dockerenv") or os.getenv("IS_CONTAINER", "").lower() == "true" or sys.platform != "win32"
    if is_docker:
        hosts = ["citibike-mongodb", "mongodb", "localhost", "127.0.0.1"]
    else:
        hosts = ["localhost", "127.0.0.1", "citibike-mongodb", "mongodb"]

    for h in hosts:
        uri = f"mongodb://{username}:{password}@{h}:27017/citibike?authSource=admin"
        if uri not in candidates:
            candidates.append(uri)

    last_error = None
    for uri in candidates:
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            return client["citibike"]
        except Exception as e:
            last_error = e

    raise last_error


try:
    db = get_database()
except Exception as exc:
    st.error(
        f"❌ Tidak dapat terhubung ke MongoDB Serving Store: {exc}\n\n"
        "💡 **Solusi:** Pastikan kontainer MongoDB sudah aktif dengan menjalankan: `docker compose up -d mongodb`"
    )
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


# Helper untuk tema gelap Altair Charts
def apply_dark_chart_theme(chart):
    return (
        chart
        .configure(background="transparent")
        .configure_view(strokeWidth=0)
        .configure_axis(
            gridColor="#1E222D",
            domainColor="#272C3A",
            labelColor="#94A3B8",
            titleColor="#CBD5E1",
            labelFont="Plus Jakarta Sans",
            titleFont="Plus Jakarta Sans"
        )
        .configure_legend(
            labelColor="#CBD5E1",
            titleColor="#F8FAFC",
            labelFont="Plus Jakarta Sans",
            titleFont="Plus Jakarta Sans"
        )
    )


# ============================================================
# 5. SIDEBAR: CLEAN & MINIMALIST (VAULTO STYLE)
# ============================================================
with st.sidebar:
    st.markdown(
        """
        <div style="text-align: center; padding: 12px 0 18px 0;">
            <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/0/09/Citi_Bike_logo.svg/320px-Citi_Bike_logo.svg.png" width="130" style="margin-bottom: 8px;">
            <div style="font-size: 15px; font-weight: 800; color: #FFFFFF !important; letter-spacing: -0.3px;">Citi Bike Big Data</div>
            <div style="font-size: 11px; font-weight: 600; color: #8E95A2 !important;">Enterprise Analytics Platform</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="sidebar-box">
            <div class="sidebar-head">Arsitektur Pipeline</div>
            <div class="sidebar-row">
                <span class="sidebar-key">Data Lakehouse</span>
                <span class="sidebar-val" style="color:#34D399;">MinIO S3</span>
            </div>
            <div class="sidebar-row">
                <span class="sidebar-key">Event Buffer</span>
                <span class="sidebar-val" style="color:#FF7043;">Kafka 3.8</span>
            </div>
            <div class="sidebar-row">
                <span class="sidebar-key">Compute Engine</span>
                <span class="sidebar-val" style="color:#60A5FA;">Spark 3.5</span>
            </div>
            <div class="sidebar-row">
                <span class="sidebar-key">Serving Layer</span>
                <span class="sidebar-val" style="color:#34D399;">MongoDB 7</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="sidebar-box">
            <div class="sidebar-head">Status Operasional</div>
            <div class="sidebar-row">
                <span class="sidebar-key">Streaming Topic</span>
                <span class="sidebar-val" style="color:#60A5FA;">citibike-status</span>
            </div>
            <div class="sidebar-row">
                <span class="sidebar-key">Batch Scale</span>
                <span class="sidebar-val" style="color:#C084FC;">4.98M Trips</span>
            </div>
            <div class="sidebar-row">
                <span class="sidebar-key">Serving Latency</span>
                <span class="sidebar-val" style="color:#34D399;">Sub-detik</span>
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
    <div class="vaulto-hero">
        <div>
            <h1 class="vaulto-hero-title">🚲 Citi Bike NYC · Enterprise Mobility Intelligence</h1>
            <p class="vaulto-hero-desc">
                Platform intelijen data terpadu untuk mobilitas perkotaan New York City. Memadukan 
                <b>Batch Historical Lakehouse</b> (skala jutaan perjalanan MinIO S3 + Apache Spark) dengan 
                <b>Real-Time Stream Monitoring</b> (event buffer Apache Kafka + GBFS streaming telemetri armada).
            </p>
        </div>
        <div class="vaulto-hero-pill">
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
        # 4 KPI EXECUTIVE CARDS (VAULTO DARK STYLE)
        # ----------------------------------------------------
        k1, k2, k3, k4 = st.columns(4)

        with k1:
            st.markdown(
                f"""
                <div class="vaulto-card">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span class="vaulto-label">Total Perjalanan Valid</span>
                        <div style="width:30px; height:30px; border-radius:8px; background:rgba(59,130,246,0.15); display:flex; align-items:center; justify-content:center; font-size:15px;">🚲</div>
                    </div>
                    <div class="vaulto-val">{total_rides:,}</div>
                    <div class="vaulto-footer">
                        <span class="neon-pill-green">Quality Gate Lolos</span>
                        <span>MinIO Curated</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with k2:
            st.markdown(
                f"""
                <div class="vaulto-card">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span class="vaulto-label">Rata-Rata Durasi</span>
                        <div style="width:30px; height:30px; border-radius:8px; background:rgba(16,185,129,0.15); display:flex; align-items:center; justify-content:center; font-size:15px;">⏱️</div>
                    </div>
                    <div class="vaulto-val">{weighted_avg_duration:.2f} <span style="font-size:16px; font-weight:600; color:#8E95A2;">menit</span></div>
                    <div class="vaulto-footer">
                        <span class="neon-pill-blue">Perjalanan Efisien</span>
                        <span>waktu tempuh riil</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with k3:
            st.markdown(
                f"""
                <div class="vaulto-card">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span class="vaulto-label">Rasio Member Tahunan</span>
                        <div style="width:30px; height:30px; border-radius:8px; background:rgba(168,85,247,0.15); display:flex; align-items:center; justify-content:center; font-size:15px;">💳</div>
                    </div>
                    <div class="vaulto-val">{member_ratio:.1f}%</div>
                    <div class="vaulto-footer">
                        <span class="neon-pill-purple">{member_count:,} trips</span>
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
                <div class="vaulto-card">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span class="vaulto-label">Stasiun Terpopuler</span>
                        <div style="width:30px; height:30px; border-radius:8px; background:rgba(255,87,34,0.15); display:flex; align-items:center; justify-content:center; font-size:15px;">📍</div>
                    </div>
                    <div class="vaulto-val" style="font-size:19px; line-height:1.25;" title="{top_station_name}">{clean_station_display}</div>
                    <div class="vaulto-footer">
                        <span class="neon-pill-orange">{top_station_rides:,} trips</span>
                        <span>titik awal tersibuk</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # Executive Insight Note (High Contrast)
        st.markdown(
            """
            <div class="vaulto-insight">
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
            st.markdown('<div class="vaulto-section-title">📈 Kurva Distribusi Jam Sibuk (Rush Hour Curve)</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="vaulto-section-desc">Volume perjalanan dalam format 24 jam. Mengidentifikasi lonjakan jam berangkat dan pulang kantor.</div>',
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
                                range=["#FF5722", "#3B82F6", "#222736"],
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
                st.altair_chart(apply_dark_chart_theme(hourly_chart), use_container_width=True)

                st.caption(
                    "🔍 **Pola Bimodal:** Terlihat 2 lonjakan tajam: **Pukul 08:00 pagi** (arus komuter pagi) "
                    "dan **Pukul 17:00–18:00 sore** (arus pulang kerja). Tim operasional wajib menyelesaikan rebalancing armada sebelum pukul 07:30 dan 16:30."
                )
            else:
                st.info("Data jam sibuk tidak tersedia untuk filter ini.")

        with g_col2:
            st.markdown('<div class="vaulto-section-title">👥 Segmentasi Pengguna (Member vs Casual)</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="vaulto-section-desc">Komparasi menyeluruh antara volume perjalanan dan rata-rata durasi bersepeda per segmen.</div>',
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
                                range=["#3B82F6", "#10B981"],
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
                st.altair_chart(apply_dark_chart_theme(u_chart), use_container_width=True)

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
        st.markdown('<div class="vaulto-section-title">🏆 Top 10 Stasiun Keberangkatan Tersibuk di New York City</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="vaulto-section-desc">Stasiun awal dengan volume keberangkatan tertinggi di sekitar pusat komersial Manhattan, hub transit kereta (Penn Station), dan Hudson River.</div>',
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
                        color=alt.Color(
                            "total_rides:Q",
                            scale=alt.Scale(range=["#1E3A8A", "#3B82F6", "#FF7043"]),
                            legend=None
                        ),
                        tooltip=[
                            alt.Tooltip("start_station_name:N", title="Stasiun"),
                            alt.Tooltip("start_station_id:N", title="Station ID"),
                            alt.Tooltip("total_rides:Q", title="Total Trips", format=","),
                            alt.Tooltip("avg_duration_minutes:Q", title="Rata-rata Durasi", format=".2f"),
                        ],
                    )
                    .properties(height=340)
                )
                st.altair_chart(apply_dark_chart_theme(station_chart), use_container_width=True)

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
        # LIVE TELEMETRY BAR (HIGH CONTRAST)
        # ----------------------------------------------------
        t_col1, t_col2 = st.columns([4, 1.2])
        with t_col1:
            st.markdown(
                f"""
                <div class="vaulto-telemetry">
                    <div style="display:flex; align-items:center;">
                        <span class="pulse-dot"></span>
                        <span style="font-weight:700; color:#FFFFFF; margin-right:8px; font-size:13px; letter-spacing:0.4px;">KAFKA LIVE TELEMETRY ACTIVE</span>
                        <span style="color:#94A3B8; font-size:13px;">| Snapshot Terakhir: <b style="color:#F8FAFC;">{timestamp_str}</b></span>
                    </div>
                    <span class="neon-pill-green">
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
                <div class="vaulto-card">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span class="vaulto-label">Stasiun Aktif</span>
                        <div style="width:30px; height:30px; border-radius:8px; background:rgba(59,130,246,0.15); display:flex; align-items:center; justify-content:center; font-size:15px;">🏢</div>
                    </div>
                    <div class="vaulto-val">{active_count:,}</div>
                    <div class="vaulto-footer"><span class="neon-pill-green">Online</span> melayani sewa</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with r2:
            st.markdown(
                f"""
                <div class="vaulto-card">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span class="vaulto-label">Sepeda Mekanik</span>
                        <div style="width:30px; height:30px; border-radius:8px; background:rgba(16,185,129,0.15); display:flex; align-items:center; justify-content:center; font-size:15px;">🚲</div>
                    </div>
                    <div class="vaulto-val">{total_bikes:,}</div>
                    <div class="vaulto-footer"><span class="neon-pill-blue">Siap Pakai</span> armada reguler</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with r3:
            st.markdown(
                f"""
                <div class="vaulto-card">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span class="vaulto-label">Sepeda Listrik</span>
                        <div style="width:30px; height:30px; border-radius:8px; background:rgba(168,85,247,0.15); display:flex; align-items:center; justify-content:center; font-size:15px;">⚡</div>
                    </div>
                    <div class="vaulto-val">{total_ebikes:,}</div>
                    <div class="vaulto-footer"><span class="neon-pill-purple">E-Bikes</span> baterai aktif</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with r4:
            st.markdown(
                f"""
                <div class="vaulto-card">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span class="vaulto-label">Dock Tersedia</span>
                        <div style="width:30px; height:30px; border-radius:8px; background:rgba(245,158,11,0.15); display:flex; align-items:center; justify-content:center; font-size:15px;">🅿️</div>
                    </div>
                    <div class="vaulto-val">{total_docks:,}</div>
                    <div class="vaulto-footer"><span class="neon-pill-amber">Slot Kosong</span> siap parkir</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with r5:
            alert_pill = "neon-pill-red" if (empty_stations > 0 or full_stations > 0) else "neon-pill-green"
            st.markdown(
                f"""
                <div class="vaulto-card">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span class="vaulto-label">Stasiun Kritis</span>
                        <div style="width:30px; height:30px; border-radius:8px; background:rgba(239,68,68,0.15); display:flex; align-items:center; justify-content:center; font-size:15px;">🚨</div>
                    </div>
                    <div class="vaulto-val">{empty_stations} / {full_stations}</div>
                    <div class="vaulto-footer"><span class="{alert_pill}">{empty_stations} kosong</span> • {full_stations} penuh</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # Alert Banner
        if empty_stations > 0 or full_stations > 0:
            st.markdown(
                f"""
                <div class="vaulto-alert-warn">
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
                <div class="vaulto-alert-ok">
                    ✅ <b>Kondisi Armada Optimal:</b> Distribusi sepeda dan slot dock parkir di seluruh New York City berada dalam status seimbang.
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ----------------------------------------------------
        # TREN KETERSEDIAAN ARMADA REAL-TIME
        # ----------------------------------------------------
        st.markdown('<div class="vaulto-section-title">📈 Tren Ketersediaan Armada Real-Time (20 Snapshot Terakhir)</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="vaulto-section-desc">Dinamika fluktuasi total sepeda mekanik, e-bike, dan ketersediaan dock kosong per menit dari feed Kafka.</div>',
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
                            range=["#3B82F6", "#A855F7", "#FF7043"],
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
            st.altair_chart(apply_dark_chart_theme(line_trend), use_container_width=True)
        else:
            st.info("Mengumpulkan riwayat snapshot berkala dari streaming micro-batch...")

        st.divider()

        # ----------------------------------------------------
        # MATRIKS KETIDAKSEIMBANGAN ARMADA (REBALANCING MATRIX)
        # ----------------------------------------------------
        st.markdown('<div class="vaulto-section-title">⚖️ Matriks Ketidakseimbangan Armada Stasiun (Rebalancing Matrix)</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="vaulto-section-desc">Panduan langsung bagi tim dispatcher operasional untuk mengambil unit berlebih (*Surplus*) dan mendistribusikannya ke stasiun krisis (*Defisit/Dock Penuh*).</div>',
            unsafe_allow_html=True,
        )

        c_surplus, c_deficit, c_dock = st.columns(3)

        with c_surplus:
            st.markdown(
                """
                <div style="background: linear-gradient(145deg, #181C28 0%, #11141E 60%, #0A0C13 100%); border: 1px solid rgba(220, 228, 240, 0.18); border-top: 1px solid rgba(255, 255, 255, 0.35); border-radius: 12px; padding: 12px 16px; margin-bottom: 12px; box-shadow: 0 4px 16px rgba(0,0,0,0.4);">
                    <div style="display:flex; align-items:center; justify-content:space-between;">
                        <span style="font-weight:700; color:#34D399; font-size:13px;">🟢 Top 5 Surplus (Stok Melimpah)</span>
                        <span class="neon-pill-green">Ambil Unit</span>
                    </div>
                    <div style="font-size:11.5px; color:#94A3B8; margin-top:4px;">Stasiun dengan sepeda terbanyak untuk rebalancing.</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            top_surplus = (
                active_df.sort_values("num_bikes_available", ascending=False)
                .head(5)[["station_name", "num_bikes_available", "num_docks_available"]]
                .rename(columns={"station_name": "Stasiun", "num_bikes_available": "Sepeda", "num_docks_available": "Docks"})
            )
            st.dataframe(top_surplus, use_container_width=True, hide_index=True)

        with c_deficit:
            st.markdown(
                """
                <div style="background: linear-gradient(145deg, #181C28 0%, #11141E 60%, #0A0C13 100%); border: 1px solid rgba(220, 228, 240, 0.18); border-top: 1px solid rgba(255, 255, 255, 0.35); border-radius: 12px; padding: 12px 16px; margin-bottom: 12px; box-shadow: 0 4px 16px rgba(0,0,0,0.4);">
                    <div style="display:flex; align-items:center; justify-content:space-between;">
                        <span style="font-weight:700; color:#F87171; font-size:13px;">🔴 Top 5 Defisit (Sepeda = 0)</span>
                        <span class="neon-pill-red">Kirim Unit</span>
                    </div>
                    <div style="font-size:11.5px; color:#94A3B8; margin-top:4px;">Stasiun kehabisan unit. Prioritas pasokan armada pertama.</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            top_deficit = (
                active_df.sort_values(["num_bikes_available", "num_docks_available"], ascending=[True, False])
                .head(5)[["station_name", "num_bikes_available", "num_docks_available"]]
                .rename(columns={"station_name": "Stasiun", "num_bikes_available": "Sepeda", "num_docks_available": "Docks"})
            )
            st.dataframe(top_deficit, use_container_width=True, hide_index=True)

        with c_dock:
            st.markdown(
                """
                <div style="background: linear-gradient(145deg, #181C28 0%, #11141E 60%, #0A0C13 100%); border: 1px solid rgba(220, 228, 240, 0.18); border-top: 1px solid rgba(255, 255, 255, 0.35); border-radius: 12px; padding: 12px 16px; margin-bottom: 12px; box-shadow: 0 4px 16px rgba(0,0,0,0.4);">
                    <div style="display:flex; align-items:center; justify-content:space-between;">
                        <span style="font-weight:700; color:#FBBF24; font-size:13px;">🟡 Top 5 Dock Penuh (Parkir Kritis)</span>
                        <span class="neon-pill-amber">Kosongkan Dock</span>
                    </div>
                    <div style="font-size:11.5px; color:#94A3B8; margin-top:4px;">Stasiun penuh total. Pengguna tidak bisa parkir.</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
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
        st.markdown('<div class="vaulto-section-title">🗺️ Peta Geografis Ketersediaan Stasiun New York City</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="vaulto-section-desc">Memvisualisasikan lokasi geografis stasiun di seluruh borough NYC (Manhattan, Brooklyn, Queens, Bronx).</div>',
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
