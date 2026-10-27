# 🚴‍♂️ Citi Bike Big Data Analytics & Real-Time Monitoring Platform

[![Branch](https://img.shields.io/badge/branch-eksperimen--2.1-blue.svg)](https://github.com/Ariqueeezz/citi-bike-bigdata)
[![Status](https://img.shields.io/badge/status-work--in--progress-orange.svg)](#)
[![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-HDFS%203.2.1-yellow.svg)](https://hadoop.apache.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.7-E25A1C.svg)](https://spark.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-8.0-47A248.svg)](https://www.mongodb.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.50.0-FF4B4B.svg)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Docker%20Compose-v2-2496ED.svg)](https://www.docker.com/)

> ⚠️ **Catatan Status (WIP):** Dokumentasi ini merupakan dokumentasi teknis sementara (*work-in-progress*) untuk pengembangan yang sedang berjalan pada branch `eksperimen-2.1`.

---

## 📌 1. Gambaran Umum Proyek

Proyek **Citi Bike Big Data** adalah platform *end-to-end distributed data engineering* dan *analytics* yang dibangun untuk mengolah, menganalisis, serta memantau operasional armada sepeda berbagi terbesar di New York City (**Citi Bike / Lyft**).

Arsitektur sistem ini mengadopsi pola **Hybrid / Lambda-style Architecture** yang memisahkan dua alur beban kerja utama:
1. **Batch Processing Pipeline (Historical Trip Analytics):** Mengambil data historis perjalanan sepeda dalam skala besar (dimulai dari dataset Januari 2026), menyimpannya di *data lake* terdistribusi (**HDFS**), memproses dan mengagregasi data menggunakan klaster **Apache Spark**, lalu menyajikan hasilnya ke **MongoDB** untuk visualisasi pola mobilitas urban (jam sibuk, komparasi *member vs casual*, rute dan stasiun tersibuk).
2. **Real-Time Streaming / Ingestion Pipeline (Live Fleet & Station Monitoring):** Mengambil data telemetri publik langsung melalui **Citi Bike GBFS v2.3 API** (*General Bikeshare Feed Specification*) setiap 60 detik. Memantau ketersediaan sepeda standar, e-bike, slot dermaga kosong (*empty docks*), serta mendeteksi anomali/stasiun kritis secara langsung (*real-time*).
3. **Serving & Visualization Layer:** Menyajikan visualisasi terpadu dan interaktif melalui aplikasi multi-tab **Streamlit** yang dilengkapi peta spasial NYC dan pembaruan otomatis setiap 30 detik.

---

## 🏗️ 2. Arsitektur Sistem

```
+----------------------------------------------------------------------------------------------------+
|                                         DATA INGESTION                                             |
+----------------------------------------------------------------------------------------------------+
       |                                                                        |
       | [Batch Ingestion - Historical]                                         | [Real-Time Ingestion - Live Feed]
       v                                                                        v
+-------------------------------+                                +-------------------------------+
|  Citi Bike Historical Trips   |                                |    Citi Bike GBFS v2.3 APIs   |
|   (NYC Monthly CSV Data)      |                                | (station_status / information)|
+-------------------------------+                                +-------------------------------+
       |                                                                        |
       v (hdfs dfs -put)                                                        v (HTTP polling 60s)
+-------------------------------+                                +-------------------------------+
|     Hadoop HDFS Storage       |                                |       realtime-collector      |
|  (NameNode :9000 / Web :9870) |                                |  (collector/realtime/collector|
|  Path: /raw/2026/01/          |                                +-------------------------------+
+-------------------------------+                                               |
       |                                                                        v (Store JSON)
       |                                                         +-------------------------------+
       |                                                         |  ./data/realtime/ snapshots   |
       |                                                         +-------------------------------+
       |                                                                 |               |
       |                                               (Archived to HDFS)|               v (Direct Upsert)
       v                                                                 v        +----------------------+
+-------------------------------------------------------------+          |        | MongoDB Collection:  |
|                 Apache Spark 3.5.7 Cluster                  |          |        | realtime_station_    |
|   - Spark Master (:8080, :7077)                             |          |        | status               |
|   - Spark Worker (:8081, 2 cores, 1GB RAM)                  |          |        +----------------------+
|   - Reads from HDFS (hdfs://namenode:9000/raw/...)          |          |                   |
|   - Distributed aggregation & ETL                           |          |                   |
+-------------------------------------------------------------+          |                   |
       |                                                                 |                   |
       v (Aggregated batch stats written via Mongo-Spark Connector)       v                   |
+-----------------------------------------------------------------------------+              |
|                               MongoDB 8.0                                   |              |
| Database: citibike                                                          |              |
| Collections:                                                                |              |
|   - exp2_hourly_stats        (Rides by hour & date)                         |<-------------+
|   - exp2_user_type_stats     (Rides & duration by Member vs Casual)         |
|   - exp2_station_stats       (Top stations by trip counts)                  |
|   - historical_*             (Experiment 1 baseline collections)            |
|   - realtime_station_status  (Live station availability snapshots)          |
+-----------------------------------------------------------------------------+
                                      |
                                      v (PyMongo queries)
+-----------------------------------------------------------------------------+
|                      Streamlit Dashboard (Port 8501)                        |
|   - Tab 1: Experiment 1 · Historical (Baseline standalone Spark)            |
|   - Tab 2: Experiment 2 · HDFS + Spark Batch Analytics (Distributed Data)   |
|   - Tab 3: Real-Time Monitoring (30s auto-refresh, NYC Map, Alerts, Trends) |
+-----------------------------------------------------------------------------+
```

---

## ⚙️ 3. Tech Stack & Komponen Sistem

| Komponen | Teknologi & Versi | Peran & Konfigurasi | Port Eksternal |
|---|---|---|---|
| **Data Lake Storage** | **Apache Hadoop HDFS** (`bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`, `hadoop-datanode`) | Penyimpanan terdistribusi untuk data mentah CSV dan arsip snapshot GBFS. Replikasi: 1. | `9870` (NameNode Web), `9000` (HDFS RPC) |
| **Distributed Processing** | **Apache Spark 3.5.7** (`apache/spark:3.5.7`) | Pemrosesan ETL dan agregasi skala besar (1 Master, 1 Worker dengan 2 CPU core, limit RAM 1GB). | `8080` (Master Web), `8081` (Worker Web), `7077` (Spark RPC) |
| **Serving Database** | **MongoDB 8.0** (`mongo:8`) | Penyimpanan dokumen untuk data analitik teragregasi dan snapshot stasiun real-time. Memori dibatasi 1GB. | `27017` |
| **Ingestion Collector** | **Python 3.11** (`collector/realtime/collector.py`) | *Service* polling otomatis feed GBFS Lyft setiap 60 detik, menyimpan snapshot lokal dan memperbarui MongoDB. | Background daemon |
| **Web Visualization** | **Streamlit 1.50** (`app/streamlit/app.py`) | Dashboard analitik interaktif multi-tab dengan auto-refresh 30 detik untuk visualisasi metrik dan pemetaan spasial. | `8501` |
| **Orchestration** | **Docker Compose v2** | Mengelola seluruh siklus hidup klaster dengan konfigurasi dual-compose berbatas memori (*resource limits*). | - |

---

## 📁 4. Struktur Direktori Proyek

```text
citi-bike-bigdata/
├── .env                              # Variabel lingkungan & kredensial Mongo (tidak dikomit)
├── .env.example                      # Template variabel lingkungan
├── .gitignore                        # Konfigurasi ignore file lokal, HDFS data, dan cache
├── docker-compose.yml                # Compose utama: MongoDB, Spark Master/Worker, Streamlit, Collector
├── docker-compose.hdfs.yml           # Compose klaster HDFS: NameNode & DataNode
├── docker-compose.exp1.backup.yml    # Arsip konfigurasi Eksperimen 1 (sebelum integrasi HDFS)
│
├── app/
│   └── streamlit/
│       ├── app.py                    # Aplikasi dashboard Streamlit (3 Tab: Exp 1, Exp 2, Real-Time)
│       └── requirements.txt          # Dependensi Python UI (streamlit, pymongo, pandas, requests)
│
├── collector/
│   └── realtime/
│       ├── collector.py              # Script polling GBFS real-time API
│       └── requirements.txt          # Dependensi poller (requests)
│
├── data/                             # Staging data lokal (volume mount)
│   ├── raw/                          # File CSV data mentah historis
│   ├── processed/                    # Output perantara
│   └── realtime/                     # Snapshot JSON berkala GBFS
│
├── hdfs/                             # Persistent volume storage untuk HDFS lokal (git-ignored)
│   ├── namenode/                     # Direktori metadata NameNode
│   └── datanode/                     # Direktori blok DataNode
│
└── spark/                            # Volume kerja Spark
    └── ivy/cache/                    # Cache dependensi Spark Ivy (mis. mongo-spark-connector)
```

---

## 📊 5. Fitur Utama & Modul Analitik

### A. Batch Analytics (Eksperimen 2: HDFS + Spark)
* **Validasi & Pembersihan Data:** Memvalidasi durasi perjalanan, menangani missing values, dan mengonversi format waktu.
* **Distribusi Penggunaan Per Jam:** Agregasi perjalanan harian dan per jam untuk mengidentifikasi *morning peak* dan *evening peak*.
* **Analisis Segmen Pengguna:** Perbandingan rasio volume perjalanan dan rata-rata durasi (*average duration*) antara **Member** (komuter reguler) dan **Casual** (turis/pengguna insidentil).
* **Identifikasi Stasiun Tersibuk:** Peringkat stasiun dengan keberangkatan tertinggi di New York City.

### B. Real-Time Telemetry & Monitoring
* **Metrik Operasional Utama:** Total stasiun aktif menyewakan, stasiun terpasang, ketersediaan sepeda manual, e-bike, dan slot dermaga kosong.
* **Sistem Peringatan Dini (Alerts):**
  - 🚨 **Empty Stations:** Peringatan untuk stasiun dengan 0 sepeda tersedia.
  - ⚠️ **Full Stations:** Peringatan untuk stasiun dengan 0 dermaga kosong (pengguna tidak bisa mengembalikan sepeda).
  - ⏱️ **Stale Reporting Stations:** Deteksi stasiun yang tidak mengirimkan pembaruan status lebih dari 1 jam.
* **Deteksi Surplus vs. Defisit:** Membantu tim operasi logistik mengetahui stasiun mana yang membutuhkan penambahan armada (*rebalancing*) atau pengosongan dermaga.
* **Peta Interaktif NYC:** Visualisasi persebaran stasiun secara geografis dengan filter instan untuk stasiun kritis ($\le 2$ sepeda) dan surplus ($\ge 10$ sepeda).

---

## 🚀 6. Panduan Menjalankan Sistem

### Persyaratan Awal
- Docker & Docker Compose v2 terpasang.
- Spesifikasi RAM minimal yang disarankan: **8 GB** (resource limit telah disetel agar tidak memberatkan host).

### Langkah 1: Persiapan Lingkungan
Salin konfigurasi template environment dan sesuaikan kredensial jika diperlukan:
```bash
cp .env.example .env
```

### Langkah 2: Menjalankan Klaster HDFS
Jalankan NameNode dan DataNode terlebih dahulu:
```bash
docker compose -f docker-compose.hdfs.yml up -d
```
Pastikan NameNode berjalan sehat dengan membuka Web UI di [http://localhost:9870](http://localhost:9870).

### Langkah 3: Menyiapkan Direktori Data Lake di HDFS
Buat direktori di HDFS dan masukkan data mentah perjalanan:
```bash
# Membuat direktori HDFS
docker exec -it namenode hdfs dfs -mkdir -p /raw/2026/01/
docker exec -it namenode hdfs dfs -mkdir -p /raw/realtime/gbfs/

# Mengunggah data CSV mentah (jika ada file di folder data/raw)
docker exec -it namenode hdfs dfs -put /data/raw/*.csv /raw/2026/01/
```

### Langkah 4: Menjalankan Seluruh Service Pendukung
Jalankan MongoDB, Spark Cluster, Streamlit, dan Real-time Collector:
```bash
docker compose up -d
```

### Langkah 5: Mengakses Antarmuka Web
| Service | URL Akses | Deskripsi |
|---|---|---|
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | Dashboard visualisasi analitik & real-time monitoring |
| **HDFS NameNode UI** | [http://localhost:9870](http://localhost:9870) | Monitoring storage klaster HDFS dan browser file |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) | Monitoring status worker, job, dan alokasi resource Spark |
| **Spark Worker UI** | [http://localhost:8081](http://localhost:8081) | Detail core & executor Spark Worker |

---

## 📈 7. Rencana Perkembangan (Roadmap)

- [x] **Eksperimen 1:** Proof of concept Spark standalone + MongoDB.
- [x] **Eksperimen 2:** Integrasi klaster HDFS sebagai Data Lake terdistribusi untuk data batch.
- [x] **Eksperimen 2.1:** Pembersihan repository, optimasi cache Spark Ivy, dan penyesuaian resource constraint kontainer.
- [x] **Real-Time Poller:** Pengambilan data status GBFS v2.3 ke MongoDB dan penyimpanan snapshot berkala.
- [x] **Interactive Dashboard:** Dashboard Streamlit multi-tab dengan auto-refresh dan peta spasial.
- [ ] **Automated Spark Streaming:** Mengubah polling real-time menjadi pipeline streaming terintegrasi (Spark Structured Streaming / Kafka).
- [ ] **Predictive Machine Learning:** Model prediksi kebutuhan sepeda di tiap stasiun berdasarkan data historis dan cuaca.
- [ ] **Orchestration Workflow:** Otomasi penjadwalan ETL harian menggunakan Apache Airflow.

---

## 👥 Kontributor
- **Repository:** [Ariqueeezz/citi-bike-bigdata](https://github.com/Ariqueeezz/citi-bike-bigdata)
- **Branch Aktif:** `eksperimen-2.1`
