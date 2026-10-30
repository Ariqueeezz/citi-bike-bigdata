# 🚴‍♂️ Citi Bike Big Data Analytics & Real-Time Monitoring Platform

[![Branch](https://img.shields.io/badge/branch-eksperimen--2.1-blue.svg)](https://github.com/Ariqueeezz/citi-bike-bigdata)
[![Status](https://img.shields.io/badge/status-active-success.svg)](#)
[![Data Lake](https://img.shields.io/badge/Data%20Lake-MinIO%20S3-C72C48.svg)](https://min.io/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.8.0%20(KRaft)-black.svg)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.7-E25A1C.svg)](https://spark.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-8.0-47A248.svg)](https://www.mongodb.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.50.0-FF4B4B.svg)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Docker%20Compose-v2-2496ED.svg)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB.svg)](https://www.python.org/)

---

## 📌 1. Gambaran Umum Proyek

Proyek **Citi Bike Big Data** adalah platform *end-to-end distributed data engineering* dan *analytics* modern yang dirancang untuk mengolah, menganalisis, serta memantau operasional armada sepeda berbagi terbesar di New York City (**Citi Bike / Lyft**).

Arsitektur sistem ini menggunakan **MinIO Object Storage (S3-compatible Data Lake)** sebagai **satu-satunya Data Lake pusat** untuk seluruh alur kerja (*batch processing* data historis maupun pengarsipan *real-time streaming*), meniadakan ketergantungan pada cluster HDFS:

1. **MinIO S3 sebagai Single Source of Truth (Centralized Data Lake):**
   * **`citibike-raw`**: Menampung data mentah perjalanan bulanan NYC Citi Bike dalam format CSV (`s3a://citibike-raw/2026/01/*.csv`).
   * **`citibike-curated`**: Menyimpan dataset terkurasi dan terstandarisasi dalam format Parquet terkompresi Snappy (`s3a://citibike-curated/2026/01/trips`).
   * **`citibike-realtime`**: Menampung buffer snapshot status stasiun GBFS live terbaru.
   * **`citibike-archive`**: Menyimpan arsip historis status telemetri armada secara terstruktur dengan partisi tanggal (`year=YYYY/month=MM/day=DD/`).

2. **Batch Processing Pipeline (Distributed Historical Trip Analytics):**
   * Pembersihan data, validasi kualitas (*Data Quality Gate*), dan agregasi analitik terdistribusi dijalankan oleh klaster **Apache Spark 3.5.7** membaca langsung dari MinIO via driver S3A (`s3a://`).
   * Mengekstrak tren mobilitas urban (jam sibuk pagi 08:00 & sore 17:00-18:00, rasio *Subscriber vs Casual*, durasi rata-rata, dan pemeringkatan stasiun terpadat).
   * Hasil agregasi disimpan secara otomatis ke **MongoDB 8.0** (`minio_hourly_stats`, `minio_station_stats`, `minio_user_type_stats`) untuk kebutuhan visualisasi latensi rendah.

3. **Real-Time Streaming & Decoupled Archiving Pipeline:**
   * Pengambilan telemetri publik langsung dari **Citi Bike GBFS v2.3 API** (*General Bikeshare Feed Specification*) setiap 60 detik melalui *daemon collector*.
   * Mengalirkan snapshot telemetri stasiun ke message broker **Apache Kafka 3.8.0 (KRaft Mode)** pada topik `citibike-station-status` dengan replikasi pesan berkecepatan tinggi.
   * **Spark Structured Streaming** mengonsumsi pesan Kafka secara *real-time*, mem-parsing payload JSON, memvalidasi integritas data, dan melakukan *upsert* langsung ke koleksi MongoDB `realtime_station_status`.
   * **Pengarsipan Terpisah (Decoupled Batch Archival):** Pengarsipan stream ke bucket `citibike-archive` dipisahkan dari loop pengunggahan realtime. Pengarsipan dijalankan secara berkala atau *on-demand* (misalnya 2 minggu sekali) melalui aksi dedicated runner, mengunggah data stream lokal yang belum pernah diarsipkan ke partisi tanggal MinIO.

4. **Data Quality Gate & Database Optimization:**
   * Modul validasi kualitas data (**Data Quality Validation Suite**) menyaring anomali batch (durasi $\le 0$ atau $> 24$ jam, tanggal di luar periode target, dan *null timestamp*) serta memvalidasi ketersediaan stasiun real-time.
   * Optimasi MongoDB melalui **TTL (Time-To-Live) Index** otomatis yang memangkas data streaming berumur $> 24$ jam, dilengkapi *compound performance indexes* pada field query analitik.

5. **Serving, Visualization & Orchestration:**
   * Dashboard interaktif multi-tab berbasis **Streamlit 1.50** yang dilengkapi peta spasial interaktif NYC, pemantauan status armada real-time (auto-refresh 30 detik), deteksi stasiun kritis (*empty/full docks*), serta visualisasi tren historis.
   * Runner otomasi **PowerShell (`run_pipeline.ps1`)** dan CLI Python (`scripts/pipeline.py`) dengan auto-setup dependensi serta opsi container zero-setup (`-Docker`).

---

## 🏗️ 2. Arsitektur Sistem Terpusat MinIO Data Lake

```
+-----------------------------------------------------------------------------------------------------------------------+
|                                                  DATA INGESTION LAYER                                                 |
+-----------------------------------------------------------------------------------------------------------------------+
           |                                                                               |
           | [Batch Ingestion - Historical CSV]                                            | [Real-Time GBFS v2.3 Feeds]
           v                                                                               v
+-----------------------------------+                                           +---------------------------------------+
|   Citi Bike Historical Datasets   |                                           |    Citi Bike GBFS Live JSON API       |
|   (data/raw/2026-01/*.csv)        |                                           |  (station_status & information)       |
+-----------------------------------+                                           +---------------------------------------+
           |                                                                               |
           | (Data Quality Validator Gate)                                                 | (HTTP Polling 60s)
           v                                                                               v
+-----------------------------------+                                           +---------------------------------------+
|      MinIO Ingestion Manager      |                                           |      citibike-realtime-collector      |
|    (scripts/minio_manager.py)     |                                           |     (collector/realtime/collector.py) |
+-----------------------------------+                                           +---------------------------------------+
           |                                                                               |                     |
           v (S3 Ingest)                                                                   v (Kafka Producer)    v (Local Staging)
+-------------------------------------------------------------------+           +---------------------+   +---------------------+
|                  MinIO S3 Centralized Data Lake                   |           |    Apache Kafka     |   |   ./data/realtime/  |
|  • citibike-raw       : s3a://citibike-raw/2026/01/*.csv          |           |     (KRaft Broker)  |   |   station_status_*. |
|  • citibike-curated   : s3a://citibike-curated/2026/01/trips      |           | Topic: citibike-    |   |   json snapshots    |
|  • citibike-realtime  : buffer snapshot telemetri terbaru         |           | station-status      |   +---------------------+
|  • citibike-archive   : year=YYYY/month=MM/day=DD/*.json          |           +---------------------+              │
+-------------------------------------------------------------------+                      |                         │
        ▲                                                                                  v (Structured Streaming)  │
        │                                                                       +-------------------------------+    │
        │ [Dedicated Archival Action: .\run_pipeline.ps1 archive] ─────────────│          Apache Spark Cluster |    │
        │ (Pengarsipan Inkremental Terpisah per 2 Minggu / On-Demand)          |             (Streaming)        |    │
        │                                                                       +-------------------------------+    │
        │                                                                                      |                     │
        │ (S3A FileSystem Read)                                                                v (Streaming Upsert)  │
        v                                                                       +-------------------------------+    │
+-------------------------------------------------------------+                 |          MongoDB 8.0          |    │
|                 Apache Spark 3.5.7 Cluster                  |                 | Database: citibike            |    │
|                     (Batch Processing)                      |                 | Collections:                  |    │
|       Script: spark/batch/citibike_batch.py                 |                 |  • minio_*_stats              |    │
+-------------------------------------------------------------+                 |  • realtime_station_status    |◄───┘
       |                                             |                          |    (TTL 24 Jam Auto-Prune)    |
       v (Curated Parquet write)                     v (Batch Aggregations)     +-------------------------------+
+-----------------------------+               +-------------------------------+                │
|          MinIO S3           |               |          MongoDB 8.0          |                v (PyMongo queries)
|   citibike-curated bucket   |               | Collections: minio_*_stats    | +-------------------------------+
+-----------------------------+               +-------------------------------+ | Streamlit Dashboard (Port 8501|
                                                                                +-------------------------------+

+-----------------------------------------------------------------------------------------------------------------------+
|                                     ORCHESTRATION & QUALITY GATE (AUTOMATION)                                         |
|  • PowerShell Runner : .\run_pipeline.ps1 [check | validate | up | minio-setup | archive | optimize | batch | stream] |
|  • Container Runner  : Dockerfile & docker-compose.yml (Service: pipeline-runner with auto-setup)                      |
|  • Data Quality Gate : scripts/data_quality_validator.py  |  MongoDB Optimizer: scripts/mongo_optimizer.py             |
+-----------------------------------------------------------------------------------------------------------------------+
```

---

## ⚙️ 3. Tech Stack & Komponen Sistem

| Komponen | Teknologi & Versi | Peran & Detail Konfigurasi | Port Eksternal |
|---|---|---|---|
| **Centralized Data Lake Storage** | **MinIO S3** (`minio-community:local`) | *Object storage* terpusat performa tinggi kompatibel Amazon S3. Menampung 4 bucket utama: `citibike-raw`, `citibike-curated`, `citibike-realtime`, dan `citibike-archive`. | `9002` (S3 API), `9003` (Web Console) |
| **Message Streaming Broker** | **Apache Kafka 3.8.0** (`apache/kafka:3.8.0`) | *Distributed message streaming* dalam mode **KRaft** (tanpa ZooKeeper). Menyerap stream status stasiun secara real-time pada topik `citibike-station-status`. Limit memori: 768MB. | `9092` (Internal), `9094` (Host) |
| **Distributed Data Processing** | **Apache Spark 3.5.7** (`apache/spark:3.5.7`) | Pemrosesan Batch ETL dan Structured Streaming (1 Master, 1 Worker dengan 2 CPU cores, RAM 1.5GB). Dilengkapi driver Hadoop AWS SDK S3A (`org.apache.hadoop.fs.s3a.S3AFileSystem`) dan MongoDB Connector. | `8080` (Master Web), `8081` (Worker Web), `7077` (Spark RPC) |
| **Serving Database** | **MongoDB 8.0** (`mongo:8`) | Database NoSQL dokumen untuk menyajikan data analitik dan snapshot real-time. Dilengkapi **TTL Index (retensi 24 jam)** dan *performance indexing*. Limit memori: 1GB. | `27017` |
| **Real-Time Data Collector** | **Python 3.11** (`collector/realtime/collector.py`) | *Service* polling feed GBFS Lyft setiap 60 detik. Menerbitkan event ke Apache Kafka dan menyimpan snapshot lokal & realtime buffer. Limit memori: 256MB. | Background daemon |
| **Web Visualization** | **Streamlit 1.50** (`app/streamlit/app.py`) | Antarmuka dashboard analitik 2-Tab interaktif (Batch Historical Analytics & Real-Time Telemetry Monitor) dengan visualisasi metrik agregat, peringatan stasiun kritis (*empty/full*), dan peta spasial NYC. | `8501` |
| **Pipeline Containerization** | **Docker & Dockerfile** (`python:3.11-slim`) | Container image mandiri (`citibike-pipeline:latest`) yang membundle seluruh dependensi Python dan Docker CLI untuk eksekusi pipeline zero-setup di host. | - |
| **Automation Runner** | **PowerShell & Python Orchestrator** (`run_pipeline.ps1`, `pipeline.py`) | Skrip orkestrasi modular dengan auto-setup dependensi dari `requirements.txt` dan dukungan container flag (`-Docker`). | CLI |

---

## 📁 4. Struktur Direktori Proyek

```text
citi-bike-bigdata/
├── .dockerignore                     # Rule pengabaian build image Docker
├── .env                              # Variabel konfigurasi lingkungan & kredensial
├── .env.example                      # Template referensi variabel konfigurasi
├── .gitignore                        # Konfigurasi ignore file git lokal & data
├── docker-compose.yml                # Compose utama (Kafka, MinIO, Mongo, Spark, Streamlit, Collector, Pipeline-Runner)
├── Dockerfile                        # Dockerfile utama container pipeline-runner & tools
├── Dockerfile.minio                  # Dockerfile build MinIO community image
├── requirements.txt                  # Daftar lengkap dependensi Python proyek
├── run_pipeline.ps1                  # Automation runner interaktif PowerShell (Lokal & Docker mode)
│
├── app/
│   └── streamlit/
│       ├── app.py                    # Aplikasi dashboard Streamlit (2 Tab: Batch Historical & Real-Time Monitoring)
│       └── requirements.txt          # Dependensi khusus modul Streamlit
│
├── collector/
│   └── realtime/
│       ├── collector.py              # Poller GBFS real-time -> Kafka & MinIO Buffer
│       └── requirements.txt          # Dependensi poller (requests, minio, kafka-python-ng)
│
├── data/                             # Direktori data lokal (volume mount)
│   ├── raw/                          # File CSV data mentah historis (contoh: 2026-01)
│   ├── checkpoint_kafka/             # Direktori state & commit checkpoint Spark Streaming
│   └── realtime/                     # Snapshot berkala JSON GBFS
│
├── reports/                          # Laporan pengujian & evaluasi kinerja sistem
│   ├── evaluasi_sistem.md            # Laporan resmi Markdown (Tabel 4.1 hasil pengukuran riil)
│   └── evaluasi_sistem.json          # Rekapitulasi benchmark terstruktur JSON
│
├── scripts/                          # Suite otomasi data & database
│   ├── pipeline.py                   # Orchestrator CLI Python end-to-end terpusat MinIO
│   ├── data_quality_validator.py     # Validator kualitas data batch CSV & real-time GBFS
│   ├── system_evaluator.py           # Evaluator fungsional & performa sistem (Tabel 4.1)
│   ├── minio_manager.py              # Setup bucket, upload batch, & pengarsipan inkremental stream
│   └── mongo_optimizer.py            # Pembuat TTL Index (24 jam) & Compound Performance Index
│
└── spark/                            # Modul Apache Spark terdistribusi
    ├── batch/
    │   ├── citibike_batch.py         # Batch processing Spark resmi menggunakan MinIO S3 & Mongo
    │   ├── citibike_batch_minio.py   # Versi batch processing multi-bulan MinIO
    │   └── load_mongodb.py           # Utilitas pemuat data ke MongoDB
    ├── integration/
    │   └── verify_curated.py         # Skrip verifikasi skema & data Parquet di MinIO S3
    ├── streaming/
    │   └── citibike_realtime.py      # Spark Structured Streaming (Kafka -> MongoDB)
    └── ivy/
        └── jars/                     # Dependensi JAR (Hadoop AWS, Mongo Connector, Kafka Clients)
```

---

## 📊 5. Fitur Utama & Modul Analitik

### A. Centralized Batch Analytics (MinIO S3A)
* **Single Centralized Data Lake:** Seluruh alur data mentah dan olahan disimpan di MinIO S3A (`s3a://citibike-raw` $\rightarrow$ `s3a://citibike-curated`), menghilangkan kompleksitas klaster HDFS ganda.
* **Agregasi Mobilitas Urban:** Perhitungan volume perjalanan per jam, identifikasi *rush hours* komuter (08:00 pagi dan 17:00-18:00 sore), serta tren mingguan.
* **Segmentasi Pengguna:** Analisis perbandingan rasio total perjalanan dan durasi antara anggota reguler (*Member*) dan pengguna kasual (*Casual*).
* **Top Origin Stations:** Peringkat stasiun dengan frekuensi keberangkatan tertinggi di seluruh distrik New York City.

### B. Real-Time Telemetry & Decoupled Archival
* **High-Throughput Ingestion:** Polling berkala GBFS feed v2.3 Lyft setiap menit, diterbitkan ke topik Kafka `citibike-station-status`.
* **Spark Structured Streaming:** Menjalankan pemrosesan stream secara *continuous*, mem-parsing payload nested JSON, dan melakukan *upsert* ke MongoDB.
* **Decoupled Incremental Stream Archiving:**
  - Pengarsipan tidak dilakukan setiap menit demi menghemat koneksi dan I/O MinIO.
  - Skrip pengarsip secara cerdas membandingkan daftar file lokal di `data/realtime/` dengan objek yang sudah tersimpan di `citibike-archive`.
  - Mengunggah file stream baru yang belum pernah diarsipkan dengan partisi tanggal Hive: `s3://citibike-archive/year=YYYY/month=MM/day=DD/*.json`.
  - Dapat dijalankan berkala (misal 2 minggu sekali dengan `--days 14`) atau sekali klik lewat runner PowerShell.
* **Early Warning System (Alerting):**
  - 🚨 **Empty Stations:** Peringatan untuk stasiun dengan 0 sepeda tersedia.
  - ⚠️ **Full Stations:** Peringatan untuk stasiun dengan 0 dermaga kosong (pengguna tidak dapat mengembalikan sepeda).
  - ⏱️ **Stale Reporting Stations:** Deteksi stasiun yang tidak mengirimkan pembaruan status lebih dari 1 jam.
* **Peta Interaktif Spasial NYC:** Visualisasi posisi geografis seluruh stasiun Citi Bike dengan filter instan untuk stasiun kritis ($\le 2$ sepeda) dan stasiun surplus ($\ge 10$ sepeda).

### C. Data Quality Gate & Database Optimization
* **Batch Validation:** Pengecekan konsistensi durasi perjalanan (0 < durasi <= 24 jam), pemfilteran nilai *null*, dan pembuangan trip di luar batas bulan target.
* **Real-Time Validation:** Verifikasi Unix epoch timestamp, verifikasi stasiun aktif menyewakan (*active renting*) vs sekadar terpasang secara fisik (*installed*), dan penandaan stasiun usang (*stale* > 60 menit).
* **Automated Data Retention (TTL):** Data status real-time secara otomatis dihapus oleh MongoDB setelah berumur 24 jam menggunakan TTL Index pada `snapshot_time`.
* **Performance Indexing:** Index komposit terpasang pada field `snapshot_time`, `station_id`, `is_installed`, `is_renting`, serta field analitik `minio_*`.

### D. Automated System Testing & Performance Evaluator (Tabel 4.1 Benchmark)
Modul evaluasi sistem otomatis (`scripts/system_evaluator.py`) menguji 8 metrik kunci sesuai Bab IV proposal:
* ⏱️ **Waktu Eksekusi & Throughput Batch:** Mengukur durasi pemrosesan PySpark membaca data dari MinIO dan mencatat throughput baris/detik.
* ⚡ **Latensi & Throughput Streaming:** Mengukur jeda waktu dari pengunduhan GBFS hingga tersimpan di MongoDB serta daya tampung laju data masuk.
* 🖥️ **Penggunaan Sumber Daya:** Memantau utilisasi CPU dan alokasi memori kontainer Docker melalui `docker stats` untuk memastikan ketiadaan *Out-Of-Memory* (OOM).
* 🛡️ **Data Quality Gate Verification:** Menguji keandalan penyaringan 100% data anomali durasi, null timestamp, dan pemisahan stasiun offline.
* 🎯 **Akurasi Agregasi (Ground Truth Validation):** Membandingkan hasil agregasi Spark dengan kalkulasi referensi independen Pandas (akurasi 100%).
* 🔄 **Pemulihan Gangguan (Fault Recovery):** Memvalidasi persistensi *Write-Ahead Log* dan checkpoint Kafka pada Spark Streaming untuk pemulihan instan tanpa kehilangan data (*Zero Data Loss*).
* 📑 **Laporan Otomatis:** Menghasilkan laporan evaluasi siap pakai di `reports/evaluasi_sistem.md` dan `reports/evaluasi_sistem.json`.

---

## 🚀 6. Panduan Menjalankan Sistem

### Persyaratan Awal
- **Docker & Docker Compose v2** terpasang dan Docker Desktop aktif.
- Spesifikasi RAM host yang disarankan: **8 GB** atau lebih.
- File konfigurasi `.env` (disalin dari `.env.example`).

---

### Cara Praktis: Menggunakan Pipeline Runner (`run_pipeline.ps1`)

Repository ini dilengkapi dengan script otomasi **PowerShell Runner** yang dapat mendeteksi dependensi, menginstall paket yang kurang secara otomatis, atau berjalan sepenuhnya di dalam kontainer Docker:

#### 1. Masuk ke Menu Interaktif
Buka terminal PowerShell di root folder proyek, lalu jalankan:
```powershell
.\run_pipeline.ps1
```
Menu interaktif akan muncul dengan 11 pilihan aksi otomatis:
```text
======================================================================
         CITI BIKE NYC BIG DATA - AUTOMATION PIPELINE RUNNER          
                (MinIO S3 Centralized Data Lake)                      
======================================================================
Lingkungan Eksekusi: Python Lokal (Auto-Setup Dependensi) | Data Lake: MinIO S3

Pilih aksi yang ingin dijalankan:
  1. check       - Periksa file konfigurasi (.env) dan status Docker
  2. validate    - Jalankan Data Quality Validation Suite
  3. up          - Nyalakan semua container Docker (Kafka, MinIO, MongoDB, Spark, Streamlit)
  4. minio-setup - Buat bucket MinIO Data Lake dan upload data sampel
  5. archive     - Arsipkan data stream ke MinIO citibike-archive (Inkremental / Partisi Tanggal)
  6. optimize    - Terapkan Indexing dan TTL Data Retention di MongoDB
  7. batch       - Eksekusi Spark Batch Processing (MinIO S3A) dan Sinkronisasi MongoDB
  8. stream      - Jalankan Spark Streaming Real-time (GBFS -> Kafka -> MongoDB)
  9. evaluate    - Jalankan Evaluasi Kinerja Sistem & Pengukuran Benchmark (Tabel 4.1)
  10. all        - Jalankan seluruh alur End-to-End secara otomatis
  11. exit       - Keluar
```

#### 2. Menjalankan Perintah Langsung (CLI Command)
Anda dapat memanggil aksi secara langsung tanpa membuka menu:

```powershell
# 0. Langsung Run di Powershell/CMD jika bermasalah
powershell.exe -ExecutionPolicy Bypass -File run_pipeline.ps1

# 1. Periksa kelengkapan file .env dan status Docker
.\run_pipeline.ps1 check

# 2. Jalankan Data Quality Validation
.\run_pipeline.ps1 validate

# 3. Nyalakan seluruh layanan container
.\run_pipeline.ps1 up

# 4. Inisialisasi bucket MinIO dan upload data mentah CSV
.\run_pipeline.ps1 minio-setup

# 5. Arsipkan file stream lokal yang belum diarsipkan ke MinIO citibike-archive
.\run_pipeline.ps1 archive

# (Opsional) Arsipkan hanya data 14 hari terakhir (2 minggu):
.\run_pipeline.ps1 archive -Days 14

# 6. Pasang TTL Index (24 jam) dan Index performa di MongoDB
.\run_pipeline.ps1 optimize

# 7. Jalankan Spark Batch Analytics menggunakan MinIO Data Lake
.\run_pipeline.ps1 batch

# 8. Jalankan Spark Structured Streaming (Kafka -> MongoDB) di background
.\run_pipeline.ps1 stream

# 9. Jalankan Pengujian dan Evaluasi Kinerja Sistem (Tabel 4.1)
.\run_pipeline.ps1 evaluate

# 10. Eksekusi alur lengkap End-to-End secara berurutan
.\run_pipeline.ps1 all
```

#### 3. Menjalankan Menggunakan Container Docker (Zero Local Setup)
Jika Anda tidak ingin menginstall Python atau dependensi modul apapun di komputer lokal Anda, cukup tambahkan switch `-Docker`:

```powershell
# Seluruh dependensi dan script akan dieksekusi di dalam container pipeline-runner:
.\run_pipeline.ps1 -Docker validate
.\run_pipeline.ps1 -Docker archive
.\run_pipeline.ps1 -Docker optimize
.\run_pipeline.ps1 -Docker evaluate
.\run_pipeline.ps1 -Docker all
```

---

### 📊 Hasil Pengujian dan Evaluasi Sistem (Tabel 4.1)

Pengujian dilakukan untuk mengevaluasi fungsionalitas dan performa sistem pada konfigurasi Spark 1 Master + 1 Worker:

| No | Metrik Evaluasi | Metode Pengukuran | Hasil yang Diharapkan | Hasil Pengukuran Riil | Status |
| :-: | :--- | :--- | :--- | :--- | :---: |
| **1** | **Waktu Eksekusi Batch** | Selisih waktu mulai dan selesai pemrosesan Spark membaca CSV hingga tersimpan | Pemrosesan selesai tanpa kegagalan; durasi sebanding dengan volume dataset | **5.21 detik** (Dataset 194 MB / ~1 Juta Baris) | **PASSED ✅** |
| **2** | **Throughput Batch** | Jumlah baris perjalanan valid yang diproses per detik | Mampu memproses seluruh baris valid dengan throughput terukur | **191,938.58 baris/detik** | **PASSED ✅** |
| **3** | **Latensi Streaming** | Selisih waktu antara pengunduhan snapshot GBFS hingga tersimpan di MongoDB | Data status tersedia di dashboard dengan latensi rendah (< 5 detik) stabil | **1.20 detik** | **PASSED ✅** |
| **4** | **Throughput Streaming** | Jumlah rekaman status stasiun yang diproses per detik | Mengimbangi laju data masuk (~2.400 stasiun per polling 60 detik) | **1,800.00 rekaman/detik** | **PASSED ✅** |
| **5** | **Penggunaan CPU/RAM** | Pemantauan kontainer via `docker stats` dan limit alokasi memori | Pemakaian sumber daya terkendali dalam batas alokasi tanpa kegagalan OOM | **Stabil (CPU Puncak < 78%, RAM Total ~3,8 GB dari batas 6 GB)** | **PASSED ✅** |
| **6** | **Kualitas Data** | Rasio penyaringan baris anomali (durasi $\le 0$ atau $> 24$ jam, null timestamp) | Baris anomali tersaring 100%, data valid lolos dan terstandarisasi | **100% Anomali Tersaring (Kualitas Data Valid 99.95%)** | **PASSED ✅** |
| **7** | **Akurasi Agregasi** | Perbandingan hasil perhitungan Spark dengan ground truth Pandas | Jumlah total perjalanan dan nilai agregasi cocok dengan acuan | **Akurasi 100% (Selisih Count = 0, Deviasi Pecahan $\le 0.01$)** | **PASSED ✅** |
| **8** | **Pemulihan Gangguan** | Waktu pemulihan layanan setelah worker dihentikan dan verifikasi checkpoint | Pipeline streaming berlanjut otomatis tanpa kehilangan pesan (*Zero Data Loss*) | **Recovery Time: ~4.5 detik (Kafka Checkpoint Commit Aktif)** | **PASSED ✅** |

---

### Cara Manual (Docker Compose Standar)

Jika Anda ingin menjalankan layanan secara manual menggunakan Docker CLI murni:

```bash
# 1. Salin konfigurasi environment
cp .env.example .env

# 2. Jalankan seluruh kontainer pendukung (Kafka, MinIO, MongoDB, Spark, Streamlit, Collector)
docker compose up -d

# 3. Jalankan inisialisasi bucket MinIO
docker compose run --rm pipeline-runner minio-setup

# 4. Arsipkan data stream ke MinIO Data Lake
docker compose run --rm pipeline-runner archive

# 5. Terapkan index & TTL retention MongoDB
docker compose run --rm pipeline-runner optimize

# 6. Eksekusi Spark Batch Processing di MinIO Data Lake
docker exec -e SPARK_SUBMIT_OPTS="-Duser.home=/tmp" citibike-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/work-dir/ivy/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/opt/spark/work-dir/ivy/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/ivy/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar,/opt/spark/work-dir/ivy/jars/org.mongodb.spark_mongo-spark-connector_2.12-10.7.0.jar,/opt/spark/work-dir/ivy/jars/org.mongodb_bson-5.1.4.jar,/opt/spark/work-dir/ivy/jars/org.mongodb_mongodb-driver-core-5.1.4.jar,/opt/spark/work-dir/ivy/jars/org.mongodb_mongodb-driver-sync-5.1.4.jar,/opt/spark/work-dir/ivy/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/ivy/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/ivy/jars/kafka-clients-3.4.1.jar,/opt/spark/work-dir/ivy/jars/commons-pool2-2.11.1.jar \
  /opt/spark/work-dir/batch/citibike_batch.py

# 7. Jalankan Spark Structured Streaming di background
docker exec -d citibike-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/work-dir/ivy/jars/org.mongodb.spark_mongo-spark-connector_2.12-10.7.0.jar,/opt/spark/work-dir/ivy/jars/org.mongodb_bson-5.1.4.jar,/opt/spark/work-dir/ivy/jars/org.mongodb_mongodb-driver-core-5.1.4.jar,/opt/spark/work-dir/ivy/jars/org.mongodb_mongodb-driver-sync-5.1.4.jar,/opt/spark/work-dir/ivy/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/ivy/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/ivy/jars/kafka-clients-3.4.1.jar,/opt/spark/work-dir/ivy/jars/commons-pool2-2.11.1.jar \
  /opt/spark/work-dir/streaming/citibike_realtime.py
```

---

### 🌐 Port & Akses Antarmuka Web

Setelah kontainer dinyalakan, seluruh layanan web dapat diakses langsung melalui browser Anda:

| Layanan | URL Akses | Kredensial Default | Deskripsi |
|---|---|---|---|
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | - | Dashboard visualisasi metrik analitik historis & peta telemetri real-time |
| **MinIO Web Console** | [http://localhost:9003](http://localhost:9003) | User: `minioadmin`<br>Pass: `minioadmin` | Object Storage UI browser untuk memantau isi bucket Data Lake |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) | - | Status cluster, monitoring workers, streaming jobs, dan alokasi resource |
| **Spark Worker UI** | [http://localhost:8081](http://localhost:8081) | - | Rincian core CPU dan executor memori pada Spark Worker |

---

## 📈 7. Rencana Perkembangan (Roadmap)

- [x] **Eksperimen 1:** Proof of concept Spark standalone + MongoDB.
- [x] **Eksperimen 2:** Pengujian Data Lake terdistribusi untuk data batch historis.
- [x] **Eksperimen 2.1:** Pembersihan repository, optimasi cache Spark Ivy, dan penyesuaian batas konsumsi memori kontainer.
- [x] **MinIO S3 Centralized Data Lake:** Menjadikan MinIO satu-satunya Data Lake terpusat (4 bucket: `raw`, `curated`, `realtime`, `archive`) dan meniadakan arsitektur HDFS ganda.
- [x] **Real-Time Message Streaming (Apache Kafka):** Broker Kafka KRaft mode untuk menyalurkan event status stasiun GBFS v2.3.
- [x] **Automated Spark Structured Streaming:** Konsumsi event streaming Kafka ke MongoDB secara terdistribusi.
- [x] **Decoupled Incremental Stream Archiving:** Pengarsipan periodik terpisah (misal 2 minggu sekali) ke bucket `citibike-archive` via runner CLI.
- [x] **Data Quality & Retention Suite:** Validasi integritas data CSV & GBFS serta pengaturan TTL Index 24 jam pada MongoDB.
- [x] **Automated Pipeline CLI & Container Runner:** Runner PowerShell (`run_pipeline.ps1`), CLI Orchestrator (`pipeline.py`), dan Dockerfile tools runner terisolasi.
- [x] **Automated System Evaluation & Performance Benchmark:** Skrip evaluasi performa mandiri (`scripts/system_evaluator.py`) yang mengukur 8 metrik kunci Tabel 4.1 dan menghasilkan laporan resmi otomatis.
- [ ] **Predictive Machine Learning:** Model prediksi kebutuhan armada dan ketersediaan slot stasiun berbasis cuaca dan historis.
- [ ] **Orchestration Workflow:** Penjadwalan workflow batch harian menggunakan Apache Airflow.

---

## 👥 Kontributor
- **Repository:** [Ariqueeezz/citi-bike-bigdata](https://github.com/Ariqueeezz/citi-bike-bigdata)
- **Branch Aktif:** `eksperimen-2.1`
