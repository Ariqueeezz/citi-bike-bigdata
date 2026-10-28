# 🚴‍♂️ Citi Bike Big Data Analytics & Real-Time Monitoring Platform

[![Branch](https://img.shields.io/badge/branch-eksperimen--2.1-blue.svg)](https://github.com/Ariqueeezz/citi-bike-bigdata)
[![Status](https://img.shields.io/badge/status-active-success.svg)](#)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.8.0%20(KRaft)-black.svg)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.7-E25A1C.svg)](https://spark.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3%20Data%20Lake-C72C48.svg)](https://min.io/)
[![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-HDFS%203.2.1-yellow.svg)](https://hadoop.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-8.0-47A248.svg)](https://www.mongodb.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.50.0-FF4B4B.svg)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Docker%20Compose-v2-2496ED.svg)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB.svg)](https://www.python.org/)

---

## 📌 1. Gambaran Umum Proyek

Proyek **Citi Bike Big Data** adalah platform *end-to-end distributed data engineering* dan *analytics* terpadu yang dirancang untuk mengolah, menganalisis, serta memantau operasional armada sepeda berbagi terbesar di New York City (**Citi Bike / Lyft**).

Sistem ini mengadopsi pola **Hybrid / Lambda Architecture** modern yang memisahkan beban kerja analitik data historis skala besar (*batch processing*) dan pemantauan kondisi armada secara langsung (*real-time streaming*):

1. **Batch Processing Pipeline (Historical Trip Analytics):**
   * Mengolah jutaan baris data mentah perjalanan bulanan NYC Citi Bike (format CSV).
   * Mendukung penyimpanan terdistribusi ganda: **MinIO Object Storage (S3-compatible Data Lake)** dan **Apache Hadoop HDFS**, lengkap dengan skrip migrasi data otomatis (*HDFS $\rightarrow$ MinIO*).
   * Pembersihan data dan agregasi analitik terdistribusi dijalankan oleh klaster **Apache Spark 3.5.7**, mengekstrak wawasan tren mobilitas perkotaan (jam sibuk pagi & sore, segmentasi *Subscriber vs Casual*, durasi rata-rata, dan pemeringkatan stasiun terpadat).
   * Hasil agregasi disimpan secara terstruktur di **MongoDB 8.0** untuk kebutuhan visualisasi.

2. **Real-Time Streaming Pipeline (Live Fleet & Station Telemetry):**
   * Pengambilan telemetri publik langsung dari **Citi Bike GBFS v2.3 API** (*General Bikeshare Feed Specification*) setiap 60 detik melalui *daemon collector*.
   * Mengalirkan snapshot telemetri stasiun ke message broker **Apache Kafka 3.8.0 (KRaft Mode)** pada topik `citibike-station-status` dengan replikasi pesan berkecepatan tinggi.
   * **Spark Structured Streaming** mengonsumsi pesan Kafka secara *real-time*, mem-parsing payload JSON, memvalidasi integritas data, dan melakukan *upsert* langsung ke koleksi MongoDB `realtime_station_status`.
   * Snapshot telemetri secara simultan diarsipkan ke bucket **MinIO** (`citibike-realtime` & `citibike-archive`) sebagai rekaman historis mentah.

3. **Data Quality Gate & Database Optimization:**
   * Modul validasi kualitas data (**Data Quality Validation Suite**) untuk menyaring data batch yang anomali (tanggal di luar periode target, durasi perjalanan $\le 0$ atau $> 24$ jam, serta *null timestamp*) dan memverifikasi metrik telemetri real-time.
   * Optimasi MongoDB melalui **TTL (Time-To-Live) Index** otomatis yang memangkas data streaming berumur $> 24$ jam, dilengkapi *compound performance indexes* untuk menjamin latensi rendah pada query dashboard.

4. **Serving & Visualization Layer:**
   * Dashboard interaktif multi-tab berbasis **Streamlit 1.50** yang dilengkapi peta spasial interaktif NYC, pemantauan status armada real-time (auto-refresh 30 detik), deteksi stasiun kritis (*empty/full docks*), serta visualisasi tren historis.

5. **Automated Pipeline Orchestration:**
   * Dilengkapi runner otomasi **PowerShell (`run_pipeline.ps1`)** dan CLI Python (`scripts/pipeline.py`) yang mendukung eksekusi lokal dengan instalasi dependensi otomatis maupun eksekusi terisolasi tanpa instalasi lokal menggunakan **Docker Container Runner (`Dockerfile`)**.

---

## 🏗️ 2. Arsitektur Sistem Terkini

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
|    Data Lake Ingestion Manager    |                                           |      citibike-realtime-collector      |
|    (scripts/minio_manager.py)     |                                           |     (collector/realtime/collector.py) |
+-----------------------------------+                                           +---------------------------------------+
       |                           |                                                       |                     |
       v (S3 Put)                  v (HDFS Put)                                            v (Kafka Producer)    v (S3 Archive)
+-----------------------+   +-----------------------+                           +---------------------+   +---------------------+
|      MinIO S3         |   |      Hadoop HDFS      |                           |    Apache Kafka     |   |      MinIO S3       |
|  citibike-raw bucket  |   |  (hdfs://namenode)    |                           |     (KRaft Broker)  |   |  citibike-realtime  |
+-----------------------+   +-----------------------+                           | Topic: citibike-    |   |  citibike-archive   |
       |                           |                                            | station-status      |   +---------------------+
       |                           | (Migration via Spark)                      +---------------------+
       +-------------+-------------+                                                       |
                     |                                                                     v (Consumer / Structured Streaming)
                     v                                                          +-----------------------------------------------+
+-------------------------------------------------------------+                 |          Apache Spark 3.5.7 Cluster           |
|                 Apache Spark 3.5.7 Cluster                  |                 |             (Structured Streaming)            |
|                     (Batch Processing)                      |                 |   Script: spark/streaming/citibike_realtime.py|
|   Script: spark/batch/citibike_batch_minio.py               |                 +-----------------------------------------------+
+-------------------------------------------------------------+                                        |
       |                                             |                                                 |
       v (Curated Parquet write)                     v (Batch Aggregations write)                      v (Streaming Upsert)
+-----------------------------+               +-------------------------------------------------------------------------+
|          MinIO S3           |               |                               MongoDB 8.0                               |
|   citibike-curated bucket   |               | Database: citibike                                                      |
+-----------------------------+               | Collections:                                                            |
                                              |   • exp2_hourly_stats       (Agregasi perjalanan per jam)               |
                                              |   • exp2_user_type_stats    (Perbandingan Member vs Casual)             |
                                              |   • exp2_station_stats      (Peringkat stasiun terpopuler)              |
                                              |   • realtime_station_status (Telemetri armada live - TTL 24 Jam)        |
                                              |   • historical_*            (Baseline koleksi Eksperimen 1)             |
                                              +-------------------------------------------------------------------------+
                                                                                           |
                                                                                           v (PyMongo queries)
                                              +-------------------------------------------------------------------------+
                                              |                     Streamlit Dashboard (Port 8501)                     |
                                              |   • Tab 1: Historical Baseline (Experiment 1 Standalone Spark)          |
                                              |   • Tab 2: Distributed Batch Analytics (Experiment 2 MinIO / HDFS)      |
                                              |   • Tab 3: Real-Time Fleet & Station Monitoring (Map, Alerts, Trends)   |
                                              +-------------------------------------------------------------------------+

+-----------------------------------------------------------------------------------------------------------------------+
|                                     ORCHESTRATION & QUALITY GATE (AUTOMATION)                                         |
|  • PowerShell Runner : .\run_pipeline.ps1 [check | validate | up | minio-setup | migrate | optimize | batch | stream] |
|  • Container Runner  : Dockerfile & docker-compose.yml (Service: pipeline-runner with auto-setup)                      |
|  • Data Quality Gate : scripts/data_quality_validator.py  |  MongoDB Optimizer: scripts/mongo_optimizer.py             |
+-----------------------------------------------------------------------------------------------------------------------+
```

---

## ⚙️ 3. Tech Stack & Komponen Sistem

| Komponen | Teknologi & Versi | Peran & Detail Konfigurasi | Port Eksternal |
|---|---|---|---|
| **Message Streaming Broker** | **Apache Kafka 3.8.0** (`apache/kafka:3.8.0`) | *Distributed message streaming* dalam mode **KRaft** (tanpa ZooKeeper). Menyerap stream snapshot status stasiun secara real-time pada topik `citibike-station-status`. Limit memori: 768MB. | `9092` (Internal), `9094` (Host) |
| **Cloud Object Storage (Data Lake)** | **MinIO S3** (`minio-community:local`) | *Object storage* berkinerja tinggi kompatibel Amazon S3. Menampung 4 bucket: `citibike-realtime`, `citibike-archive`, `citibike-raw`, dan `citibike-curated`. | `9002` (S3 API), `9003` (Web Console) |
| **Distributed Storage (HDFS)** | **Apache Hadoop HDFS** (`bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`) | Penyimpanan blok terdistribusi untuk eksperimen perbandingan dan sumber migrasi data ke MinIO. | `9870` (NameNode Web), `9000` (RPC) |
| **Distributed Data Processing** | **Apache Spark 3.5.7** (`apache/spark:3.5.7`) | Pemrosesan Batch ETL dan Structured Streaming (1 Master, 1 Worker dengan 2 CPU cores, RAM 1.5GB). Dilengkapi driver S3A AWS SDK dan MongoDB Spark Connector. | `8080` (Master Web), `8081` (Worker Web), `7077` (Spark RPC) |
| **Serving Database** | **MongoDB 8.0** (`mongo:8`) | Database NoSQL dokumen untuk menyajikan data analitik dan snapshot real-time. Dilengkapi **TTL Index (retensi 24 jam)** dan *performance indexing*. Limit memori: 1GB. | `27017` |
| **Real-Time Data Collector** | **Python 3.11** (`collector/realtime/collector.py`) | *Service* polling feed GBFS Lyft setiap 60 detik. Menerbitkan event ke Apache Kafka dan mengarsipkan snapshot ke MinIO & storage lokal. Limit memori: 256MB. | Background daemon |
| **Web Visualization** | **Streamlit 1.50** (`app/streamlit/app.py`) | Antarmuka dashboard analitik 3-Tab interaktif dengan visualisasi metrik agregat, peringatan stasiun kritis (*empty/full*), dan peta spasial NYC. | `8501` |
| **Pipeline Containerization** | **Docker & Dockerfile** (`python:3.11-slim`) | Container image mandiri (`citibike-pipeline:latest`) yang membundle seluruh dependensi Python dan Docker CLI untuk eksekusi pipeline zero-setup di host. | - |
| **Automation Runner** | **PowerShell & Python Orchestrator** (`run_pipeline.ps1`, `pipeline.py`) | Skrip orkestrasi modular dengan auto-setup dependensi dari `requirements.txt` dan dukungan container flag (`-Docker`). | CLI |

---

## 📁 4. Struktur Direktori Proyek

```text
citi-bike-bigdata/
├── .dockerignore                     # Rule pengabaian build image Docker
├── .env                              # Variabel konfigurasi lingkungan & kredensial
├── .env.example                      # Template referensi variabel konfigurasi
├── .gitignore                        # Konfigurasi ignore file git lokal & data lake
├── docker-compose.yml                # Compose utama (Kafka, MinIO, Mongo, Spark, Streamlit, Collector, Pipeline-Runner)
├── docker-compose.hdfs.yml           # Compose klaster opsional HDFS (NameNode & DataNode)
├── docker-compose.exp1.backup.yml    # Arsip konfigurasi Eksperimen 1
├── Dockerfile                        # Dockerfile utama container pipeline-runner & tools
├── Dockerfile.minio                  # Dockerfile build MinIO community image
├── requirements.txt                  # Daftar lengkap dependensi Python proyek
├── run_pipeline.ps1                  # Automation runner interaktif PowerShell (Lokal & Docker mode)
│
├── app/
│   └── streamlit/
│       ├── app.py                    # Aplikasi dashboard Streamlit (3 Tab: Exp 1, Exp 2, Real-Time)
│       └── requirements.txt          # Dependensi khusus modul Streamlit
│
├── collector/
│   └── realtime/
│       ├── collector.py              # Poller GBFS real-time -> Kafka & MinIO
│       └── requirements.txt          # Dependensi poller (requests, minio, kafka-python-ng)
│
├── data/                             # Direktori data lokal (volume mount)
│   ├── raw/                          # File CSV data mentah historis (contoh: 2026-01)
│   ├── processed/                    # Output perantara analitik
│   └── realtime/                     # Snapshot berkala JSON GBFS
│
├── scripts/                          # Suite otomasi data & database
│   ├── pipeline.py                   # Orchestrator CLI Python end-to-end
│   ├── data_quality_validator.py     # Validator kualitas data batch CSV & real-time GBFS
│   ├── minio_manager.py              # Inisialisasi bucket MinIO, upload, & inspeksi objek
│   └── mongo_optimizer.py            # Pembuat TTL Index (24 jam) & Compound Performance Index
│
└── spark/                            # Modul Apache Spark terdistribusi
    ├── batch/
    │   ├── citibike_batch_minio.py   # Batch processing Spark menggunakan MinIO S3 & Mongo
    │   ├── citibike_batch.py         # Batch processing baseline (Eksperimen 1)
    │   └── load_mongodb.py           # Utilitas pemuat data ke MongoDB
    ├── integration/
    │   └── migrate_hdfs_to_minio.py  # Skrip migrasi data HDFS -> MinIO via Apache Spark
    ├── streaming/
    │   └── citibike_realtime.py      # Spark Structured Streaming (Kafka -> MongoDB)
    └── ivy/
        └── jars/                     # Dependensi JAR (Hadoop AWS, Mongo Connector, Kafka Clients)
```

---

## 📊 5. Fitur Utama & Modul Analitik

### A. Batch Analytics & MinIO Data Lake
* **MinIO Object Storage Integration:** Pengelolaan data mentah (`citibike-raw`), data olahan teragregasi (`citibike-curated`), serta migrasi seamless dari klaster HDFS.
* **Agregasi Mobilitas Urban:** Perhitungan volume perjalanan per jam, identifikasi *rush hours* komuter (08:00 pagi dan 17:00-18:00 sore), serta tren mingguan.
* **Segmentasi Pengguna:** Analisis perbandingan rasio total perjalanan dan durasi antara anggota reguler (*Member*) dan pengguna kasual (*Casual*).
* **Top Origin Stations:** Peringkat stasiun dengan frekuensi keberangkatan tertinggi di seluruh distrik New York City.

### B. Real-Time Telemetry, Kafka & Spark Streaming
* **High-Throughput Ingestion:** Polling berkala GBFS feed v2.3 Lyft setiap menit, dikirimkan ke topik Kafka `citibike-station-status`.
* **Spark Structured Streaming:** Menjalankan pemrosesan stream secara *continuous*, mem-parsing payload nested JSON, dan melakukan *upsert* ke MongoDB.
* **Early Warning System (Alerting):**
  - 🚨 **Empty Stations:** Peringatan untuk stasiun dengan 0 sepeda tersedia.
  - ⚠️ **Full Stations:** Peringatan untuk stasiun dengan 0 dermaga kosong (pengguna tidak dapat mengembalikan sepeda).
  - ⏱️ **Stale Reporting Stations:** Deteksi stasiun yang tidak mengirimkan pembaruan status lebih dari 1 jam.
* **Peta Interaktif Spasial NYC:** Visualisasi posisi geografis seluruh stasiun Citi Bike dengan filter instan untuk stasiun kritis ($\le 2$ sepeda) dan stasiun surplus ($\ge 10$ sepeda).

### C. Data Quality Gate & Database Optimization
* **Batch Validation:** Pengecekan konsistensi durasi perjalanan, pemfilteran nilai *null*, dan pembuangan trip di luar batas bulan target (misal: trip malam tahun baru 2025-12-31 pada dataset Januari 2026).
* **Real-Time Validation:** Verifikasi Unix epoch timestamp, verifikasi stasiun aktif menyewakan (*active renting*) vs sekadar terpasang secara fisik (*installed*).
* **Automated Data Retention (TTL):** Data status real-time secara otomatis dihapus oleh MongoDB setelah berumur 24 jam sehingga kapasitas memori kontainer tetap hemat dan stabil.
* **Performance Indexing:** Index komposit terpasang pada field `snapshot_time`, `station_id`, `is_installed`, `is_renting`, serta field analitik historis.

---

## 🚀 6. Panduan Menjalankan Sistem

### Persyaratan Awal
- **Docker & Docker Compose v2** terpasang dan Docker Desktop aktif.
- Spesifikasi RAM host yang disarankan: **8 GB** atau lebih (semua kontainer telah dibatasi *memory limit*-nya).
- File konfigurasi `.env` (disalin dari `.env.example`).

---

### Cara Praktis: Menggunakan Pipeline Runner (`run_pipeline.ps1`)

Repository ini dilengkapi dengan script otomasi **PowerShell Runner** yang dapat mendeteksi dependensi, menginstall paket yang kurang secara otomatis, atau berjalan sepenuhnya di dalam kontainer Docker:

#### 1. Masuk ke Menu Interaktif
Buka terminal PowerShell di root folder proyek, lalu jalankan:
```powershell
.\run_pipeline.ps1
```
Menu interaktif akan muncul dengan 10 pilihan aksi otomatis:
```text
======================================================================
         CITI BIKE NYC BIG DATA - AUTOMATION PIPELINE RUNNER          
======================================================================
Lingkungan Eksekusi: Python Lokal (Auto-Setup Dependensi) | Mode Data: minio

Pilih aksi yang ingin dijalankan:
  1. check       - Periksa file konfigurasi (.env) dan status Docker
  2. validate    - Jalankan Data Quality Validation Suite
  3. up          - Nyalakan semua container Docker (MongoDB, MinIO, Spark, Streamlit)
  4. minio-setup - Buat bucket MinIO dan upload data sampel
  5. migrate     - Migrasi data dari Hadoop HDFS ke MinIO via Apache Spark
  6. optimize    - Terapkan Indexing dan TTL Data Retention di MongoDB
  7. batch       - Eksekusi Spark Batch Processing (Mode: minio) dan Sinkronisasi MongoDB
  8. stream      - Jalankan Spark Streaming Real-time (GBFS ke MongoDB)
  9. all         - Jalankan seluruh alur End-to-End secara otomatis
  10. exit       - Keluar
```

#### 2. Menjalankan Perintah Langsung (CLI Command)
Anda dapat memanggil aksi secara langsung tanpa membuka menu:

```powershell
# 1. Periksa kelengkapan file .env dan status Docker
.\run_pipeline.ps1 check

# 2. Jalankan Data Quality Validation
.\run_pipeline.ps1 validate

# 3. Nyalakan seluruh layanan container
.\run_pipeline.ps1 up

# 4. Inisialisasi bucket MinIO dan upload data mentah CSV
.\run_pipeline.ps1 minio-setup

# 5. Pasang TTL Index (24 jam) dan Index performa di MongoDB
.\run_pipeline.ps1 optimize

# 6. Jalankan Spark Batch Analytics menggunakan MinIO Data Lake
.\run_pipeline.ps1 batch --mode minio

# 7. Jalankan Spark Structured Streaming (Kafka -> MongoDB) di background
.\run_pipeline.ps1 stream

# 8. Eksekusi alur lengkap End-to-End secara berurutan
.\run_pipeline.ps1 all
```

#### 3. Menjalankan Menggunakan Container Docker (Zero Local Setup)
Jika Anda tidak ingin menginstall Python atau dependensi modul apapun di komputer lokal Anda, cukup tambahkan switch `-Docker`:

```powershell
# Seluruh dependensi dan script akan dieksekusi di dalam container pipeline-runner:
.\run_pipeline.ps1 -Docker validate
.\run_pipeline.ps1 -Docker optimize
.\run_pipeline.ps1 -Docker all
```

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

# 4. Terapkan index & TTL retention MongoDB
docker compose run --rm pipeline-runner optimize

# 5. Eksekusi Spark Batch Processing
docker exec -e SPARK_SUBMIT_OPTS="-Duser.home=/tmp" citibike-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/work-dir/ivy/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/opt/spark/work-dir/ivy/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/ivy/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar,/opt/spark/work-dir/ivy/jars/org.mongodb.spark_mongo-spark-connector_2.12-10.7.0.jar,/opt/spark/work-dir/ivy/jars/org.mongodb_bson-5.1.4.jar,/opt/spark/work-dir/ivy/jars/org.mongodb_mongodb-driver-core-5.1.4.jar,/opt/spark/work-dir/ivy/jars/org.mongodb_mongodb-driver-sync-5.1.4.jar,/opt/spark/work-dir/ivy/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/ivy/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/ivy/jars/kafka-clients-3.4.1.jar,/opt/spark/work-dir/ivy/jars/commons-pool2-2.11.1.jar \
  /opt/spark/work-dir/batch/citibike_batch_minio.py

# 6. Jalankan Spark Structured Streaming di background
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
| **HDFS NameNode UI** *(Opsional)* | [http://localhost:9870](http://localhost:9870) | - | File browser dan status kesehatan klaster Hadoop HDFS |

---

## 📈 7. Rencana Perkembangan (Roadmap)

- [x] **Eksperimen 1:** Proof of concept Spark standalone + MongoDB.
- [x] **Eksperimen 2:** Integrasi klaster HDFS sebagai Data Lake terdistribusi untuk data batch historis.
- [x] **Eksperimen 2.1:** Pembersihan repository, optimasi cache Spark Ivy, dan penyesuaian batas konsumsi memori kontainer.
- [x] **MinIO S3 Object Storage Data Lake:** Integrasi 4 bucket (`realtime`, `archive`, `raw`, `curated`) dan migrasi data dari HDFS.
- [x] **Real-Time Message Streaming (Apache Kafka):** Broker Kafka KRaft mode untuk menyalurkan event status stasiun GBFS v2.3.
- [x] **Automated Spark Structured Streaming:** Konsumsi event streaming Kafka ke MongoDB secara terdistribusi.
- [x] **Data Quality & Retention Suite:** Validasi integritas data CSV & GBFS serta pengaturan TTL Index 24 jam pada MongoDB.
- [x] **Automated Pipeline CLI & Container Runner:** Runner PowerShell (`run_pipeline.ps1`), CLI Orchestrator (`pipeline.py`), dan Dockerfile tools runner terisolasi.

---

## 👥 Kontributor
- **Repository:** [Ariqueeezz/citi-bike-bigdata](https://github.com/Ariqueeezz/citi-bike-bigdata)
- **Branch Aktif:** `eksperimen-2.1`
