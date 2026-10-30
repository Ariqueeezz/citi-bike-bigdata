#!/usr/bin/env python3
"""
Citi Bike Big Data Pipeline Orchestrator & Automation CLI
Menangani otomatisasi alur end-to-end terpusat pada MinIO S3 Data Lake:
1. Verifikasi Lingkungan & Kontainer
2. Validasi Kualitas Data (Data Quality Check)
3. Inisialisasi MinIO Object Storage (Buckets: realtime, archive, raw, curated)
4. Pengarsipan Data Stream Inkremental ke MinIO citibike-archive
5. Eksekusi Spark Batch Processing (MinIO S3A -> Curated Parquet -> MongoDB)
6. Optimasi MongoDB (TTL Retention 24 Jam & Indexing)
7. Spark Structured Streaming (GBFS Real-time via Kafka -> MongoDB)
"""

import os
import sys
import subprocess
import argparse
import time

# Pastikan output console di Windows mendukung karakter UTF-8
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def load_environment_variables():
    """Memuat file .env ke environment process jika ada."""
    env_path = os.path.join(PROJECT_ROOT, ".env")
    if os.path.exists(env_path):
        try:
            from dotenv import load_dotenv
            load_dotenv(env_path)
        except ImportError:
            # Fallback parsing .env secara manual tanpa dependensi eksternal
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip())


# Inisialisasi variabel environment saat script dimuat
load_environment_variables()

S3_JARS = (
    "/opt/spark/work-dir/ivy/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,"
    "/opt/spark/work-dir/ivy/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,"
    "/opt/spark/work-dir/ivy/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar"
)

MONGO_JARS = (
    "/opt/spark/work-dir/ivy/jars/org.mongodb.spark_mongo-spark-connector_2.12-10.7.0.jar,"
    "/opt/spark/work-dir/ivy/jars/org.mongodb_bson-5.1.4.jar,"
    "/opt/spark/work-dir/ivy/jars/org.mongodb_mongodb-driver-core-5.1.4.jar,"
    "/opt/spark/work-dir/ivy/jars/org.mongodb_mongodb-driver-sync-5.1.4.jar"
)

KAFKA_JARS = (
    "/opt/spark/work-dir/ivy/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
    "/opt/spark/work-dir/ivy/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,"
    "/opt/spark/work-dir/ivy/jars/kafka-clients-3.4.1.jar,"
    "/opt/spark/work-dir/ivy/jars/commons-pool2-2.11.1.jar"
)

STREAMING_JARS = f"{MONGO_JARS},{KAFKA_JARS}"
ALL_JARS = f"{S3_JARS},{MONGO_JARS},{KAFKA_JARS}"


def print_banner(text):
    print("\n" + "=" * 65)
    print(f"  {text}")
    print("=" * 65)


def run_command(cmd, check=True, cwd=PROJECT_ROOT):
    """Menjalankan perintah shell dan menangani output secara langsung."""
    print(f"\n[RUN] {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd)
    if check and result.returncode != 0:
        print(f"❌ Perintah gagal dengan exit code {result.returncode}")
        sys.exit(result.returncode)
    return result.returncode


def check_environment():
    """Memverifikasi konfigurasi file .env dan status Docker."""
    print_banner("1. MEMERIKSA STATUS LINGKUNGAN KERJA")

    env_path = os.path.join(PROJECT_ROOT, ".env")
    example_env_path = os.path.join(PROJECT_ROOT, ".env.example")

    if not os.path.exists(env_path):
        print("⚠️  File .env tidak ditemukan. Membuat otomatis dari .env.example ...")
        if os.path.exists(example_env_path):
            with open(example_env_path, "r", encoding="utf-8") as f_in, open(env_path, "w", encoding="utf-8") as f_out:
                f_out.write(f_in.read())
            print("✅ File .env berhasil dibuat.")
            load_environment_variables()
        else:
            print("❌ File .env.example tidak ditemukan. Silakan konfigurasi kredensial MongoDB & MinIO.")
    else:
        print("✅ File .env tersedia.")

    # Cek Docker daemon
    print("Memeriksa koneksi Docker Daemon ...")
    docker_check = subprocess.run("docker info", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if docker_check.returncode != 0:
        print("⚠️  Docker Desktop belum berjalan atau daemon tidak merespons.")
        print("   Silakan buka aplikasi Docker Desktop di Windows Anda terlebih dahulu.")
        return False
    print("✅ Docker Daemon aktif dan siap digunakan.")

    # Cek container
    print("\nStatus Kontainer Proyek:")
    subprocess.run("docker compose ps", shell=True, cwd=PROJECT_ROOT)
    return True


def run_data_quality():
    """Menjalankan suite validasi kualitas data."""
    print_banner("2. MENJALANKAN VALIDASI KUALITAS DATA")
    validator_path = os.path.join(os.path.dirname(__file__), "data_quality_validator.py")
    return run_command(f'"{sys.executable}" "{validator_path}"')


def start_infrastructure():
    """Menyalakan container docker compose."""
    print_banner("3. MENYALAKAN INFRASTRUKTUR DOCKER (MINIO, MONGO, KAFKA, SPARK, STREAMLIT)")
    cmd = "docker compose up -d"
    run_command(cmd)
    print("✅ Kontainer berhasil dinyalakan.")


def run_minio_setup():
    """Menginisialisasi bucket MinIO dan mengunggah data sampel jika tersedia."""
    print_banner("INISIALISASI MINIO DATA LAKE BUCKETS")
    mgr_path = os.path.join(os.path.dirname(__file__), "minio_manager.py")
    run_command(f'"{sys.executable}" "{mgr_path}" init')
    run_command(f'"{sys.executable}" "{mgr_path}" upload')
    run_command(f'"{sys.executable}" "{mgr_path}" list')


def run_stream_archive(days=None):
    """Menjalankan pengarsipan berkala data stream ke MinIO citibike-archive."""
    print_banner("PENGARSIPAN DATA STREAM KE MINIO DATA LAKE (CITIBIKE-ARCHIVE)")
    mgr_path = os.path.join(os.path.dirname(__file__), "minio_manager.py")
    cmd = f'"{sys.executable}" "{mgr_path}" archive'
    if days:
        cmd += f" --days {days}"
    return run_command(cmd)


def run_mongo_optimization():
    """Menjalankan script optimasi index dan TTL retention MongoDB."""
    print_banner("OPTIMASI MONGODB (INDEXING & TTL DATA RETENTION)")
    opt_path = os.path.join(os.path.dirname(__file__), "mongo_optimizer.py")
    return run_command(f'"{sys.executable}" "{opt_path}"')


def run_streaming_pipeline():
    """Menjalankan Spark Structured Streaming untuk data realtime GBFS Kafka ke MongoDB."""
    print_banner("MENJALANKAN SPARK STREAMING (REALTIME GBFS VIA KAFKA -> MONGODB)")
    script = "/opt/spark/work-dir/streaming/citibike_realtime.py"
    cmd = (
        f"docker exec -d citibike-spark-master /opt/spark/bin/spark-submit "
        f"--master spark://spark-master:7077 "
        f"--jars {STREAMING_JARS} "
        f"{script}"
    )
    print("Memulai Spark Streaming di background Spark Master...")
    run_command(cmd)
    print("✅ Spark Streaming berhasil dijalankan di background.")


def run_batch_processing():
    """Menjalankan Spark Batch Processing terpusat di MinIO Data Lake."""
    print_banner("4. MENJALANKAN SPARK BATCH PROCESSING (MINIO S3A DATA LAKE)")
    batch_script = "/opt/spark/work-dir/batch/citibike_batch.py"
    print("Mengeksekusi Spark Batch Processing dengan MinIO Data Lake ...")
    cmd = (
        f"docker exec -e SPARK_SUBMIT_OPTS=\"-Duser.home=/tmp\" citibike-spark-master /opt/spark/bin/spark-submit "
        f"--master spark://spark-master:7077 "
        f"--jars {ALL_JARS} "
        f"{batch_script}"
    )
    run_command(cmd)
    print("\n✅ Batch Processing MinIO & Sinkronisasi MongoDB Selesai Sukses!")


def run_pipeline_end_to_end():
    """Menjalankan seluruh alur kerja secara terpadu terpusat di MinIO."""
    print_banner("MEMULAI OTOMASI PIPELINE END-TO-END (MINIO DATA LAKE)")
    docker_ok = check_environment()
    if not docker_ok:
        print("\nSilakan nyalakan Docker Desktop terlebih dahulu untuk melanjutkan eksekusi pipeline.")
        sys.exit(1)

    start_infrastructure()
    print("\nMenunggu layanan stabil (5 detik) ...")
    time.sleep(5)

    run_data_quality()
    run_minio_setup()
    run_stream_archive()
    run_mongo_optimization()
    run_batch_processing()

    print_banner("PIPELINE SELESAI DENGAN SUKSES! 🎉")
    print("Dashboard Streamlit : http://localhost:8501")
    print("MinIO Web Console   : http://localhost:9003")


def run_system_evaluation():
    """Menjalankan suite pengujian dan evaluasi kinerja sistem (Tabel 4.1)."""
    print_banner("PENGUJIAN DAN EVALUASI SISTEM (TABEL 4.1 BENCHMARK)")
    eval_path = os.path.join(os.path.dirname(__file__), "system_evaluator.py")
    return run_command(f'"{sys.executable}" "{eval_path}"')


def main():
    parser = argparse.ArgumentParser(
        description="Citi Bike Big Data Automation CLI (MinIO Centralized Architecture)"
    )
    parser.add_argument(
        "action",
        choices=["check", "validate", "up", "minio-setup", "archive", "optimize", "batch", "stream", "evaluate", "all"],
        help="Aksi: check, validate, up, minio-setup, archive, optimize, batch, stream, evaluate (Tabel 4.1), all."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Batasan rentang hari arsip stream (misal: 14 untuk 2 minggu terakhir). Default: arsipkan semua file belum terarsip."
    )

    args = parser.parse_args()

    if args.action == "check":
        check_environment()
    elif args.action == "validate":
        run_data_quality()
    elif args.action == "up":
        start_infrastructure()
    elif args.action == "minio-setup":
        run_minio_setup()
    elif args.action == "archive":
        run_stream_archive(days=args.days)
    elif args.action == "optimize":
        run_mongo_optimization()
    elif args.action == "batch":
        run_batch_processing()
    elif args.action == "stream":
        run_streaming_pipeline()
    elif args.action == "evaluate":
        run_system_evaluation()
    elif args.action == "all":
        run_pipeline_end_to_end()


if __name__ == "__main__":
    main()
