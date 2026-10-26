#!/usr/bin/env python3
"""
Citi Bike Big Data Pipeline Orchestrator & Automation CLI
Menangani otomatisasi alur end-to-end:
1. Verifikasi Lingkungan & Kontainer
2. Validasi Kualitas Data (Data Quality Check)
3. Eksekusi Spark Batch Processing (Local / HDFS)
4. Optimasi MongoDB (TTL Retention & Indexing)
5. Ekspor Analitik ke MongoDB
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
        else:
            print("❌ File .env.example tidak ditemukan. Silakan konfigurasi kredensial MongoDB.")
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
    return run_command(f"python \"{validator_path}\"")


def start_infrastructure(with_hdfs=False):
    """Menyalakan container docker compose."""
    print_banner("3. MENYALAKAN INFRASTRUKTUR DOCKER")
    if with_hdfs:
        print("Menyalakan mode HDFS (docker-compose.yml + docker-compose.hdfs.yml) ...")
        cmd = "docker compose -f docker-compose.yml -f docker-compose.hdfs.yml up -d"
    else:
        print("Menyalakan mode Standar (docker-compose.yml) ...")
        cmd = "docker compose up -d"
    run_command(cmd)
    print("✅ Kontainer berhasil dinyalakan.")


def run_mongo_optimization():
    """Menjalankan script optimasi index dan TTL retention MongoDB."""
    print_banner("OPTIMASI MONGODB (INDEXING & TTL DATA RETENTION)")
    opt_path = os.path.join(os.path.dirname(__file__), "mongo_optimizer.py")
    return run_command(f"python \"{opt_path}\"")


def run_batch_processing(mode="local"):
    """Menjalankan Spark Batch Processing di dalam kontainer Spark Master."""
    print_banner(f"4. MENJALANKAN SPARK BATCH PROCESSING (Mode: {mode.upper()})")

    if mode == "local":
        script_in_container = "/opt/spark/work-dir/batch/citibike_batch.py"
        cmd = (
            f"docker exec citibike-spark-master /opt/spark/bin/spark-submit "
            f"--master spark://spark-master:7077 {script_in_container}"
        )
        run_command(cmd)

        print("\nMemuat data hasil olahan ke MongoDB...")
        load_mongo_script = "/opt/spark/work-dir/batch/load_mongodb.py"
        load_cmd = (
            f"docker exec citibike-spark-master /opt/spark/bin/spark-submit "
            f"--master spark://spark-master:7077 "
            f"--jars /opt/spark/work-dir/ivy/jars/org.mongodb.spark_mongo-spark-connector_2.12-10.7.0.jar,"
            f"/opt/spark/work-dir/ivy/jars/org.mongodb_bson-5.1.4.jar,"
            f"/opt/spark/work-dir/ivy/jars/org.mongodb_mongodb-driver-core-5.1.4.jar,"
            f"/opt/spark/work-dir/ivy/jars/org.mongodb_mongodb-driver-sync-5.1.4.jar "
            f"{load_mongo_script}"
        )
        run_command(load_cmd)

    elif mode == "hdfs":
        batch_script = "/opt/spark/work-dir/integration/batch_hdfs.py"
        agg_script = "/opt/spark/work-dir/integration/aggregate_hdfs.py"
        export_script = "/opt/spark/work-dir/integration/export_mongo.py"

        print("Langkah A: Ingest & Clean ke Curated HDFS ...")
        run_command(f"docker exec citibike-spark-master /opt/spark/bin/spark-submit {batch_script}")

        print("\nLangkah B: Agregasi Analitik HDFS ...")
        run_command(f"docker exec citibike-spark-master /opt/spark/bin/spark-submit {agg_script}")

        print("\nLangkah C: Ekspor Agregasi HDFS ke MongoDB ...")
        export_cmd = (
            f"docker exec citibike-spark-master /opt/spark/bin/spark-submit "
            f"--jars /opt/spark/work-dir/ivy/jars/org.mongodb.spark_mongo-spark-connector_2.12-10.7.0.jar,"
            f"/opt/spark/work-dir/ivy/jars/org.mongodb_bson-5.1.4.jar,"
            f"/opt/spark/work-dir/ivy/jars/org.mongodb_mongodb-driver-core-5.1.4.jar,"
            f"/opt/spark/work-dir/ivy/jars/org.mongodb_mongodb-driver-sync-5.1.4.jar "
            f"{export_script}"
        )
        run_command(export_cmd)

    print(f"\n✅ Batch Processing ({mode.upper()}) & Sinkronisasi MongoDB Selesai Sukses!")


def run_pipeline_end_to_end(mode="local", with_hdfs=False):
    """Menjalankan seluruh alur kerja secara terpadu."""
    print_banner("MEMULAI OTOMASI PIPELINE END-TO-END")
    docker_ok = check_environment()
    if not docker_ok:
        print("\nSilakan nyalakan Docker Desktop terlebih dahulu untuk melanjutkan eksekusi pipeline.")
        sys.exit(1)

    start_infrastructure(with_hdfs=with_hdfs)
    print("\nMenunggu layanan stabil (5 detik) ...")
    time.sleep(5)

    run_data_quality()
    run_mongo_optimization()
    run_batch_processing(mode=mode)

    print_banner("PIPELINE SELESAI DENGAN SUKSES! 🎉")
    print("Dashboard Streamlit dapat diakses di browser: http://localhost:8501")


def main():
    parser = argparse.ArgumentParser(
        description="Citi Bike Big Data Automation CLI"
    )
    parser.add_argument(
        "action",
        choices=["check", "validate", "up", "optimize", "batch", "all"],
        help="Aksi yang ingin dijalankan: check (cek env), validate (uji kualitas data), up (nyalakan container), optimize (pasang TTL & index Mongo), batch (jalankan batch), all (jalankan seluruh alur)."
    )
    parser.add_argument(
        "--mode",
        choices=["local", "hdfs"],
        default="local",
        help="Mode penyimpanan data: local (Exp 1) atau hdfs (Exp 2). Default: local"
    )
    parser.add_argument(
        "--hdfs",
        action="store_true",
        help="Sertakan cluster HDFS saat menjalankan 'up' atau 'all'."
    )

    args = parser.parse_args()

    if args.action == "check":
        check_environment()
    elif args.action == "validate":
        run_data_quality()
    elif args.action == "up":
        start_infrastructure(with_hdfs=(args.hdfs or args.mode == "hdfs"))
    elif args.action == "optimize":
        run_mongo_optimization()
    elif args.action == "batch":
        run_batch_processing(mode=args.mode)
    elif args.action == "all":
        run_pipeline_end_to_end(mode=args.mode, with_hdfs=(args.hdfs or args.mode == "hdfs"))


if __name__ == "__main__":
    main()
