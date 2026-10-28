# ==============================================================================
# Citi Bike Big Data Analytics Platform - Dockerfile
# Container terisolasi untuk menjalankan seluruh alur pipeline & script Python
# tanpa perlu instalasi module / library manual di komputer lokal.
# ==============================================================================

FROM python:3.11-slim

# Konfigurasi environment Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive

# Install paket sistem yang diperlukan
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    procps \
    tar \
    && rm -rf /var/lib/apt/lists/*

# Install Docker CLI statis agar container dapat mengorkestrasi container lain
# via volume mount socket Docker (/var/run/docker.sock) jika dijalankan via container runner
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-26.1.4.tgz | \
    tar -xz -C /tmp && \
    mv /tmp/docker/docker /usr/local/bin/docker && \
    rm -rf /tmp/docker

WORKDIR /workspace

# Copy daftar dependensi terlebih dahulu agar memanfaatkan cache layer Docker
COPY requirements.txt /workspace/requirements.txt

# Install seluruh pustaka Python yang dibutuhkan proyek
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /workspace/requirements.txt

# Copy seluruh kode proyek ke dalam container
COPY . /workspace

# Entrypoint default mengarah ke orchestrator pipeline
ENTRYPOINT ["python", "scripts/pipeline.py"]
CMD ["help"]
