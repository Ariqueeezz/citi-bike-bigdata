# ==============================================================================
# Citi Bike NYC Big Data Pipeline Runner (PowerShell Automation)
# ==============================================================================

param(
    [Parameter(Position=0)]
    [ValidateSet("check", "validate", "up", "minio-setup", "migrate", "optimize", "batch", "stream", "all", "help")]
    [string]$Action = "help",

    [Parameter()]
    [ValidateSet("minio", "local", "hdfs")]
    [string]$Mode = "minio",

    [Parameter()]
    [switch]$Hdfs
)

$PSScriptRoot = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition

function Show-Header {
    Write-Host ""
    Write-Host "======================================================================" -ForegroundColor Cyan
    Write-Host "         CITI BIKE NYC BIG DATA - AUTOMATION PIPELINE RUNNER          " -ForegroundColor Yellow
    Write-Host "======================================================================" -ForegroundColor Cyan
}

function Show-Menu {
    Show-Header
    Write-Host "Pilih aksi yang ingin dijalankan:" -ForegroundColor Green
    Write-Host "  1. check       - Periksa file konfigurasi (.env) & status Docker"
    Write-Host "  2. validate    - Jalankan Data Quality Validation Suite"
    Write-Host "  3. up          - Nyalakan semua container Docker (MongoDB, MinIO, Spark, Streamlit)"
    Write-Host "  4. minio-setup - Buat bucket MinIO & upload data sampel"
    Write-Host "  5. migrate     - Migrasi data dari Hadoop HDFS -> MinIO via Apache Spark"
    Write-Host "  6. optimize    - Terapkan Indexing & TTL Data Retention di MongoDB"
    Write-Host "  7. batch       - Eksekusi Spark Batch Processing (Mode: $Mode) & Sinkronisasi MongoDB"
    Write-Host "  8. stream      - Jalankan Spark Streaming Real-time (GBFS -> MongoDB)"
    Write-Host "  9. all         - Jalankan seluruh alur End-to-End secara otomatis"
    Write-Host "  10. exit       - Keluar"
    Write-Host ""

    $choice = Read-Host "Masukkan nomor pilihan (1-10)"
    switch ($choice) {
        "1" { & python "$PSScriptRoot\scripts\pipeline.py" check }
        "2" { & python "$PSScriptRoot\scripts\pipeline.py" validate }
        "3" { & python "$PSScriptRoot\scripts\pipeline.py" up }
        "4" { & python "$PSScriptRoot\scripts\pipeline.py" minio-setup }
        "5" { & python "$PSScriptRoot\scripts\pipeline.py" migrate }
        "6" { & python "$PSScriptRoot\scripts\pipeline.py" optimize }
        "7" { & python "$PSScriptRoot\scripts\pipeline.py" batch --mode $Mode }
        "8" { & python "$PSScriptRoot\scripts\pipeline.py" stream }
        "9" { & python "$PSScriptRoot\scripts\pipeline.py" all --mode $Mode }
        default { Write-Host "Selesai." -ForegroundColor Gray }
    }
}

if ($Action -eq "help") {
    Show-Menu
} else {
    Show-Header
    $hdfsFlag = if ($Hdfs) { "--hdfs" } else { "" }
    & python "$PSScriptRoot\scripts\pipeline.py" $Action --mode $Mode $hdfsFlag
}
