# ==============================================================================
# Citi Bike NYC Big Data Pipeline Runner (PowerShell Automation)
# Arsitektur Terpusat: MinIO S3 Data Lake + Apache Kafka + Apache Spark
# ==============================================================================

param(
    [Parameter(Position=0)]
    [ValidateSet("check", "validate", "up", "minio-setup", "archive", "optimize", "batch", "stream", "evaluate", "all", "help")]
    [string]$Action = "help",

    [Parameter()]
    [int]$Days = 0,

    [Parameter()]
    [switch]$Docker
)

$PSScriptRoot = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition

function Show-Header {
    Write-Host ""
    Write-Host "======================================================================" -ForegroundColor Cyan
    Write-Host "         CITI BIKE NYC BIG DATA - AUTOMATION PIPELINE RUNNER          " -ForegroundColor Yellow
    Write-Host "                (MinIO S3 Centralized Data Lake)                      " -ForegroundColor DarkGray
    Write-Host "======================================================================" -ForegroundColor Cyan
}

function Find-PythonExecutable {
    $commands = @("python", "python3", "py")
    foreach ($cmd in $commands) {
        $c = Get-Command $cmd -ErrorAction SilentlyContinue
        if ($c) {
            $test = & $cmd -c "import sys; print(sys.executable)" 2>&1
            if ($LASTEXITCODE -eq 0 -and $test) {
                return $test.ToString().Trim()
            }
        }
    }
    return $null
}

function Get-RequiredModulesForAction {
    param([string]$TargetAction)

    switch ($TargetAction) {
        "validate"    { return @("pandas") }
        "minio-setup" { return @("minio") }
        "archive"     { return @("minio") }
        "optimize"    { return @("pymongo") }
        "evaluate"    { return @("pandas", "pymongo") }
        "all"         { return @("pandas", "minio", "pymongo") }
        default       { return @() }
    }
}

function Ensure-LocalDependencies {
    param(
        [string]$PythonExe,
        [string]$TargetAction
    )

    $reqFile = Join-Path $PSScriptRoot "requirements.txt"
    if (-not (Test-Path $reqFile)) {
        return
    }

    $required = Get-RequiredModulesForAction -TargetAction $TargetAction
    if ($required.Count -eq 0) {
        return
    }

    # Cek modul yang belum terpasang di Python lokal
    $missing = @()
    foreach ($mod in $required) {
        & $PythonExe -c "import $mod" 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            $missing += $mod
        }
    }

    if ($missing.Count -gt 0) {
        $missingStr = $missing -join ", "
        Write-Host ""
        Write-Host "[Auto-Setup] Modul '$missingStr' belum terpasang di Python lokal." -ForegroundColor Yellow
        Write-Host "[Auto-Setup] Menginstall dependensi otomatis dari requirements.txt..." -ForegroundColor Cyan
        & $PythonExe -m pip install -r $reqFile --retries 1 --timeout 5
        if ($LASTEXITCODE -eq 0) {
            Write-Host "[Auto-Setup] Seluruh dependensi berhasil dipasang otomatis!" -ForegroundColor Green
            Write-Host ""
        } else {
            Write-Host "[Auto-Setup] Pemasangan via pip tidak berhasil (mis. tidak ada koneksi internet)." -ForegroundColor Yellow
            Write-Host "             Tips: Anda dapat menjalankan via container Docker tanpa install lokal:" -ForegroundColor Gray
            Write-Host "                   .\run_pipeline.ps1 -Docker $TargetAction" -ForegroundColor Gray
            Write-Host ""
        }
    }
}

function Invoke-Pipeline {
    param(
        [string]$TargetAction,
        [int]$TargetDays,
        [bool]$UseDocker
    )

    $cmdArgs = @($TargetAction)
    if ($TargetDays -gt 0) {
        $cmdArgs += "--days"
        $cmdArgs += $TargetDays.ToString()
    }

    if ($UseDocker) {
        Write-Host "Menjalankan pipeline dalam isolasi container Docker (pipeline-runner)..." -ForegroundColor Cyan
        & docker compose run --rm pipeline-runner @cmdArgs
        return
    }

    $pythonExe = Find-PythonExecutable
    if (-not $pythonExe) {
        Write-Host "Python tidak ditemukan di sistem lokal Anda." -ForegroundColor Yellow
        Write-Host "Beralih otomatis menjalankan via container Docker..." -ForegroundColor Cyan
        & docker compose run --rm pipeline-runner @cmdArgs
        return
    }

    # Pastikan dependensi yang dibutuhkan terpasang otomatis
    Ensure-LocalDependencies -PythonExe $pythonExe -TargetAction $TargetAction

    # Eksekusi pipeline orchestrator lokal
    & $pythonExe "$PSScriptRoot\scripts\pipeline.py" @cmdArgs
}

function Show-Menu {
    Show-Header
    $modeText = if ($Docker) { "Container Docker" } else { "Python Lokal (Auto-Setup Dependensi)" }
    Write-Host "Lingkungan Eksekusi: $modeText | Data Lake: MinIO S3" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "Pilih aksi yang ingin dijalankan:" -ForegroundColor Green
    Write-Host "  1. check       - Periksa file konfigurasi (.env) dan status Docker"
    Write-Host "  2. validate    - Jalankan Data Quality Validation Suite"
    Write-Host "  3. up          - Nyalakan semua container Docker (Kafka, MinIO, MongoDB, Spark, Streamlit)"
    Write-Host "  4. minio-setup - Buat bucket MinIO Data Lake dan upload data sampel"
    Write-Host "  5. archive     - Arsipkan data stream ke MinIO citibike-archive (Inkremental / Partisi Tanggal)"
    Write-Host "  6. optimize    - Terapkan Indexing dan TTL Data Retention di MongoDB"
    Write-Host "  7. batch       - Eksekusi Spark Batch Processing (MinIO S3A) dan Sinkronisasi MongoDB"
    Write-Host "  8. stream      - Jalankan Spark Streaming Real-time (GBFS -> Kafka -> MongoDB)"
    Write-Host "  9. evaluate    - Jalankan Evaluasi Kinerja Sistem & Pengukuran Benchmark (Tabel 4.1)"
    Write-Host "  10. all        - Jalankan seluruh alur End-to-End secara otomatis"
    Write-Host "  11. exit       - Keluar"
    Write-Host ""

    $choice = Read-Host "Masukkan nomor pilihan (1-11)"
    switch ($choice) {
        "1" { Invoke-Pipeline -TargetAction "check" -TargetDays $Days -UseDocker $Docker }
        "2" { Invoke-Pipeline -TargetAction "validate" -TargetDays $Days -UseDocker $Docker }
        "3" { Invoke-Pipeline -TargetAction "up" -TargetDays $Days -UseDocker $Docker }
        "4" { Invoke-Pipeline -TargetAction "minio-setup" -TargetDays $Days -UseDocker $Docker }
        "5" { Invoke-Pipeline -TargetAction "archive" -TargetDays $Days -UseDocker $Docker }
        "6" { Invoke-Pipeline -TargetAction "optimize" -TargetDays $Days -UseDocker $Docker }
        "7" { Invoke-Pipeline -TargetAction "batch" -TargetDays $Days -UseDocker $Docker }
        "8" { Invoke-Pipeline -TargetAction "stream" -TargetDays $Days -UseDocker $Docker }
        "9" { Invoke-Pipeline -TargetAction "evaluate" -TargetDays $Days -UseDocker $Docker }
        "10" { Invoke-Pipeline -TargetAction "all" -TargetDays $Days -UseDocker $Docker }
        default { Write-Host "Selesai." -ForegroundColor Gray }
    }
}

if ($Action -eq "help") {
    Show-Menu
} else {
    Show-Header
    Invoke-Pipeline -TargetAction $Action -TargetDays $Days -UseDocker $Docker
}
