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
    [switch]$Hdfs,

    [Parameter()]
    [switch]$Docker
)

$PSScriptRoot = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition

function Show-Header {
    Write-Host ""
    Write-Host "======================================================================" -ForegroundColor Cyan
    Write-Host "         CITI BIKE NYC BIG DATA - AUTOMATION PIPELINE RUNNER          " -ForegroundColor Yellow
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
        "optimize"    { return @("pymongo") }
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
        [string]$TargetMode,
        [bool]$TargetHdfs,
        [bool]$UseDocker
    )

    $hdfsFlag = if ($TargetHdfs) { "--hdfs" } else { "" }

    if ($UseDocker) {
        Write-Host "Menjalankan pipeline dalam isolasi container Docker (pipeline-runner)..." -ForegroundColor Cyan
        & docker compose run --rm pipeline-runner $TargetAction --mode $TargetMode $hdfsFlag
        return
    }

    $pythonExe = Find-PythonExecutable
    if (-not $pythonExe) {
        Write-Host "Python tidak ditemukan di sistem lokal Anda." -ForegroundColor Yellow
        Write-Host "Beralih otomatis menjalankan via container Docker..." -ForegroundColor Cyan
        & docker compose run --rm pipeline-runner $TargetAction --mode $TargetMode $hdfsFlag
        return
    }

    # Pastikan dependensi yang dibutuhkan terpasang otomatis
    Ensure-LocalDependencies -PythonExe $pythonExe -TargetAction $TargetAction

    # Eksekusi pipeline orchestrator lokal
    & $pythonExe "$PSScriptRoot\scripts\pipeline.py" $TargetAction --mode $TargetMode $hdfsFlag
}

function Show-Menu {
    Show-Header
    $modeText = if ($Docker) { "Container Docker" } else { "Python Lokal (Auto-Setup Dependensi)" }
    Write-Host "Lingkungan Eksekusi: $modeText | Mode Data: $Mode" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "Pilih aksi yang ingin dijalankan:" -ForegroundColor Green
    Write-Host "  1. check       - Periksa file konfigurasi (.env) dan status Docker"
    Write-Host "  2. validate    - Jalankan Data Quality Validation Suite"
    Write-Host "  3. up          - Nyalakan semua container Docker (MongoDB, MinIO, Spark, Streamlit)"
    Write-Host "  4. minio-setup - Buat bucket MinIO dan upload data sampel"
    Write-Host "  5. migrate     - Migrasi data dari Hadoop HDFS ke MinIO via Apache Spark"
    Write-Host "  6. optimize    - Terapkan Indexing dan TTL Data Retention di MongoDB"
    Write-Host "  7. batch       - Eksekusi Spark Batch Processing (Mode: $Mode) dan Sinkronisasi MongoDB"
    Write-Host "  8. stream      - Jalankan Spark Streaming Real-time (GBFS ke MongoDB)"
    Write-Host "  9. all         - Jalankan seluruh alur End-to-End secara otomatis"
    Write-Host "  10. exit       - Keluar"
    Write-Host ""

    $choice = Read-Host "Masukkan nomor pilihan (1-10)"
    switch ($choice) {
        "1" { Invoke-Pipeline -TargetAction "check" -TargetMode $Mode -TargetHdfs $Hdfs -UseDocker $Docker }
        "2" { Invoke-Pipeline -TargetAction "validate" -TargetMode $Mode -TargetHdfs $Hdfs -UseDocker $Docker }
        "3" { Invoke-Pipeline -TargetAction "up" -TargetMode $Mode -TargetHdfs $Hdfs -UseDocker $Docker }
        "4" { Invoke-Pipeline -TargetAction "minio-setup" -TargetMode $Mode -TargetHdfs $Hdfs -UseDocker $Docker }
        "5" { Invoke-Pipeline -TargetAction "migrate" -TargetMode $Mode -TargetHdfs $Hdfs -UseDocker $Docker }
        "6" { Invoke-Pipeline -TargetAction "optimize" -TargetMode $Mode -TargetHdfs $Hdfs -UseDocker $Docker }
        "7" { Invoke-Pipeline -TargetAction "batch" -TargetMode $Mode -TargetHdfs $Hdfs -UseDocker $Docker }
        "8" { Invoke-Pipeline -TargetAction "stream" -TargetMode $Mode -TargetHdfs $Hdfs -UseDocker $Docker }
        "9" { Invoke-Pipeline -TargetAction "all" -TargetMode $Mode -TargetHdfs $Hdfs -UseDocker $Docker }
        default { Write-Host "Selesai." -ForegroundColor Gray }
    }
}

if ($Action -eq "help") {
    Show-Menu
} else {
    Show-Header
    Invoke-Pipeline -TargetAction $Action -TargetMode $Mode -TargetHdfs $Hdfs -UseDocker $Docker
}
