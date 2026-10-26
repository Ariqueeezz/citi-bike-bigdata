# ==============================================================================
# Citi Bike NYC Big Data Pipeline Runner (PowerShell Automation)
# ==============================================================================

param(
    [Parameter(Position=0)]
    [ValidateSet("check", "validate", "up", "optimize", "batch", "all", "help")]
    [string]$Action = "help",

    [Parameter()]
    [ValidateSet("local", "hdfs")]
    [string]$Mode = "local",

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
    Write-Host "  1. check     - Periksa file konfigurasi (.env) & status Docker"
    Write-Host "  2. validate  - Jalankan Data Quality Validation Suite"
    Write-Host "  3. up        - Nyalakan semua container Docker Compose"
    Write-Host "  4. optimize  - Terapkan Indexing & TTL Data Retention di MongoDB"
    Write-Host "  5. batch     - Eksekusi Spark Batch Processing & Sinkronisasi MongoDB"
    Write-Host "  6. all       - Jalankan alur End-to-End secara otomatis"
    Write-Host "  7. exit      - Keluar"
    Write-Host ""

    $choice = Read-Host "Masukkan nomor pilihan (1-7)"
    switch ($choice) {
        "1" { & python "$PSScriptRoot\scripts\pipeline.py" check }
        "2" { & python "$PSScriptRoot\scripts\pipeline.py" validate }
        "3" { & python "$PSScriptRoot\scripts\pipeline.py" up }
        "4" { & python "$PSScriptRoot\scripts\pipeline.py" optimize }
        "5" { & python "$PSScriptRoot\scripts\pipeline.py" batch --mode $Mode }
        "6" { & python "$PSScriptRoot\scripts\pipeline.py" all --mode $Mode }
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
