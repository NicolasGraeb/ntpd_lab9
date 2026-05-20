# Uruchom main.py na klastrze Spark w Dockerze (driver + worker, Python 3.8).
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Csv = Join-Path $Root "data\supermarket_sales.csv"
if (-not (Test-Path $Csv)) {
    Write-Error "Brak pliku data\supermarket_sales.csv"
}
Set-Location $Root
docker compose up -d
$mainPy = Join-Path $Root "main.py"
docker compose run --rm -v "${mainPy}:/opt/spark/work/main.py" spark-worker /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work/main.py
