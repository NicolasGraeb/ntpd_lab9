# Uruchom main.py na hoście (local[*]) z JDK 17 — wymagane przez Spark 3.5.
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Jdk = Get-ChildItem "C:\Program Files\Eclipse Adoptium\jdk-17*" -ErrorAction SilentlyContinue |
    Sort-Object Name -Descending |
    Select-Object -First 1
if (-not $Jdk) {
    Write-Error "Nie znaleziono JDK 17 w C:\Program Files\Eclipse Adoptium\"
}
$env:JAVA_HOME = $Jdk.FullName
$env:PATH = "$($Jdk.FullName)\bin;" + $env:PATH
$venvPython = Join-Path $Root ".venv\Scripts\python.exe"
if (Test-Path $venvPython) {
    $env:PYSPARK_PYTHON = $venvPython
    $env:PYSPARK_DRIVER_PYTHON = $venvPython
}
Set-Location $Root
Write-Host "JAVA_HOME=$env:JAVA_HOME"
uv run main.py
