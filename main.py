import os
from pathlib import Path


def _ensure_java17() -> None:
    """Spark 3.5 nie działa z Javą 24/25 (błąd: getSubject is not supported)."""
    if os.name != "nt":
        return
    adoptium = Path(r"C:\Program Files\Eclipse Adoptium")
    if not adoptium.is_dir():
        return
    for jdk in sorted(adoptium.glob("jdk-17*"), reverse=True):
        if (jdk / "bin" / "java.exe").is_file():
            os.environ["JAVA_HOME"] = str(jdk)
            return


_ensure_java17()

_hadoop = Path(__file__).resolve().parent / "tools" / "hadoop"
if (_hadoop / "bin" / "winutils.exe").is_file():
    os.environ["HADOOP_HOME"] = str(_hadoop)
    _bin = str(_hadoop / "bin")
    os.environ["PATH"] = _bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parent
LOCAL_CSV = ROOT / "data" / "supermarket_sales.csv"
DOCKER_CSV = "file:///opt/spark/data/supermarket_sales.csv"
DOCKER_CSV_PATH = Path("/opt/spark/data/supermarket_sales.csv")
OUTPUT_DIR = ROOT / "output"
IN_DOCKER = DOCKER_CSV_PATH.is_file()

if IN_DOCKER and not DOCKER_CSV_PATH.is_file():
    raise FileNotFoundError(f"Brak pliku w volume: {DOCKER_CSV_PATH}")
if not IN_DOCKER and not LOCAL_CSV.is_file():
    raise FileNotFoundError(
        f"Brak pliku: {LOCAL_CSV}\n"
        "Skopiuj supermarket_sales.csv do folderu data/ i uruchom ponownie."
    )

# --- Zadanie 2: punkt 2 — SparkSession i wczytanie CSV ---
builder = SparkSession.builder.appName("DataFrameExample")
if IN_DOCKER:
    builder = builder.master("spark://spark-master:7077")
else:
    builder = builder.master("local[*]")

spark = builder.getOrCreate()

csv_path = DOCKER_CSV if IN_DOCKER else LOCAL_CSV.as_uri()
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# --- punkt 3a: wyświetlanie i schemat ---
print("=== Schemat (printSchema) ===")
df.printSchema()

print("=== Pierwsze 10 wierszy (show) ===")
df.show(10, truncate=False)

# --- punkt 3b: selekcja kolumn ---
selected = df.select("Invoice ID", "Branch", "City", "Product line", "Total", "Rating")
print("=== Selekcja kolumn ===")
selected.show(5, truncate=False)

# --- punkt 3c: filtrowanie ---
filtered = df.filter((F.col("Total") > 500) & (F.col("Branch") == "A"))
print("=== Filtrowanie: Branch = A i Total > 500 (where/filter) ===")
filtered.show(5, truncate=False)

filtered_where = df.where(F.col("Rating") >= 9.0)
print("=== Filtrowanie: Rating >= 9.0 (where) ===")
filtered_where.select("City", "Product line", "Total", "Rating").show(5)

# --- punkt 3d: grupowanie i agregacje ---
summary = df.groupBy("City", "Product line").agg(
    F.sum("Total").alias("suma_sprzedazy"),
    F.avg("Rating").alias("srednia_ocena"),
    F.count("*").alias("liczba_transakcji"),
)
print("=== Grupowanie: City + Product line ===")
summary.orderBy(F.desc("suma_sprzedazy")).show(10, truncate=False)

# --- punkt 4: zapis wyniku (Parquet + CSV) ---
OUTPUT_DIR.mkdir(exist_ok=True)
parquet_path = str(OUTPUT_DIR / "podsumowanie_parquet")
csv_path_out = str(OUTPUT_DIR / "podsumowanie_csv")

summary.write.mode("overwrite").parquet(parquet_path)
summary.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_path_out)

print(f"Zapisano Parquet: {parquet_path}")
print(f"Zapisano CSV:     {csv_path_out}")

spark.stop()
