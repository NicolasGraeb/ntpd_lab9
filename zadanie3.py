"""
Zadanie 3: Praca z RDD w PySpark.
Wczytanie CSV jako RDD, ręczne parsowanie wierszy, transformacje i akcje.
"""

import os
import sys
from pathlib import Path

# Spark musi używać tego samego Pythona co venv (nie 3.13 z Windows Store).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def _ensure_java17() -> None:
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

ROOT = Path(__file__).resolve().parent
LOCAL_CSV = ROOT / "data" / "supermarket_sales.csv"
DOCKER_CSV_PATH = Path("/opt/spark/data/supermarket_sales.csv")
IN_DOCKER = DOCKER_CSV_PATH.is_file()

if IN_DOCKER and not DOCKER_CSV_PATH.is_file():
    raise FileNotFoundError(f"Brak pliku w volume: {DOCKER_CSV_PATH}")
if not IN_DOCKER and not LOCAL_CSV.is_file():
    raise FileNotFoundError(f"Brak pliku: {LOCAL_CSV}")

# Kolumny w CSV (nagłówek)
COLUMNS = [
    "invoice_id",
    "branch",
    "city",
    "customer_type",
    "gender",
    "product_line",
    "unit_price",
    "quantity",
    "tax",
    "total",
    "date",
    "time",
    "payment",
    "cogs",
    "gross_margin_pct",
    "gross_income",
    "rating",
]


def parse_line(line: str) -> dict | None:
    """Parsuj jeden wiersz CSV (bez biblioteki — split po przecinku)."""
    parts = line.split(",")
    if len(parts) != len(COLUMNS):
        return None
    try:
        return {
            "invoice_id": parts[0],
            "branch": parts[1],
            "city": parts[2],
            "customer_type": parts[3],
            "gender": parts[4],
            "product_line": parts[5],
            "unit_price": float(parts[6]),
            "quantity": int(parts[7]),
            "tax": float(parts[8]),
            "total": float(parts[9]),
            "date": parts[10],
            "time": parts[11],
            "payment": parts[12],
            "cogs": float(parts[13]),
            "gross_margin_pct": float(parts[14]),
            "gross_income": float(parts[15]),
            "rating": float(parts[16]),
        }
    except (ValueError, IndexError):
        return None


builder = SparkSession.builder.appName("Zadanie3_RDD")
if IN_DOCKER:
    builder = builder.master("spark://spark-master:7077")
else:
    builder = builder.master("local[*]")

spark = builder.getOrCreate()
sc = spark.sparkContext

csv_path = str(DOCKER_CSV_PATH) if IN_DOCKER else LOCAL_CSV.as_uri()

# --- 1. Wczytanie pliku jako RDD (linia tekstu = jeden rekord) ---
lines_rdd = sc.textFile(csv_path)
if IN_DOCKER:
    header = lines_rdd.first()
else:
    header = LOCAL_CSV.read_text(encoding="utf-8").splitlines()[0]

records_rdd = (
    lines_rdd.filter(lambda line: line != header)
    .map(parse_line)
    .filter(lambda row: row is not None)
)

# --- 2. Akcje: count, collect (mała próbka) ---
row_count = records_rdd.count()
print(f"Liczba wierszy (po parsowaniu): {row_count}")

sample = records_rdd.take(3)
print("=== collect/take — 3 pierwsze rekordy ===")
for rec in sample:
    print(rec)

# --- 3. map + filter ---
# Tylko oddział A, mapujemy na kwotę sprzedaży
branch_a_totals = (
    records_rdd.filter(lambda r: r["branch"] == "A")
    .map(lambda r: r["total"])
)

high_rating = records_rdd.filter(lambda r: r["rating"] >= 9.0)

# --- 4. reduce — suma kolumny Total (cały zbiór) ---
sum_total = records_rdd.map(lambda r: r["total"]).reduce(lambda a, b: a + b)
sum_total_branch_a = branch_a_totals.reduce(lambda a, b: a + b)
avg_rating = records_rdd.map(lambda r: r["rating"]).reduce(lambda a, b: a + b) / row_count

print(f"Suma kolumny Total (wszystkie wiersze): {sum_total:.2f}")
print(f"Suma Total — oddział A: {sum_total_branch_a:.2f}")
print(f"Średnia Rating: {avg_rating:.2f}")
print(f"Liczba transakcji z Rating >= 9.0: {high_rating.count()}")

# --- 5. map + reduceByKey — suma sprzedaży per miasto ---
city_sales = (
    records_rdd.map(lambda r: (r["city"], r["total"]))
    .reduceByKey(lambda a, b: a + b)
    .collect()
)

print("=== Suma sprzedaży per miasto (reduceByKey) ===")
for city, total in sorted(city_sales, key=lambda x: x[1], reverse=True):
    print(f"  {city}: {total:.2f}")

# --- 6. collect — top 5 najwyższych transakcji (po mapowaniu) ---
top5 = (
    records_rdd.map(lambda r: (r["total"], r["city"], r["product_line"]))
    .sortBy(lambda x: x[0], ascending=False)
    .take(5)
)

print("=== Top 5 transakcji (sortBy + take) ===")
for total, city, product in top5:
    print(f"  {total:.2f} | {city} | {product}")

spark.stop()
