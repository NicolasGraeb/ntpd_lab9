from pathlib import Path

from pyspark.sql import SparkSession

# Plik na dysku (u Ciebie) — musi leżeć w data/ obok docker-compose.yml
LOCAL_CSV = Path(__file__).resolve().parent / "data" / "supermarket_sales.csv"

# Ta sama ścieżka, ale widziana przez workera w Dockerze (volume ./data -> /opt/spark/data)
SCIEZKA_CSV = "file:///opt/spark/data/supermarket_sales.csv"

if not LOCAL_CSV.is_file():
    raise FileNotFoundError(
        f"Brak pliku: {LOCAL_CSV}\n"
        "Skopiuj supermarket_sales.csv do folderu data/ i uruchom ponownie."
    )

spark = (
    SparkSession.builder.appName("Zadanie2_OperacjeDataFrame")
    .master("spark://localhost:7077")
    .config("spark.driver.host", "host.docker.internal")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .getOrCreate()
)

print("Pomyślnie połączono ze Sparkiem!")

df = spark.read.csv(SCIEZKA_CSV, header=True, inferSchema=True)

print("--- Schemat Danych ---")
df.printSchema()

print("--- Pierwsze 5 wierszy ---")
df.show(5)

spark.stop()
