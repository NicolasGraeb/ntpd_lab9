# Lab 9 — Zadanie 1: Spark (Docker) + PySpark (uv)

## Odpowiedź w skrócie

| Element | Gdzie działa | Java na Windows? |
|---------|--------------|------------------|
| Spark (silnik) | Docker | **Nie** |
| `spark-shell` / `pyspark` | Docker | **Nie** |
| Skrypty `.py` | Docker (`spark-submit`) | **Nie** |
| Pakiet `pyspark` (uv) | Twój projekt — edycja, typy, IDE | **Nie** do uruchomienia w Dockerze |

**Nie musisz usuwać Javy z komputera**, ale do tego labu **nie musisz jej używać**. PySpark w trybie `local[*]` na Windowsie sam startuje JVM — u Ciebie jest Java 25, co często gryzie się ze Sparkiem. Dlatego uruchamiamy skrypty **w kontenerze**, gdzie jest właściwa Java.

## Co zainstalować

1. **Docker Desktop** (już masz)
2. W projekcie: `uv add pyspark` (już w `pyproject.toml`)

```bash
uv sync
```

To wszystko po stronie Pythona. Nie instaluj Sparka ręcznie na Windows.

## Uruchomienie

### 1. Kontener ze Sparkiem

```powershell
docker compose up -d
```

### 2. Test `spark-shell` (wymaganie zadania)

```powershell
docker compose run --rm spark /opt/spark/bin/spark-shell --master "local[*]"
```

W powłoce Scala:

```scala
spark.range(5).count()
:quit
```

### 3. Test `pyspark` (wymaganie zadania)

```powershell
docker compose run --rm spark /opt/spark/bin/pyspark --master "local[*]"
```

```python
spark.range(5).count()
exit()
```

### 4. Skrypt Python (PySpark) — Twój kod z repo

```powershell
docker compose run --rm spark /opt/spark/bin/spark-submit --master "local[*]" /work/scripts/check_pyspark.py
```

Powinno wypisać `OK — wierszy: 10` i tabelę.

`uv` służy do zarządzania zależnościami i pisania kodu lokalnie; **wykonanie** idzie przez Spark w Dockerze.

### 5. Stop

```powershell
docker compose down
```

## Dlaczego tak?

- **Docker** = pełny Spark + JVM + `spark-shell` / `pyspark` — spełnia punkty 1–2 zadania.
- **uv + pyspark** = spełnia punkt 3 (pakiet do skryptów) — uruchamiasz je przez `spark-submit` w kontenerze.
- **Bez Javy na hoście** — nie konfigurujesz `JAVA_HOME` pod Sparka na Windows.

## Jeśli prowadzący wymaga `uv run python skrypt.py` na hoście

Wtedy na Windows potrzebujesz **JDK 17** (nie 25) i:

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17..."
uv run python scripts/check_pyspark.py
```

(skrypt musiałby mieć `.master("local[*]")` — jak w `check_pyspark.py`). Na zajęcia wystarczy wariant z Dockerem powyżej.
