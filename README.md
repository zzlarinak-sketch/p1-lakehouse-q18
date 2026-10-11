# Практическая 1 - локальный Lakehouse, медальон и бенчмарк форматов/кодеков

Зозуля Арина. Вариант 3 - TPC-H Q18 "Large Volume Customer", масштаб sf10.

Стек: MinIO -> Lakekeeper (Iceberg REST Catalog) -> Trino, плюс Spark для п.6.2. Всё в Docker Desktop на маке.

Отчёт по форме - [отчет.md](отчет.md).

## Что где лежит

| | |
|---|---|
| `stand/up.sh` | поднять весь стек одной командой |
| `stand/catalog/` | каталоги Trino: bronze, silver, gold, bench, tpch |
| `stand/conf/` | jvm.config и config.properties для Trino (лимиты памяти) |
| `sql/medallion.sql` | слои bronze -> silver -> gold + сверка gold с оригинальным Q18 |
| `scripts/trino_http.py` | маленький клиент Trino по HTTP, без зависимостей |
| `scripts/medallion.py` | выполняет medallion.sql и собирает строки/файлы/МБ/время по таблицам |
| `scripts/bench_bronze.py` | бенчмарк: 2 формата x 4 кодека, размер, запись, Q18 cold/warm |
| `scripts/bench_repeat.py` | повторные замеры Q18 (3 раза, медиана) |
| `scripts/evolution.sh` | п.6.1: schema evolution и откат из снапшота |
| `scripts/spark_bronze.py` | п.6.2: Spark читает Bronze через Lakekeeper |
| `results/` | полученные результаты |

## Как запустить

```bash
cd stand && ./up.sh && cd ..
python3 scripts/medallion.py
python3 scripts/bench_bronze.py
python3 scripts/bench_repeat.py parquet_gzip parquet_zstd orc_none parquet_none
bash scripts/evolution.sh | tee results/evolution_output.txt
```

Spark (перед запуском `docker stop trino`, вдвоём они в память Docker не помещаются):

```bash
docker run --rm --name p1-spark --add-host host.docker.internal:host-gateway \
  -m 5g --entrypoint spark-submit \
  -e AWS_REGION=us-east-1 -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -v "$(pwd)/scripts/spark_bronze.py:/home/iceberg/spark_bronze.py" \
  tabulario/spark-iceberg:latest --driver-memory 3g /home/iceberg/spark_bronze.py
```
