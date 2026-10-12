# Lakehouse на ноутбуке: TPC-H Q18 через медальон

**Практическая работа №1** · Зозуля Арина Кирилловна, БИВТ-24-АЭ-1
Вариант 3 - запрос Q18 "Large Volume Customer" (клиенты с заказами больше 300 единиц товара), масштаб `sf10`.

Полный отчёт по форме преподавателя: **[отчет.md](отчет.md)**

## Как устроено

```
 tpch.sf10 (генератор в Trino)
        |
        v
 BRONZE  raw_customer, raw_orders, raw_lineitem, raw_nation      Parquet + GZIP    ~1.9 ГБ
        |   чистка, разбор склеенных полей, страна клиента,
        |   сумма товара по каждому заказу
        v
 SILVER  dim_customer, fct_orders                                Parquet + ZSTD    ~220 МБ
        |   заказы > 300 единиц, топ-100 по сумме
        v
 GOLD    mart_large_orders (ответ на Q18, 100 строк)             Parquet + SNAPPY  4.5 КБ
```

Хранение - MinIO (отдельный бакет на каждый слой), каталог таблиц - Lakekeeper (Iceberg REST), движок - Trino. Для проверки "открытости" данных Bronze читается ещё и из Spark.

## Запуск

Нужен Docker (у меня Docker Desktop на маке, ему выделено 7.6 ГБ памяти) и python3 без дополнительных библиотек.

1. Поднять стек - MinIO, Postgres, Lakekeeper, Trino, бакеты и warehouse:
   ```bash
   cd stand && ./up.sh && cd ..
   ```
2. Построить слои и проверить Gold против оригинального Q18:
   ```bash
   python3 scripts/medallion.py
   ```
   (или руками: `docker exec -i trino trino < sql/medallion.sql`)
3. Бенчмарк форматов и кодеков (~20 минут) и повторные замеры:
   ```bash
   python3 scripts/bench_bronze.py
   python3 scripts/bench_repeat.py parquet_gzip parquet_zstd orc_none parquet_none
   ```
4. Schema evolution и откат из снапшота (запускать после шага 2, скрипт меняет схему Silver):
   ```bash
   bash scripts/evolution.sh | tee results/evolution_output.txt
   ```
5. Spark поверх Bronze. Trino на это время останавливаю, вдвоём они в память не помещаются:
   ```bash
   docker stop trino
   docker run --rm --name p1-spark --add-host host.docker.internal:host-gateway \
     -m 5g --entrypoint spark-submit \
     -e AWS_REGION=us-east-1 -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \
     -v "$(pwd)/scripts/spark_bronze.py:/home/iceberg/spark_bronze.py" \
     tabulario/spark-iceberg:latest --driver-memory 3g /home/iceberg/spark_bronze.py
   docker start trino
   ```

Порты: Trino 8080, Lakekeeper 8181, MinIO 9000 (консоль 9101, логин minioadmin/minioadmin), Postgres 5433.

## Что получилось коротко

- Gold совпадает с оригинальным Q18 по `tpch.sf10`: 100 строк, 0 расхождений, сумма количества 30 977. Spark по Bronze даёт тот же ответ.
- Q18 по сырому Bronze идёт 3-8 с, по готовой витрине Gold - меньше 0.2 с.
- Сжатие уменьшает данные в 1.9-2.9 раза и при этом не замедляет запрос. GZIP и ZSTD почти равны по размеру и скорости чтения, но ZSTD пишется в 1.75 раза быстрее.
- Подробные таблицы и выводы - в [отчет.md](отчет.md), сырые цифры - в `results/`.

## Файлы

```
stand/
  up.sh                  поднимает весь стек
  catalog/*.properties   каталоги Trino: bronze, silver, gold, bench, tpch
  conf/                  память Trino (heap 4 ГБ, лимит запроса 2 ГБ, spill)
sql/
  medallion.sql          все три слоя + проверки нормализации и сверка с Q18
scripts/
  trino_http.py          мини-клиент Trino по HTTP, холодный старт для замеров
  medallion.py           выполняет medallion.sql, пишет статистику по таблицам
  bench_bronze.py        8 вариантов формат x кодек: размер, запись, Q18 cold/warm
  bench_q18.py           текст запроса Q18 для бенчмарка
  bench_repeat.py        повторные холодные/тёплые прогоны, медиана
  evolution.sh           ADD/RENAME COLUMN, авария, time travel, rollback
  spark_bronze.py        Spark читает Bronze через Lakekeeper
results/
  medallion_stats.csv    строки, файлы, размер, время записи по каждой таблице
  silver_checks.txt      проверка нормализации Silver
  gold_q18_top100.csv    ответ Gold
  gold_check.txt         сверка Gold с оригинальным Q18
  bench_bronze_sf10*.csv бенчмарк, два полных прогона
  bench_repeat_sf10.csv  повторы для GZIP/ZSTD
  evolution_output.txt   вывод schema evolution и отката
  spark_output.txt       вывод Spark
```
