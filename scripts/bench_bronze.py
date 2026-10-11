#!/usr/bin/env python3
# Бенчмарк Bronze: 2 формата x 4 кодека (8 вариантов).
# Для каждого варианта пишем в bench.<вариант> те же три сырые таблицы, что в Bronze
# (customer, orders, lineitem из tpch.sf10), и меряем:
#   - размер и число файлов (по $files), время записи;
#   - время своего запроса Q18 прямо по этим таблицам, холодный и тёплый прогон.
# GZIP и ZSTD на Parquet - это п.5 отчёта, остальное - общий бенчмарк форматов/кодеков.
#
# Запуск из корня репозитория: python3 scripts/bench_bronze.py

import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench_q18 import q18  # noqa: E402
from trino_http import Trino, make_cold, table_size, wait_trino  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "results", "bench_bronze_sf10.csv")
SF = "sf10"
TABLES = ["customer", "orders", "lineitem"]
COMBOS = [(f, c) for f in ("PARQUET", "ORC") for c in ("NONE", "SNAPPY", "GZIP", "ZSTD")]


wait_trino()
res = {}

print("### Фаза 1: запись 8 вариантов\n", flush=True)
for fmt, codec in COMBOS:
    schema = f"{fmt.lower()}_{codec.lower()}"
    t = Trino()
    t.run(f"CREATE SCHEMA IF NOT EXISTS bench.{schema}")
    t.run(f"SET SESSION bench.compression_codec = '{codec}'")
    r = {"формат": fmt, "кодек": codec, "запись_с": 0.0, "байт": 0, "файлов": 0}
    for tb in TABLES:
        t.run(f"DROP TABLE IF EXISTS bench.{schema}.{tb}")
        _, _, sec = t.run(f"CREATE TABLE bench.{schema}.{tb} WITH (format = '{fmt}') AS "
                          f"SELECT * FROM tpch.{SF}.{tb}")
        n, files, size = table_size(t, f"bench.{schema}.{tb}")
        r["запись_с"] += sec
        r["байт"] += size
        r["файлов"] += files
        r[f"{tb}_мб"] = round(size / 1024 ** 2, 1)
        r[f"{tb}_запись_с"] = round(sec, 1)
        print(f"[{schema}] {tb}: {n:,} строк, {files} файлов, {size / 1024 ** 2:.1f} МБ, {sec:.1f} с",
              flush=True)
    res[schema] = r

print("\n### Фаза 2: Q18 по каждому варианту, холодный и тёплый прогон\n", flush=True)
for schema, r in res.items():
    make_cold()
    t = Trino()
    rows, _, cold = t.run(q18(schema))
    _, _, warm = t.run(q18(schema))
    r["q18_cold_с"], r["q18_warm_с"], r["q18_строк"] = round(cold, 3), round(warm, 3), len(rows)
    print(f"[{schema}] cold {cold:.3f} с / warm {warm:.3f} с ({len(rows)} строк)", flush=True)

cols = ["формат", "кодек", "мб", "файлов", "запись_с", "q18_cold_с", "q18_warm_с", "q18_строк",
        "customer_мб", "orders_мб", "lineitem_мб",
        "customer_запись_с", "orders_запись_с", "lineitem_запись_с"]
with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["вариант"] + cols)
    for schema, r in res.items():
        r["мб"] = round(r["байт"] / 1024 ** 2, 1)
        r["запись_с"] = round(r["запись_с"], 1)
        w.writerow([schema] + [r.get(c, "") for c in cols])

print(f"\n### Итог -> {OUT}\n")
print(f"{'вариант':16s} {'МБ':>9s} {'файлов':>7s} {'запись,с':>9s} {'cold,с':>8s} {'warm,с':>8s}")
for schema, r in res.items():
    print(f"{schema:16s} {r['мб']:9.1f} {r['файлов']:7d} {r['запись_с']:9.1f} "
          f"{r['q18_cold_с']:8.3f} {r['q18_warm_с']:8.3f}")
