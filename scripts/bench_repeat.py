#!/usr/bin/env python3
# Повторные замеры Q18 по уже записанным вариантам из bench_bronze.py:
# N раз (холодный старт -> cold, сразу ещё раз -> warm), в отчёт идёт медиана.
# Один прогон сильно шумит, поэтому для п.5 (GZIP vs ZSTD) берём повторы.
#
# Запуск: python3 scripts/bench_repeat.py parquet_gzip parquet_zstd orc_none

import csv
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench_q18 import q18  # noqa: E402
from trino_http import Trino, make_cold, wait_trino  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "results", "bench_repeat_sf10.csv")
N = 3
schemas = sys.argv[1:] or ["parquet_gzip", "parquet_zstd"]

wait_trino()
rows = []
for schema in schemas:
    cold, warm = [], []
    for i in range(N):
        make_cold()
        t = Trino()
        cold.append(t.run(q18(schema))[2])
        warm.append(t.run(q18(schema))[2])
        print(f"[{schema}] прогон {i + 1}: cold {cold[-1]:.3f} с / warm {warm[-1]:.3f} с", flush=True)
    rows.append([schema, *[round(x, 3) for x in cold], *[round(x, 3) for x in warm],
                 round(statistics.median(cold), 3), round(statistics.median(warm), 3)])
    print(f"[{schema}] медиана: cold {rows[-1][-2]} с / warm {rows[-1][-1]} с\n", flush=True)

with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["вариант"] + [f"cold_{i + 1}" for i in range(N)] + [f"warm_{i + 1}" for i in range(N)]
               + ["cold_медиана", "warm_медиана"])
    w.writerows(rows)
print(f"-> {OUT}")
