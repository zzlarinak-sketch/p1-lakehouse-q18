#!/usr/bin/env python3
# Строит медальон по sql/medallion.sql и собирает для отчёта: время записи, строки, файлы, МБ
# по каждой таблице, ответ Gold и сверку Gold с оригинальным Q18.
#
# Запуск из корня репозитория: python3 scripts/medallion.py

import csv
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from trino_http import Trino, split_sql, table_size, wait_trino  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL = os.path.join(ROOT, "sql", "medallion.sql")
OUT = os.path.join(ROOT, "results")

wait_trino()
t = Trino()
stmts = split_sql(open(SQL, encoding="utf-8").read())
stats, selects = [], []

for s in stmts:
    m = re.match(r"CREATE TABLE (\S+)", s)
    head = " ".join(s.split())[:90]
    if s.upper().startswith("SELECT") or s.upper().startswith("WITH"):
        rows, cols, sec = t.run(s)
        selects.append((cols, rows, sec))
        print(f"[{sec:7.2f} с] {head}", flush=True)
        continue
    _, _, sec = t.run(s)
    print(f"[{sec:7.2f} с] {head}", flush=True)
    if m:
        fq = m.group(1)
        n, files, size = table_size(t, fq)
        codec = t.session.get(fq.split(".")[0] + ".compression_codec", "ZSTD (по умолчанию)")
        stats.append([fq, n, files, size, round(size / 1024 ** 2, 2), codec, round(sec, 2)])
        print(f"           -> {n:,} строк, {files} файлов, {size / 1024 ** 2:.2f} МБ, {codec}",
              flush=True)

# время запроса к витрине (тёплое, просто для сравнения с запросом по bronze)
_, _, gold_sec = t.run("SELECT * FROM gold.lakehouse.mart_large_orders ORDER BY totalprice DESC, orderdate")

with open(os.path.join(OUT, "medallion_stats.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["таблица", "строк", "файлов", "байт", "мб", "кодек", "запись_с"])
    w.writerows(stats)

# selects: 0 - проверка нормализации silver, 1 - ответ gold, 2 - сверка с оригинальным Q18
norm_cols, norm_rows, _ = selects[0]
print("\n### Проверка нормализации Silver:", dict(zip(norm_cols, norm_rows[0])))
with open(os.path.join(OUT, "silver_checks.txt"), "w", encoding="utf-8") as f:
    f.write(f"проверка нормализации silver: {dict(zip(norm_cols, norm_rows[0]))}\n")

gold_cols, gold_rows, _ = selects[1]
with open(os.path.join(OUT, "gold_q18_top100.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(gold_cols)
    w.writerows(gold_rows)

chk_cols, chk_rows, _ = selects[2]
print("\n### Сверка Gold с оригинальным Q18:", dict(zip(chk_cols, chk_rows[0])))
print(f"### Запрос к витрине Gold: {gold_sec:.3f} с")
with open(os.path.join(OUT, "gold_check.txt"), "w", encoding="utf-8") as f:
    f.write(f"сверка gold с оригинальным Q18 (tpch.sf10): {dict(zip(chk_cols, chk_rows[0]))}\n")
    f.write(f"запрос к витрине gold: {gold_sec:.3f} с\n")
