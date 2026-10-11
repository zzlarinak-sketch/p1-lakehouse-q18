#!/bin/bash
# п.6.1: эволюция схемы на Silver и восстановление из снапшота (time travel).
# Таблица: silver.lakehouse.dim_customer
# Запуск: bash scripts/evolution.sh | tee results/evolution_output.txt

T="silver.lakehouse.dim_customer"
META='silver.lakehouse."dim_customer$'
q() { echo "> $1"; docker exec trino trino --output-format ALIGNED --execute "$1" 2>/dev/null; echo; }
v() { docker exec trino trino --execute "$1" 2>/dev/null | tr -d '"'; }

echo "### 0. Исходная схема и данные"
q "DESCRIBE $T"
q "SELECT custkey, name, nation_name, mktsegment FROM $T ORDER BY custkey LIMIT 3"
q "SELECT snapshot_id, operation, committed_at FROM ${META}snapshots\" ORDER BY committed_at"
q "SELECT count(*) AS files FROM ${META}files\""

echo "### 1. ADD COLUMN: добавили колонку vip (признак крупного клиента)"
q "ALTER TABLE $T ADD COLUMN vip boolean"
q "SELECT custkey, name, vip FROM $T ORDER BY custkey LIMIT 3"
q "SELECT count(*) AS files FROM ${META}files\""
echo "### заполнили vip для клиентов с крупными заказами (> 300 единиц) - это уже новый снапшот"
q "UPDATE $T SET vip = true WHERE custkey IN (SELECT custkey FROM silver.lakehouse.fct_orders WHERE total_quantity > 300)"
q "SELECT vip, count(*) AS cnt FROM $T GROUP BY vip"

echo "### 2. RENAME COLUMN: name -> customer_name"
q "ALTER TABLE $T RENAME COLUMN name TO customer_name"
q "SELECT custkey, customer_name, vip FROM $T ORDER BY custkey LIMIT 3"
q "SELECT snapshot_id, operation, committed_at FROM ${META}snapshots\" ORDER BY committed_at"

echo "### 3. Запоминаем текущий (хороший) снапшот"
GOOD=$(v "SELECT snapshot_id FROM ${META}snapshots\" ORDER BY committed_at DESC LIMIT 1")
echo "GOOD = $GOOD"
q "SELECT count(*) AS rows_before FROM $T"

echo "### 4. Авария: случайно удалили всех клиентов не из сегмента BUILDING"
q "DELETE FROM $T WHERE mktsegment <> 'BUILDING'"
q "SELECT count(*) AS rows_after_delete FROM $T"

echo "### 5. Time travel: смотрим таблицу до удаления"
q "SELECT count(*) AS rows_in_good_snapshot FROM $T FOR VERSION AS OF $GOOD"

echo "### 6. Восстановление: откат таблицы на хороший снапшот"
q "ALTER TABLE $T EXECUTE rollback_to_snapshot($GOOD)"
q "SELECT count(*) AS rows_after_rollback FROM $T"
q "SELECT vip, count(*) AS cnt FROM $T GROUP BY vip"
q "SELECT made_current_at, snapshot_id, is_current_ancestor FROM ${META}history\" ORDER BY made_current_at"
q "DESCRIBE $T"
