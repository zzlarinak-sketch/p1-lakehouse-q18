"""Запрос варианта 3 (Q18) по сырым таблицам в bench.<вариант>."""


def q18(schema):
    return f"""
SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, sum(l.quantity) AS total_quantity
FROM bench.{schema}.customer c
JOIN bench.{schema}.orders   o ON c.custkey = o.custkey
JOIN bench.{schema}.lineitem l ON o.orderkey = l.orderkey
WHERE o.orderkey IN (
  SELECT orderkey FROM bench.{schema}.lineitem GROUP BY orderkey HAVING sum(quantity) > 300
)
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100"""
