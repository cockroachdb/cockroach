SELECT NULL AS table_cat,
       n.nspname AS table_schem,
       ct.relname AS TABLE_NAME,
       NOT i.indisunique AS non_unique,
       NULL AS index_qualifier,
       ci.relname AS index_name,
       CASE i.indisclustered
         WHEN TRUE THEN 1
         ELSE CASE am.amname
           WHEN 'hash' THEN 2
           ELSE 3
         END
       END AS TYPE,
       (i.KEYS).n AS ordinal_position,
       trim(BOTH '"' FROM pg_catalog.pg_get_indexdef(ci.oid, (i.KEYS).n, FALSE)) AS COLUMN_NAME,
       CASE am.amcanorder
         WHEN TRUE THEN CASE i.indoption[(i.keys).n - 1] & 1
           WHEN 1 THEN 'D'
           ELSE 'A'
         END
         ELSE NULL
       END AS asc_or_desc,
       ci.reltuples AS CARDINALITY,
       ci.relpages AS pages,
       pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS filter_condition
FROM pg_catalog.pg_class ct
JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
JOIN (
  SELECT i.indexrelid,
         i.indrelid,
         i.indoption,
         i.indisunique,
         i.indisclustered,
         i.indpred,
         i.indexprs,
         information_schema._pg_expandarray(i.indkey) AS KEYS
  FROM pg_catalog.pg_index i
) i
  ON (ct.oid = i.indrelid)
JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
JOIN pg_catalog.pg_am am ON (ci.relam = am.oid)
WHERE TRUE
  AND n.nspname = 'public'
  AND ct.relname = 'j'
ORDER BY non_unique,
         TYPE,
         index_name,
         ordinal_position
