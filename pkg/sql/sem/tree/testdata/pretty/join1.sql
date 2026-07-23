SELECT     NULL::text  AS pktable_cat,
       pkn.nspname AS pktable_schem,
       pkc.relname AS pktable_name,
       pka.attname AS pkcolumn_name,
       NULL::text  AS fktable_cat,
       fkn.nspname AS fktable_schem,
       fkc.relname AS fktable_name,
       fka.attname AS fkcolumn_name,
       pos.n       AS key_seq,
       CASE con.confupdtype
            WHEN 'c' THEN 0
            WHEN 'n' THEN 2
            WHEN 'd' THEN 4
            WHEN 'r' THEN 1
            WHEN 'a' THEN 3
            ELSE NULL
       END AS update_rule,
       CASE con.confdeltype
            WHEN 'c' THEN 0
            WHEN 'n' THEN 2
            WHEN 'd' THEN 4
            WHEN 'r' THEN 1
            WHEN 'a' THEN 3
            ELSE NULL
       END          AS delete_rule,
       con.conname  AS fk_name,
       pkic.relname AS pk_name,
       CASE
            WHEN con.condeferrable
            AND      con.condeferred THEN 5
            WHEN con.condeferrable THEN 6
            ELSE 7
       END AS deferrability
  FROM     pg_catalog.pg_namespace pkn,
       pg_catalog.pg_class pkc,
       pg_catalog.pg_attribute pka,
       pg_catalog.pg_namespace fkn,
       pg_catalog.pg_class fkc,
       pg_catalog.pg_attribute fka,
       pg_catalog.pg_constraint con,
       pg_catalog.generate_series(1, 32) pos(n),
       pg_catalog.pg_depend dep,
       pg_catalog.pg_class pkic
  WHERE    pkn.oid = pkc.relnamespace
  AND      pkc.oid = pka.attrelid
  AND      pka.attnum = con.confkey[pos.n]
  AND      con.confrelid = pkc.oid
  AND      fkn.oid = fkc.relnamespace
  AND      fkc.oid = fka.attrelid
  AND      fka.attnum = con.conkey[pos.n]
  AND      con.conrelid = fkc.oid
  AND      con.contype = 'f'
  AND      con.oid = dep.objid
  AND      pkic.oid = dep.refobjid
  AND      pkic.relkind = 'i'
  AND      dep.classid = 'pg_constraint'::regclass::oid
  AND      dep.refclassid = 'pg_class'::regclass::oid
  AND      fkn.nspname = 'public'
  AND      fkc.relname = 'orders'
  ORDER BY pkn.nspname,
       pkc.relname,
       con.conname,
       pos.n
