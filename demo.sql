CREATE PROCEDURE foo(x INT) LANGUAGE SQL AS $$
SELECT 1;
SELECT 2;
$$;

EXPLAIN ANALYZE CALL foo(3);


--------------------------------------------------------------------------------
-- EXPLAIN ANALYZE with routines (functions, procedures, triggers).
--
-- Demonstrates how routine body plans, invocation counts, and per-node
-- execution stats surface in EXPLAIN ANALYZE output. The emphasis is on
-- stored procedures (CALL), with UDF and trigger coverage for contrast.
--
-- Run with:
--   ./cockroach demo --empty --no-line-editor < demo.sql
--------------------------------------------------------------------------------

CREATE TABLE t (a INT PRIMARY KEY, b INT, FAMILY (a, b));
INSERT INTO t SELECT i, i*10 FROM generate_series(1, 10) AS g(i);


--------------------------------------------------------------------------------
-- PART 1: STORED PROCEDURES (CALL)
--
-- EXPLAIN ANALYZE CALL shows a `call` node above the procedure's routine
-- body. Each body statement becomes a routine node named `_stmt_<kind>_<n>`
-- (or the callee's name for nested CALLs / UDF references).
--------------------------------------------------------------------------------

-- 1. SQL procedure.
CREATE PROCEDURE bump_sql(x INT) LANGUAGE SQL AS $$
  UPDATE t SET b = b + 1 WHERE a = x;
$$;

EXPLAIN ANALYZE CALL bump_sql(1);


-- 2. SQL procedure with an OUT parameter (returns a row).
CREATE PROCEDURE get_b(IN x INT, OUT res INT) LANGUAGE SQL AS $$
  SELECT b FROM t WHERE a = x
$$;

EXPLAIN ANALYZE CALL get_b(3, NULL);


-- 3. SQL procedure running a multi-table join. The routine body captures the
--    full plan of a non-trivial statement — the join operator and both input
--    scans appear under the `body stmt`, each with its own execution stats.
CREATE TABLE u (a INT PRIMARY KEY, c INT);
INSERT INTO u SELECT i, i*100 FROM generate_series(1, 10) AS g(i);

CREATE PROCEDURE join_summary(lim INT) LANGUAGE SQL AS $$
  SELECT t.a, t.b, u.c
  FROM t JOIN u ON t.a = u.a
  WHERE t.b > lim
  ORDER BY t.a;
$$;

EXPLAIN ANALYZE CALL join_summary(30);


-- 4. PL/pgSQL procedure (no control flow).
CREATE PROCEDURE bump_plpgsql(x INT) LANGUAGE PLpgSQL AS $$
BEGIN
  UPDATE t SET b = b + 1 WHERE a = x;
END
$$;

EXPLAIN ANALYZE CALL bump_plpgsql(2);


-- 5. PL/pgSQL procedure with an INOUT parameter.
CREATE PROCEDURE inc(INOUT v INT) LANGUAGE PLpgSQL AS $$
BEGIN
  v := v + 1;
END
$$;

EXPLAIN ANALYZE CALL inc(41);


-- 6. Multi-statement DML procedure (INSERT / UPDATE / DELETE). Each body
--    statement becomes its own routine node (`_stmt_exec_1/2/3`), and the
--    mutation operator reports its `actual row count` (3 inserted, 5
--    updated, 3 deleted).
CREATE PROCEDURE dml_proc() LANGUAGE PLpgSQL AS $$
BEGIN
  INSERT INTO t VALUES (100, 1000), (101, 1010), (102, 1020);
  UPDATE t SET b = b + 1 WHERE a <= 5;
  DELETE FROM t WHERE a >= 100;
END
$$;

EXPLAIN ANALYZE CALL dml_proc();


-- 7. Procedure calling another procedure (nested CALL). The callee appears
--    as its own routine node beneath the caller's body.
CREATE PROCEDURE inner_proc(x INT) LANGUAGE SQL AS $$
  UPDATE t SET b = b + 1 WHERE a = x;
$$;

CREATE PROCEDURE outer_proc(x INT) LANGUAGE PLpgSQL AS $$
BEGIN
  CALL inner_proc(x);
END
$$;

EXPLAIN ANALYZE CALL outer_proc(1);


-- 8. Procedure calling a UDF: the function shows up as a nested routine.
CREATE FUNCTION dbl(x INT) RETURNS INT VOLATILE LANGUAGE SQL AS $$
SELECT x * 2
$$;

CREATE PROCEDURE set_dbl(x INT) LANGUAGE PLpgSQL AS $$
BEGIN
  UPDATE t SET b = dbl(x) WHERE a = x;
END
$$;

EXPLAIN ANALYZE CALL set_dbl(1);


-- 9. Procedure with in-body transaction control (COMMIT) — a
--    procedure-only capability with no UDF equivalent.
CREATE PROCEDURE txn_proc() LANGUAGE PLpgSQL AS $$
BEGIN
  UPDATE t SET b = b + 1 WHERE a = 1;
  COMMIT;
END
$$;

EXPLAIN ANALYZE CALL txn_proc();


-- 10. PL/pgSQL procedure whose body uses control flow (a FOR loop), invoked
--    via CALL. Each control-flow block compiles into a nested sub-routine
--    planned lazily at execution time; under EXPLAIN ANALYZE these appear as
--    their own routine nodes (e.g. `stmt_loop_*`) with per-iteration
--    invocation counts.
CREATE PROCEDURE bump_loop(n INT) LANGUAGE PLpgSQL AS $$
BEGIN
  FOR i IN 1..n LOOP
    UPDATE t SET b = b + 1 WHERE a = i;
  END LOOP;
END
$$;

EXPLAIN ANALYZE CALL bump_loop(3);


-- 11. PL/pgSQL procedure with an IF/ELSE branch, invoked via CALL. The taken
--     branch's body is captured as a nested routine node.
CREATE PROCEDURE classify(x INT) LANGUAGE PLpgSQL AS $$
BEGIN
  IF x > 5 THEN UPDATE t SET b = 1 WHERE a = x;
  ELSE UPDATE t SET b = 0 WHERE a = x;
  END IF;
END
$$;

EXPLAIN ANALYZE CALL classify(7);


--------------------------------------------------------------------------------
-- PART 2: USER-DEFINED FUNCTIONS
--------------------------------------------------------------------------------

-- 12. Scalar SQL UDF in a projection.
--     A single routine node with `invocations` and a `body stmt` subplan.
CREATE FUNCTION lookup_volatile(x INT) RETURNS INT VOLATILE LANGUAGE SQL AS $$
SELECT b FROM t WHERE a = x
$$;

EXPLAIN ANALYZE (VERBOSE) SELECT lookup_volatile(a) FROM t WHERE a <= 3;


-- 13. Nested UDF calls (outer_fn calls inner_fn): body plans nest.
CREATE FUNCTION inner_fn(x INT) RETURNS INT VOLATILE LANGUAGE SQL AS $$
SELECT x * 10
$$;

CREATE FUNCTION outer_fn(x INT) RETURNS INT VOLATILE LANGUAGE SQL AS $$
SELECT inner_fn(x) + 1
$$;

EXPLAIN ANALYZE SELECT outer_fn(a) FROM t WHERE a <= 3;


-- 14. Multiple plan variants for the same routine. NULL vs non-NULL inputs
--     produce different body plans; each is labeled "plan variant: N of M"
--     with its own invocation count.
EXPLAIN ANALYZE
SELECT lookup_volatile(a)
FROM (VALUES (1), (2), (3), (NULL::INT), (NULL::INT)) AS v(a);


-- 15. Set-returning UDF (SETOF) in the FROM clause.
CREATE FUNCTION top_b(lim INT) RETURNS SETOF INT VOLATILE LANGUAGE SQL AS $$
SELECT b FROM t ORDER BY b DESC LIMIT lim
$$;

EXPLAIN ANALYZE SELECT * FROM top_b(3);


-- 16. Volatile UDF in a WHERE filter (invoked once per scanned row).
CREATE FUNCTION is_big(x INT) RETURNS BOOL VOLATILE LANGUAGE SQL AS $$
SELECT x > 50
$$;

EXPLAIN ANALYZE SELECT a FROM t WHERE is_big(b);


-- 17. PL/pgSQL *function* with control flow (a loop) invoked via SELECT. The
--     same control-flow loop works both here (function via SELECT) and in a
--     CALL'd procedure (see examples 10-11): the loop expands into nested
--     routine bodies in the plan.
CREATE FUNCTION sum_to(n INT) RETURNS INT LANGUAGE PLpgSQL AS $$
DECLARE
  s INT := 0;
BEGIN
  FOR i IN 1..n LOOP
    s := s + i;
  END LOOP;
  RETURN s;
END
$$;

EXPLAIN ANALYZE SELECT sum_to(a) FROM t WHERE a <= 3;


--------------------------------------------------------------------------------
-- PART 3: TRIGGERS
--------------------------------------------------------------------------------

-- 18. AFTER INSERT trigger: the fired trigger function appears under an
--     `after-triggers` node.
CREATE TABLE audit (id INT PRIMARY KEY DEFAULT unique_rowid(), a INT, b INT);

CREATE FUNCTION audit_insert() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$
BEGIN
  INSERT INTO audit (a, b) VALUES ((NEW).a, (NEW).b);
  RETURN NEW;
END
$$;

CREATE TRIGGER trg_audit AFTER INSERT ON t
  FOR EACH ROW EXECUTE FUNCTION audit_insert();

EXPLAIN ANALYZE INSERT INTO t VALUES (100, 1000);


-- 19. BEFORE INSERT trigger that mutates the incoming row: appears under a
--     `before-triggers` node.
CREATE FUNCTION bump_before() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$
BEGIN
  NEW.b := (NEW).b + 1;
  RETURN NEW;
END
$$;

CREATE TRIGGER trg_bump BEFORE INSERT ON t
  FOR EACH ROW EXECUTE FUNCTION bump_before();

EXPLAIN ANALYZE INSERT INTO t VALUES (200, 2000);
