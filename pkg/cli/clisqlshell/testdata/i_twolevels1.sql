-- test that \echo spills into the second level
\set echo

\i testdata/i_twolevels2.sql

-- at this point, the second level has disabled echo.
-- verify this.
SELECT 456;

