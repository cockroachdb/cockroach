// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
	"go/constant"
	"reflect"
	"testing"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
)

// TestParse verifies that we can parse the supplied SQL and regenerate the SQL
// string from the syntax tree.
func TestParse(t *testing.T) {
	testData := []struct {
		sql string
	}{
		{``},
		{`VALUES ("")`},

		{`BEGIN TRANSACTION`},
		{`BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT`},
		{`BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE`},
		{`BEGIN TRANSACTION PRIORITY LOW`},
		{`BEGIN TRANSACTION PRIORITY NORMAL`},
		{`BEGIN TRANSACTION PRIORITY HIGH`},
		{`BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE, PRIORITY HIGH`},
		{`COMMIT TRANSACTION`},
		{`ROLLBACK TRANSACTION`},
		{"SAVEPOINT foo"},

		{`CREATE DATABASE a`},
		{`CREATE DATABASE a TEMPLATE = 'template0'`},
		{`CREATE DATABASE a TEMPLATE = 'invalid'`},
		{`CREATE DATABASE a ENCODING = 'UTF8'`},
		{`CREATE DATABASE a ENCODING = 'INVALID'`},
		{`CREATE DATABASE a LC_COLLATE = 'C.UTF-8'`},
		{`CREATE DATABASE a LC_COLLATE = 'INVALID'`},
		{`CREATE DATABASE a LC_CTYPE = 'C.UTF-8'`},
		{`CREATE DATABASE a LC_CTYPE = 'INVALID'`},
		{`CREATE DATABASE a TEMPLATE = 'template0' ENCODING = 'UTF8' LC_COLLATE = 'C.UTF-8' LC_CTYPE = 'INVALID'`},
		{`CREATE DATABASE IF NOT EXISTS a`},
		{`CREATE DATABASE IF NOT EXISTS a TEMPLATE = 'template0'`},
		{`CREATE DATABASE IF NOT EXISTS a TEMPLATE = 'invalid'`},
		{`CREATE DATABASE IF NOT EXISTS a ENCODING = 'UTF8'`},
		{`CREATE DATABASE IF NOT EXISTS a ENCODING = 'INVALID'`},
		{`CREATE DATABASE IF NOT EXISTS a LC_COLLATE = 'C.UTF-8'`},
		{`CREATE DATABASE IF NOT EXISTS a LC_COLLATE = 'INVALID'`},
		{`CREATE DATABASE IF NOT EXISTS a LC_CTYPE = 'C.UTF-8'`},
		{`CREATE DATABASE IF NOT EXISTS a LC_CTYPE = 'INVALID'`},
		{`CREATE DATABASE IF NOT EXISTS a TEMPLATE = 'template0' ENCODING = 'UTF8' LC_COLLATE = 'C.UTF-8' LC_CTYPE = 'INVALID'`},

		{`CREATE INDEX a ON b (c)`},
		{`CREATE INDEX a ON b.c (d)`},
		{`CREATE INDEX ON a (b)`},
		{`CREATE INDEX ON a (b) STORING (c)`},
		{`CREATE INDEX ON a (b) INTERLEAVE IN PARENT c (d)`},
		{`CREATE INDEX ON a (b ASC, c DESC)`},
		{`CREATE UNIQUE INDEX a ON b (c)`},
		{`CREATE UNIQUE INDEX a ON b (c) STORING (d)`},
		{`CREATE UNIQUE INDEX a ON b (c) INTERLEAVE IN PARENT d (e, f)`},
		{`CREATE UNIQUE INDEX a ON b.c (d)`},

		{`CREATE TABLE a ()`},
		{`CREATE TABLE a (b INT)`},
		{`CREATE TABLE a (b INT, c INT)`},
		{`CREATE TABLE a (b CHAR)`},
		{`CREATE TABLE a (b CHAR(3))`},
		{`CREATE TABLE a (b VARCHAR)`},
		{`CREATE TABLE a (b VARCHAR(3))`},
		{`CREATE TABLE a (b STRING)`},
		{`CREATE TABLE a (b STRING(3))`},
		{`CREATE TABLE a (b FLOAT)`},
		{`CREATE TABLE a (b SERIAL)`},
		{`CREATE TABLE a (b SMALLSERIAL)`},
		{`CREATE TABLE a (b BIGSERIAL)`},
		{`CREATE TABLE a (b INT NULL)`},
		{`CREATE TABLE a (b INT CONSTRAINT maybe NULL)`},
		{`CREATE TABLE a (b INT NOT NULL)`},
		{`CREATE TABLE a (b INT CONSTRAINT always NOT NULL)`},
		{`CREATE TABLE a (b INT PRIMARY KEY)`},
		{`CREATE TABLE a (b INT UNIQUE)`},
		{`CREATE TABLE a (b INT NULL PRIMARY KEY)`},
		{`CREATE TABLE a (b INT DEFAULT 1)`},
		{`CREATE TABLE a (b INT CONSTRAINT one DEFAULT 1)`},
		{`CREATE TABLE a (b INT DEFAULT now())`},
		{`CREATE TABLE a (a INT CHECK (a > 0))`},
		{`CREATE TABLE a (a INT CONSTRAINT positive CHECK (a > 0))`},
		{`CREATE TABLE a (a INT DEFAULT 1 CHECK (a > 0))`},
		{`CREATE TABLE a (a INT CONSTRAINT one DEFAULT 1 CHECK (a > 0))`},
		{`CREATE TABLE a (a INT DEFAULT 1 CONSTRAINT positive CHECK (a > 0))`},
		{`CREATE TABLE a (a INT CONSTRAINT one DEFAULT 1 CONSTRAINT positive CHECK (a > 0))`},
		{`CREATE TABLE a (a INT CONSTRAINT one CHECK (a > 0) CONSTRAINT two CHECK (a < 10))`},
		// "0" lost quotes previously.
		{`CREATE TABLE a (b INT, c TEXT, PRIMARY KEY (b, c, "0"))`},
		{`CREATE TABLE a (b INT, c TEXT, FOREIGN KEY (b) REFERENCES other)`},
		{`CREATE TABLE a (b INT, c TEXT, FOREIGN KEY (b, c) REFERENCES other)`},
		{`CREATE TABLE a (b INT, c TEXT, FOREIGN KEY (b, c) REFERENCES other (x, y))`},
		{`CREATE TABLE a (b INT, c TEXT, CONSTRAINT s FOREIGN KEY (b, c) REFERENCES other (x, y))`},
		{`CREATE TABLE a (b INT, c TEXT, INDEX (b, c))`},
		{`CREATE TABLE a (b INT, c TEXT, INDEX d (b, c))`},
		{`CREATE TABLE a (b INT, c TEXT, CONSTRAINT d UNIQUE (b, c))`},
		{`CREATE TABLE a (b INT, c TEXT, CONSTRAINT d UNIQUE (b, c) INTERLEAVE IN PARENT d (e, f))`},
		{`CREATE TABLE a (b INT, UNIQUE (b))`},
		{`CREATE TABLE a (b INT, UNIQUE (b) STORING (c))`},
		{`CREATE TABLE a (b INT, INDEX (b))`},
		{`CREATE TABLE a (b INT, c INT REFERENCES foo)`},
		{`CREATE TABLE a (b INT, c INT CONSTRAINT ref REFERENCES foo)`},
		{`CREATE TABLE a (b INT, c INT REFERENCES foo (bar))`},
		{`CREATE TABLE a (b INT, INDEX (b) STORING (c))`},
		{`CREATE TABLE a (b INT, c TEXT, INDEX (b ASC, c DESC) STORING (c))`},
		{`CREATE TABLE a (b INT, INDEX (b) INTERLEAVE IN PARENT c (d, e))`},
		{`CREATE TABLE a (b INT, FAMILY (b))`},
		{`CREATE TABLE a (b INT, c STRING, FAMILY foo (b), FAMILY (c))`},
		{`CREATE TABLE a (b INT) INTERLEAVE IN PARENT foo (c, d)`},
		{`CREATE TABLE a (b INT) INTERLEAVE IN PARENT foo (c) CASCADE`},
		{`CREATE TABLE a.b (b INT)`},
		{`CREATE TABLE IF NOT EXISTS a (b INT)`},

		{`CREATE TABLE a AS SELECT * FROM b`},
		{`CREATE TABLE IF NOT EXISTS a AS SELECT * FROM b`},
		{`CREATE TABLE a AS SELECT * FROM b ORDER BY c`},
		{`CREATE TABLE IF NOT EXISTS a AS SELECT * FROM b ORDER BY c`},
		{`CREATE TABLE a AS SELECT * FROM b LIMIT 3`},
		{`CREATE TABLE IF NOT EXISTS a AS SELECT * FROM b LIMIT 3`},
		{`CREATE TABLE a AS VALUES ('one', 1), ('two', 2), ('three', 3)`},
		{`CREATE TABLE IF NOT EXISTS a AS VALUES ('one', 1), ('two', 2), ('three', 3)`},
		{`CREATE TABLE a (str, num) AS VALUES ('one', 1), ('two', 2), ('three', 3)`},
		{`CREATE TABLE IF NOT EXISTS a (str, num) AS VALUES ('one', 1), ('two', 2), ('three', 3)`},
		{`CREATE TABLE a AS SELECT * FROM b UNION SELECT * FROM c`},
		{`CREATE TABLE IF NOT EXISTS a AS SELECT * FROM b UNION SELECT * FROM c`},
		{`CREATE TABLE a AS SELECT * FROM b UNION VALUES ('one', 1) ORDER BY c LIMIT 5`},
		{`CREATE TABLE IF NOT EXISTS a AS SELECT * FROM b UNION VALUES ('one', 1) ORDER BY c LIMIT 5`},

		{`CREATE VIEW a AS SELECT * FROM b`},
		{`CREATE VIEW a AS SELECT b.* FROM b LIMIT 5`},
		{`CREATE VIEW a AS (SELECT c, d FROM b WHERE c > 0 ORDER BY c)`},
		{`CREATE VIEW a (x, y) AS SELECT c, d FROM b`},
		{`CREATE VIEW a AS VALUES (1, 'one'), (2, 'two')`},
		{`CREATE VIEW a (x, y) AS VALUES (1, 'one'), (2, 'two')`},
		{`CREATE VIEW a AS TABLE b`},

		{`DELETE FROM a`},
		{`DELETE FROM a.b`},
		{`DELETE FROM a WHERE a = b`},
		{`DELETE FROM a WHERE a = b RETURNING a, b`},
		{`DELETE FROM a WHERE a = b RETURNING 1, 2`},
		{`DELETE FROM a WHERE a = b RETURNING a + b`},
		{`DELETE FROM a WHERE a = b RETURNING NOTHING`},

		{`DROP DATABASE a`},
		{`DROP DATABASE IF EXISTS a`},
		{`DROP TABLE a`},
		{`DROP TABLE a.b`},
		{`DROP TABLE a, b`},
		{`DROP TABLE IF EXISTS a`},
		{`DROP TABLE a RESTRICT`},
		{`DROP TABLE a.b RESTRICT`},
		{`DROP TABLE a, b RESTRICT`},
		{`DROP TABLE IF EXISTS a RESTRICT`},
		{`DROP TABLE a CASCADE`},
		{`DROP TABLE a.b CASCADE`},
		{`DROP TABLE a, b CASCADE`},
		{`DROP TABLE IF EXISTS a CASCADE`},
		{`DROP INDEX a.b@c`},
		{`DROP INDEX a`},
		{`DROP INDEX a.b`},
		{`DROP INDEX IF EXISTS a.b@c`},
		{`DROP INDEX a.b@c, d@f`},
		{`DROP INDEX IF EXISTS a.b@c, d@f`},
		{`DROP INDEX a.b@c CASCADE`},
		{`DROP INDEX IF EXISTS a.b@c RESTRICT`},
		{`DROP VIEW a`},
		{`DROP VIEW a.b`},
		{`DROP VIEW a, b`},
		{`DROP VIEW IF EXISTS a`},
		{`DROP VIEW a RESTRICT`},
		{`DROP VIEW IF EXISTS a, b RESTRICT`},
		{`DROP VIEW a.b CASCADE`},
		{`DROP VIEW a, b CASCADE`},

		{`EXPLAIN SELECT 1`},
		{`EXPLAIN EXPLAIN SELECT 1`},
		{`EXPLAIN (DEBUG) SELECT 1`},
		{`EXPLAIN (A, B, C) SELECT 1`},
		{`SELECT * FROM [EXPLAIN SELECT 1]`},

		{`HELP count`},
		{`HELP VARCHAR`},

		{`SHOW BARFOO`},
		{`SHOW DATABASE`},
		{`SHOW SYNTAX`},

		{`SHOW CLUSTER SETTING a`},
		{`SHOW CLUSTER SETTING all`},

		{`SHOW DATABASES`},
		{`SHOW TABLES`},
		{`SHOW TABLES FROM a`},
		{`SHOW COLUMNS FROM a`},
		{`SHOW COLUMNS FROM a.b.c`},
		{`SHOW INDEXES FROM a`},
		{`SHOW INDEXES FROM a.b.c`},
		{`SHOW CONSTRAINTS FROM a`},
		{`SHOW CONSTRAINTS FROM a.b.c`},
		{`SHOW TABLES FROM a; SHOW COLUMNS FROM b`},
		{`SHOW USERS`},
		{`SHOW TESTING_RANGES FROM TABLE d.t`},
		{`SHOW TESTING_RANGES FROM TABLE t`},
		{`SHOW TESTING_RANGES FROM INDEX d.t@i`},
		{`SHOW TESTING_RANGES FROM INDEX t@i`},
		{`SHOW TESTING_RANGES FROM INDEX d.i`},
		{`SHOW TESTING_RANGES FROM INDEX i`},

		// Tables are the default, but can also be specified with
		// GRANT x ON TABLE y. However, the stringer does not output TABLE.
		{`SHOW GRANTS`},
		{`SHOW GRANTS ON foo`},
		{`SHOW GRANTS ON foo, db.foo`},
		{`SHOW GRANTS ON DATABASE foo, bar`},
		{`SHOW GRANTS ON DATABASE foo FOR bar`},
		{`SHOW GRANTS FOR bar, baz`},

		{`SHOW TRANSACTION ISOLATION LEVEL`},
		{`SHOW TRANSACTION PRIORITY`},

		{`PREPARE a AS SELECT 1`},
		{`PREPARE a AS INSERT INTO a VALUES (1)`},
		{`PREPARE a AS UPDATE a SET b = 3`},
		{`PREPARE a AS DELETE FROM a`},
		{`PREPARE a (INT) AS SELECT 1`},
		{`PREPARE a (STRING, STRING) AS SELECT 1`},

		{`EXECUTE a`},
		{`EXECUTE a (1)`},
		{`EXECUTE a (1, 1)`},
		{`EXECUTE a (1 + 1)`},

		{`DEALLOCATE a`},
		{`DEALLOCATE ALL`},

		// Tables are the default, but can also be specified with
		// GRANT x ON TABLE y. However, the stringer does not output TABLE.
		{`GRANT SELECT ON foo TO root`},
		{`GRANT SELECT, DELETE, UPDATE ON foo, db.foo TO root, bar`},
		{`GRANT DROP ON DATABASE foo TO root`},
		{`GRANT ALL ON DATABASE foo TO root, test`},
		{`GRANT SELECT, INSERT ON DATABASE bar TO foo, bar, baz`},
		{`GRANT SELECT, INSERT ON DATABASE db1, db2 TO foo, bar, baz`},
		{`GRANT SELECT, INSERT ON DATABASE db1, db2 TO "test-user"`},

		// Tables are the default, but can also be specified with
		// REVOKE x ON TABLE y. However, the stringer does not output TABLE.
		{`REVOKE SELECT ON foo FROM root`},
		{`REVOKE UPDATE, DELETE ON foo, db.foo FROM root, bar`},
		{`REVOKE INSERT ON DATABASE foo FROM root`},
		{`REVOKE ALL ON DATABASE foo FROM root, test`},
		{`REVOKE SELECT, INSERT ON DATABASE bar FROM foo, bar, baz`},
		{`REVOKE SELECT, INSERT ON DATABASE db1, db2 FROM foo, bar, baz`},

		{`INSERT INTO a VALUES (1)`},
		{`INSERT INTO a.b VALUES (1)`},
		{`INSERT INTO a VALUES (1, 2)`},
		{`INSERT INTO a VALUES (1, DEFAULT)`},
		{`INSERT INTO a VALUES (1, 2), (3, 4)`},
		{`INSERT INTO a VALUES (a + 1, 2 * 3)`},
		{`INSERT INTO a(a, b) VALUES (1, 2)`},
		{`INSERT INTO a(a, a.b) VALUES (1, 2)`},
		{`INSERT INTO a SELECT b, c FROM d`},
		{`INSERT INTO a DEFAULT VALUES`},
		{`INSERT INTO a VALUES (1) RETURNING a, b`},
		{`INSERT INTO a VALUES (1, 2) RETURNING 1, 2`},
		{`INSERT INTO a VALUES (1, 2) RETURNING a + b, c`},
		{`INSERT INTO a VALUES (1, 2) RETURNING NOTHING`},

		{`UPSERT INTO a VALUES (1)`},
		{`UPSERT INTO a.b VALUES (1)`},
		{`UPSERT INTO a VALUES (1, 2)`},
		{`UPSERT INTO a VALUES (1, DEFAULT)`},
		{`UPSERT INTO a VALUES (1, 2), (3, 4)`},
		{`UPSERT INTO a VALUES (a + 1, 2 * 3)`},
		{`UPSERT INTO a(a, b) VALUES (1, 2)`},
		{`UPSERT INTO a(a, a.b) VALUES (1, 2)`},
		{`UPSERT INTO a SELECT b, c FROM d`},
		{`UPSERT INTO a DEFAULT VALUES`},
		{`UPSERT INTO a DEFAULT VALUES RETURNING a, b`},
		{`UPSERT INTO a DEFAULT VALUES RETURNING 1, 2`},
		{`UPSERT INTO a DEFAULT VALUES RETURNING a + b`},
		{`UPSERT INTO a DEFAULT VALUES RETURNING NOTHING`},

		{`INSERT INTO a VALUES (1) ON CONFLICT DO NOTHING`},
		{`INSERT INTO a VALUES (1) ON CONFLICT (a) DO NOTHING`},
		{`INSERT INTO a VALUES (1) ON CONFLICT DO UPDATE SET a = 1`},
		{`INSERT INTO a VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 1`},
		{`INSERT INTO a VALUES (1) ON CONFLICT (a, b) DO UPDATE SET a = 1`},
		{`INSERT INTO a VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 1, b = excluded.a`},
		{`INSERT INTO a VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 1 WHERE b > 2`},
		{`INSERT INTO a VALUES (1) ON CONFLICT (a) DO UPDATE SET a = DEFAULT`},
		{`INSERT INTO a VALUES (1) ON CONFLICT (a) DO UPDATE SET (a, b) = (SELECT 1, 2)`},
		{`INSERT INTO a VALUES (1) ON CONFLICT (a) DO UPDATE SET (a, b) = (SELECT 1, 2) RETURNING a, b`},
		{`INSERT INTO a VALUES (1) ON CONFLICT (a) DO UPDATE SET (a, b) = (SELECT 1, 2) RETURNING 1, 2`},
		{`INSERT INTO a VALUES (1) ON CONFLICT (a) DO UPDATE SET (a, b) = (SELECT 1, 2) RETURNING a + b`},
		{`INSERT INTO a VALUES (1) ON CONFLICT (a) DO UPDATE SET (a, b) = (SELECT 1, 2) RETURNING NOTHING`},

		{`SELECT 1 + 1`},
		{`SELECT - 1`},
		{`SELECT + 1`},
		{`SELECT .1`},
		{`SELECT 1.2e1`},
		{`SELECT 1.2e+1`},
		{`SELECT 1.2e-1`},
		{`SELECT true AND false`},
		{`SELECT true AND NULL`},
		{`SELECT true = false`},
		{`SELECT (true = false)`},
		{`SELECT (ARRAY['a', 'b'])[2]`},
		{`SELECT (ARRAY(VALUES (1), (2)))[1]`},
		{`SELECT (SELECT 1)`},
		{`SELECT ((SELECT 1))`},
		{`SELECT (SELECT ARRAY['a', 'b'])[2]`},
		{`SELECT ((SELECT ARRAY['a', 'b']))[2]`},
		{`SELECT ((((VALUES (1)))))`},
		{`SELECT EXISTS (SELECT 1)`},
		{`SELECT (VALUES (1))`},
		{`SELECT (1, 2, 3)`},
		{`SELECT (ROW(1, 2, 3))`},
		{`SELECT (ROW())`},
		{`SELECT (TABLE a)`},
		{`SELECT 0x1`},

		{`SELECT 1 FROM t`},
		{`SELECT 1, 2 FROM t`},
		{`SELECT * FROM t`},
		{`SELECT "*" FROM t`},
		{`SELECT a, b FROM t`},
		{`SELECT a AS b FROM t`},
		{`SELECT a.* FROM t`},
		{`SELECT a = b FROM t`},
		{`SELECT $1 FROM t`},
		{`SELECT $1, $2 FROM t`},
		{`SELECT NULL FROM t`},
		{`SELECT 0.1 FROM t`},
		{`SELECT a FROM t`},
		{`SELECT a.b FROM t`},
		{`SELECT a.b.* FROM t`},
		{`SELECT a.b[1] FROM t`},
		{`SELECT a.b[1 + 1:4][3] FROM t`},
		{`SELECT a.b[:4][3] FROM t`},
		{`SELECT a.b[1 + 1:][3] FROM t`},
		{`SELECT a.b[:][3] FROM t`},
		{`SELECT 'a' FROM t`},
		{`SELECT 'a' FROM t@bar`},
		{`SELECT 'a' FROM t@{NO_INDEX_JOIN}`},
		{`SELECT 'a' FROM t@{FORCE_INDEX=bar,NO_INDEX_JOIN}`},

		{`SELECT '1':::INT`},

		{`SELECT '1'::INT`},
		{`SELECT BOOL 'foo'`},
		{`SELECT INT 'foo'`},
		{`SELECT REAL 'foo'`},
		{`SELECT DECIMAL 'foo'`},
		{`SELECT DATE 'foo'`},
		{`SELECT TIMESTAMP 'foo'`},
		{`SELECT TIMESTAMP WITH TIME ZONE 'foo'`},
		{`SELECT CHAR 'foo'`},

		{`SELECT 'a' AS "12345"`},
		{`SELECT 'a' AS clnm`},

		{`SELECT 0xf0 FROM t`},
		{`SELECT 0xF0 FROM t`},

		// Escaping may change since the scanning process loses information
		// (you can write e'\'' or ''''), but these are the idempotent cases.
		// Generally, anything that needs to escape plus \ and ' leads to an
		// escaped string.
		{`SELECT e'a\'a' FROM t`},
		{`SELECT e'a\\\\na' FROM t`},
		{`SELECT e'\\\\n' FROM t`},
		{`SELECT "a""a" FROM t`},
		{`SELECT a FROM "t\n"`}, // no escaping in sql identifiers
		{`SELECT a FROM "t"""`}, // no escaping in sql identifiers

		{`SELECT "FROM" FROM t`},
		{`SELECT CAST(1 AS TEXT)`},
		{`SELECT ANNOTATE_TYPE(1, TEXT)`},
		{`SELECT a FROM t AS bar`},
		{`SELECT a FROM t AS bar (bar1)`},
		{`SELECT a FROM t AS bar (bar1, bar2, bar3)`},
		{`SELECT a FROM t WITH ORDINALITY`},
		{`SELECT a FROM t WITH ORDINALITY AS bar`},
		{`SELECT a FROM (SELECT 1 FROM t)`},
		{`SELECT a FROM (SELECT 1 FROM t) AS bar`},
		{`SELECT a FROM (SELECT 1 FROM t) AS bar (bar1)`},
		{`SELECT a FROM (SELECT 1 FROM t) AS bar (bar1, bar2, bar3)`},
		{`SELECT a FROM (SELECT 1 FROM t) WITH ORDINALITY`},
		{`SELECT a FROM (SELECT 1 FROM t) WITH ORDINALITY AS bar`},
		{`SELECT a FROM generate_series(1, 32)`},
		{`SELECT a FROM generate_series(1, 32) AS s (x)`},
		{`SELECT a FROM generate_series(1, 32) WITH ORDINALITY AS s (x)`},
		{`SELECT a FROM t1, t2`},
		{`SELECT a FROM t AS t1`},
		{`SELECT a FROM t AS t1 (c1)`},
		{`SELECT a FROM t AS t1 (c1, c2, c3, c4)`},
		{`SELECT a FROM s.t`},

		{`SELECT COUNT(DISTINCT a) FROM t`},
		{`SELECT COUNT(ALL a) FROM t`},

		{`SELECT a FROM t WHERE a = b`},
		{`SELECT a FROM t WHERE NOT (a = b)`},
		{`SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t)`},
		{`SELECT a FROM t WHERE NOT true`},
		{`SELECT a FROM t WHERE NOT false`},
		{`SELECT a FROM t WHERE a IN (b)`},
		{`SELECT a FROM t WHERE a IN (b, c)`},
		{`SELECT a FROM t WHERE a IN (SELECT a FROM t)`},
		{`SELECT a FROM t WHERE a NOT IN (b, c)`},
		{`SELECT a FROM t WHERE a = ANY ARRAY[b, c]`},
		{`SELECT a FROM t WHERE a != SOME ARRAY[b, c]`},
		{`SELECT a FROM t WHERE a LIKE ALL ARRAY[b, c]`},
		{`SELECT a FROM t WHERE a LIKE b`},
		{`SELECT a FROM t WHERE a NOT LIKE b`},
		{`SELECT a FROM t WHERE a ILIKE b`},
		{`SELECT a FROM t WHERE a NOT ILIKE b`},
		{`SELECT a FROM t WHERE a SIMILAR TO b`},
		{`SELECT a FROM t WHERE a NOT SIMILAR TO b`},
		{`SELECT a FROM t WHERE a ~ b`},
		{`SELECT a FROM t WHERE a !~ b`},
		{`SELECT a FROM t WHERE a ~* c`},
		{`SELECT a FROM t WHERE a !~* c`},
		{`SELECT a FROM t WHERE a BETWEEN b AND c`},
		{`SELECT a FROM t WHERE a NOT BETWEEN b AND c`},
		{`SELECT a FROM t WHERE a IS NULL`},
		{`SELECT a FROM t WHERE a IS NOT NULL`},
		{`SELECT a FROM t WHERE a IS true`},
		{`SELECT a FROM t WHERE a IS NOT true`},
		{`SELECT a FROM t WHERE a IS false`},
		{`SELECT a FROM t WHERE a IS NOT false`},
		{`SELECT a FROM t WHERE a IS OF (INT)`},
		{`SELECT a FROM t WHERE a IS NOT OF (FLOAT, STRING)`},
		{`SELECT a FROM t WHERE a IS DISTINCT FROM b`},
		{`SELECT a FROM t WHERE a IS NOT DISTINCT FROM b`},
		{`SELECT a FROM t WHERE a < b`},
		{`SELECT a FROM t WHERE a <= b`},
		{`SELECT a FROM t WHERE a >= b`},
		{`SELECT a FROM t WHERE a != b`},
		{`SELECT a FROM t WHERE a = (SELECT a FROM t)`},
		{`SELECT a FROM t WHERE a = (b)`},
		{`SELECT a FROM t WHERE CASE WHEN a = b THEN c END`},
		{`SELECT a FROM t WHERE CASE WHEN a = b THEN c ELSE d END`},
		{`SELECT a FROM t WHERE CASE WHEN a = b THEN c WHEN b = d THEN d ELSE d END`},
		{`SELECT a FROM t WHERE CASE aa WHEN a = b THEN c END`},
		{`SELECT a FROM t WHERE a = B()`},
		{`SELECT a FROM t WHERE a = B(c)`},
		{`SELECT a FROM t WHERE a = B(c, d)`},
		{`SELECT a FROM t WHERE a = COUNT(*)`},
		{`SELECT a FROM t WHERE a = IF(b, c, d)`},
		{`SELECT a FROM t WHERE a = IFNULL(b, c)`},
		{`SELECT a FROM t WHERE a = NULLIF(b, c)`},
		{`SELECT a FROM t WHERE a = COALESCE(a, b, c, d, e)`},
		{`SELECT (a.b) FROM t WHERE (b.c) = 2`},

		{`SELECT a FROM t ORDER BY a`},
		{`SELECT a FROM t ORDER BY a ASC`},
		{`SELECT a FROM t ORDER BY a DESC`},

		{`SELECT 1 FROM t GROUP BY a`},
		{`SELECT 1 FROM t GROUP BY a, b`},

		{`SELECT a FROM t HAVING a = b`},

		{`SELECT a FROM t WINDOW w AS ()`},
		{`SELECT a FROM t WINDOW w AS (w2)`},
		{`SELECT a FROM t WINDOW w AS (PARTITION BY b)`},
		{`SELECT a FROM t WINDOW w AS (PARTITION BY b, 1 + 2)`},
		{`SELECT a FROM t WINDOW w AS (ORDER BY c)`},
		{`SELECT a FROM t WINDOW w AS (ORDER BY c, 1 + 2)`},
		{`SELECT a FROM t WINDOW w AS (PARTITION BY b ORDER BY c)`},

		{`SELECT avg(1) OVER w FROM t`},
		{`SELECT avg(1) OVER () FROM t`},
		{`SELECT avg(1) OVER (w) FROM t`},
		{`SELECT avg(1) OVER (PARTITION BY b) FROM t`},
		{`SELECT avg(1) OVER (ORDER BY c) FROM t`},
		{`SELECT avg(1) OVER (PARTITION BY b ORDER BY c) FROM t`},
		{`SELECT avg(1) OVER (w PARTITION BY b ORDER BY c) FROM t`},

		{`SELECT a FROM t UNION SELECT 1 FROM t`},
		{`SELECT a FROM t UNION SELECT 1 FROM t UNION SELECT 1 FROM t`},
		{`SELECT a FROM t UNION ALL SELECT 1 FROM t`},
		{`SELECT a FROM t EXCEPT SELECT 1 FROM t`},
		{`SELECT a FROM t EXCEPT ALL SELECT 1 FROM t`},
		{`SELECT a FROM t INTERSECT SELECT 1 FROM t`},
		{`SELECT a FROM t INTERSECT ALL SELECT 1 FROM t`},

		{`SELECT a FROM t1 JOIN t2 ON a = b`},
		{`SELECT a FROM t1 JOIN t2 USING (a)`},
		{`SELECT a FROM t1 LEFT JOIN t2 ON a = b`},
		{`SELECT a FROM t1 RIGHT JOIN t2 ON a = b`},
		{`SELECT a FROM t1 INNER JOIN t2 ON a = b`},
		{`SELECT a FROM t1 CROSS JOIN t2`},
		{`SELECT a FROM t1 NATURAL JOIN t2`},
		{`SELECT a FROM t1 INNER JOIN t2 USING (a)`},
		{`SELECT a FROM t1 FULL JOIN t2 USING (a)`},

		{`SELECT a FROM t1 AS OF SYSTEM TIME '2016-01-01'`},
		{`SELECT a FROM t1, t2 AS OF SYSTEM TIME '2016-01-01'`},

		{`SELECT a FROM t LIMIT a`},
		{`SELECT a FROM t OFFSET b`},
		{`SELECT a FROM t LIMIT a OFFSET b`},
		{`SELECT DISTINCT * FROM t`},
		{`SELECT DISTINCT a, b FROM t`},
		{`SET a = 3`},
		{`SET a = 3, 4`},
		{`SET a = '3'`},
		{`SET a = 3.0`},
		{`SET a = $1`},
		{`SET TRANSACTION ISOLATION LEVEL SNAPSHOT`},
		{`SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`},
		{`SET TRANSACTION PRIORITY LOW`},
		{`SET TRANSACTION PRIORITY NORMAL`},
		{`SET TRANSACTION PRIORITY HIGH`},
		{`SET TRANSACTION ISOLATION LEVEL SNAPSHOT, PRIORITY HIGH`},
		{`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE`},
		{`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SNAPSHOT`},
		{`SET CLUSTER SETTING a = 3`},
		{`SET CLUSTER SETTING a = '3s'`},
		{`SET CLUSTER SETTING a = '3'`},
		{`SET CLUSTER SETTING a = 3.0`},
		{`SET CLUSTER SETTING a = $1`},
		{`SET TIME ZONE 'pst8pdt'`},
		{`SET TIME ZONE 'Europe/Rome'`},
		{`SET TIME ZONE -7`},
		{`SET TIME ZONE -7.3`},
		{`SET TIME ZONE DEFAULT`},
		{`SET TIME ZONE LOCAL`},
		{`RESET a`},

		{`SELECT * FROM (VALUES (1, 2)) AS foo`},
		{`SELECT * FROM (VALUES (1, 2)) AS foo (a, b)`},

		{`SELECT * FROM [123] AS t`},
		{`SELECT * FROM [123(1, 2, 3)] AS t`},
		{`SELECT * FROM [123()] AS t`},
		{`SELECT * FROM t@[123]`},
		{`SELECT * FROM t@{FORCE_INDEX=[123],NO_INDEX_JOIN}`},
		{`SELECT * FROM [123]@[456] AS t`},
		{`SELECT * FROM [123]@{FORCE_INDEX=[456],NO_INDEX_JOIN} AS t`},

		// TODO(pmattis): Is this a postgres extension?
		{`TABLE a`}, // Shorthand for: SELECT * FROM a

		{`TRUNCATE TABLE a`},
		{`TRUNCATE TABLE a, b.c`},
		{`TRUNCATE TABLE a CASCADE`},

		{`UPDATE a SET b = 3`},
		{`UPDATE a.b SET b = 3`},
		{`UPDATE a SET b.c = 3`},
		{`UPDATE a SET b = 3, c = DEFAULT`},
		{`UPDATE a SET b = 3 + 4`},
		{`UPDATE a SET (b, c) = (3, DEFAULT)`},
		{`UPDATE a SET (b, c) = (SELECT 3, 4)`},
		{`UPDATE a SET b = 3 WHERE a = b`},
		{`UPDATE a SET b = 3 WHERE a = b RETURNING a`},
		{`UPDATE a SET b = 3 WHERE a = b RETURNING 1, 2`},
		{`UPDATE a SET b = 3 WHERE a = b RETURNING a, a + b`},
		{`UPDATE a SET b = 3 WHERE a = b RETURNING NOTHING`},

		{`UPDATE T AS "0" SET K = ''`},                 // "0" lost its quotes
		{`SELECT * FROM "0" JOIN "0" USING (id, "0")`}, // last "0" lost its quotes.

		{`ALTER DATABASE a RENAME TO b`},
		{`ALTER TABLE a RENAME TO b`},
		{`ALTER TABLE IF EXISTS a RENAME TO b`},
		{`ALTER INDEX a@b RENAME TO b`},
		{`ALTER INDEX b RENAME TO b`},
		{`ALTER INDEX IF EXISTS a@b RENAME TO b`},
		{`ALTER TABLE a RENAME COLUMN c1 TO c2`},
		{`ALTER TABLE IF EXISTS a RENAME COLUMN c1 TO c2`},

		{`ALTER TABLE a ADD b INT, ADD CONSTRAINT a_idx UNIQUE (a)`},
		{`ALTER TABLE a ADD IF NOT EXISTS b INT, ADD CONSTRAINT a_idx UNIQUE (a)`},
		{`ALTER TABLE IF EXISTS a ADD b INT, ADD CONSTRAINT a_idx UNIQUE (a)`},
		{`ALTER TABLE IF EXISTS a ADD IF NOT EXISTS b INT, ADD CONSTRAINT a_idx UNIQUE (a)`},
		{`ALTER TABLE a ADD COLUMN b INT, ADD CONSTRAINT a_idx UNIQUE (a)`},
		{`ALTER TABLE a ADD COLUMN IF NOT EXISTS b INT, ADD CONSTRAINT a_idx UNIQUE (a) NOT VALID`},
		{`ALTER TABLE IF EXISTS a ADD COLUMN b INT, ADD CONSTRAINT a_idx UNIQUE (a)`},
		{`ALTER TABLE IF EXISTS a ADD COLUMN IF NOT EXISTS b INT, ADD CONSTRAINT a_idx UNIQUE (a)`},
		{`ALTER TABLE a ADD b INT FAMILY fam_a`},
		{`ALTER TABLE a ADD b INT CREATE FAMILY`},
		{`ALTER TABLE a ADD b INT CREATE FAMILY fam_b`},
		{`ALTER TABLE a ADD b INT CREATE IF NOT EXISTS FAMILY fam_b`},

		{`ALTER TABLE a DROP b, DROP CONSTRAINT a_idx`},
		{`ALTER TABLE a DROP IF EXISTS b, DROP CONSTRAINT a_idx`},
		{`ALTER TABLE IF EXISTS a DROP b, DROP CONSTRAINT a_idx`},
		{`ALTER TABLE IF EXISTS a DROP IF EXISTS b, DROP CONSTRAINT a_idx`},
		{`ALTER TABLE a DROP COLUMN b, DROP CONSTRAINT a_idx`},
		{`ALTER TABLE a DROP COLUMN IF EXISTS b, DROP CONSTRAINT a_idx`},
		{`ALTER TABLE IF EXISTS a DROP COLUMN b, DROP CONSTRAINT a_idx`},
		{`ALTER TABLE IF EXISTS a DROP COLUMN IF EXISTS b, DROP CONSTRAINT a_idx`},
		{`ALTER TABLE a DROP COLUMN b CASCADE`},
		{`ALTER TABLE a DROP COLUMN b RESTRICT`},
		{`ALTER TABLE a DROP CONSTRAINT b CASCADE`},
		{`ALTER TABLE a DROP CONSTRAINT IF EXISTS b RESTRICT`},
		{`ALTER TABLE a VALIDATE CONSTRAINT a`},

		{`ALTER TABLE a ALTER COLUMN b SET DEFAULT 42`},
		{`ALTER TABLE a ALTER COLUMN b SET DEFAULT NULL`},
		{`ALTER TABLE a ALTER COLUMN b DROP DEFAULT`},
		{`ALTER TABLE a ALTER COLUMN b DROP NOT NULL`},
		{`ALTER TABLE a ALTER b DROP NOT NULL`},

		{`COPY t FROM STDIN`},
		{`COPY t (a, b, c) FROM STDIN`},

		{`ALTER TABLE a SPLIT AT VALUES (1)`},
		{`ALTER TABLE a SPLIT AT SELECT * FROM t`},
		{`ALTER TABLE d.a SPLIT AT VALUES ('b', 2)`},
		{`ALTER INDEX a@i SPLIT AT VALUES (1)`},
		{`ALTER INDEX d.a@i SPLIT AT VALUES (2)`},
		{`ALTER INDEX i SPLIT AT VALUES (1)`},
		{`ALTER INDEX d.i SPLIT AT VALUES (2)`},

		{`ALTER TABLE a TESTING_RELOCATE VALUES (ARRAY[1], 1)`},
		{`ALTER TABLE a TESTING_RELOCATE SELECT * FROM t`},
		{`ALTER TABLE d.a TESTING_RELOCATE VALUES (ARRAY[1, 2, 3], 'b', 2)`},
		{`ALTER INDEX d.i TESTING_RELOCATE VALUES (ARRAY[1], 2)`},

		{`ALTER TABLE a SCATTER`},
		{`ALTER TABLE a SCATTER FROM (1, 2, 3) TO (4, 5, 6)`},
		{`ALTER TABLE d.a SCATTER`},
		{`ALTER INDEX d.i SCATTER FROM (1) TO (2)`},

		{`BACKUP foo TO 'bar'`},
		{`BACKUP foo.foo, baz.baz TO 'bar'`},
		{`BACKUP foo TO 'bar' AS OF SYSTEM TIME '1' INCREMENTAL FROM 'baz'`},
		{`BACKUP foo TO $1 INCREMENTAL FROM 'bar', $2, 'baz'`},
		{`BACKUP DATABASE foo TO 'bar'`},
		{`BACKUP DATABASE foo, baz TO 'bar'`},
		{`BACKUP DATABASE foo TO 'bar' AS OF SYSTEM TIME '1' INCREMENTAL FROM 'baz'`},
		{`RESTORE foo FROM 'bar'`},
		{`RESTORE foo FROM $1`},
		{`RESTORE foo FROM $1, $2, 'bar'`},
		{`RESTORE foo, baz FROM 'bar'`},
		{`RESTORE foo, baz FROM 'bar' AS OF SYSTEM TIME '1'`},
		{`RESTORE DATABASE foo FROM 'bar'`},
		{`RESTORE DATABASE foo, baz FROM 'bar'`},
		{`RESTORE DATABASE foo, baz FROM 'bar' AS OF SYSTEM TIME '1'`},
		{`BACKUP foo TO 'bar' WITH OPTIONS ('key1', 'key2'='value')`},
		{`RESTORE foo FROM 'bar' WITH OPTIONS ('key1', 'key2'='value')`},
	}
	for _, d := range testData {
		stmts, err := parse(d.sql)
		if err != nil {
			t.Fatalf("%s: expected success, but found %s", d.sql, err)
		}
		s := stmts.String()
		if d.sql != s {
			t.Errorf("expected %s, but found %s", d.sql, s)
		}
	}
}

// TestParse2 verifies that we can parse the supplied SQL and regenerate the
// expected SQL string from the syntax tree. Note that if the input and output
// SQL strings are the same, the test case should go in TestParse instead.
func TestParse2(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
	}{
		{`CREATE DATABASE a WITH ENCODING = 'foo'`,
			`CREATE DATABASE a ENCODING = 'foo'`},
		{`CREATE DATABASE a TEMPLATE = template0`,
			`CREATE DATABASE a TEMPLATE = 'template0'`},
		{`CREATE DATABASE a TEMPLATE = invalid`,
			`CREATE DATABASE a TEMPLATE = 'invalid'`},
		{`CREATE TABLE a (b INT, UNIQUE INDEX foo (b))`,
			`CREATE TABLE a (b INT, CONSTRAINT foo UNIQUE (b))`},
		{`CREATE TABLE a (b INT, UNIQUE INDEX foo (b) INTERLEAVE IN PARENT c (d))`,
			`CREATE TABLE a (b INT, CONSTRAINT foo UNIQUE (b) INTERLEAVE IN PARENT c (d))`},
		{`CREATE INDEX ON a (b) COVERING (c)`, `CREATE INDEX ON a (b) STORING (c)`},

		{`SELECT TIMESTAMP WITHOUT TIME ZONE 'foo'`, `SELECT TIMESTAMP 'foo'`},
		{`SELECT CAST('foo' AS TIMESTAMP WITHOUT TIME ZONE)`, `SELECT CAST('foo' AS TIMESTAMP)`},

		{`SELECT 'a' FROM t@{FORCE_INDEX=bar}`, `SELECT 'a' FROM t@bar`},
		{`SELECT 'a' FROM t@{NO_INDEX_JOIN,FORCE_INDEX=bar}`,
			`SELECT 'a' FROM t@{FORCE_INDEX=bar,NO_INDEX_JOIN}`},

		{`SELECT 'a' FROM t@{FORCE_INDEX=[123]}`, `SELECT 'a' FROM t@[123]`},
		{`SELECT 'a' FROM [123]@{FORCE_INDEX=[456]} AS t`, `SELECT 'a' FROM [123]@[456] AS t`},

		{`SELECT a FROM t WHERE a IS UNKNOWN`, `SELECT a FROM t WHERE a IS NULL`},
		{`SELECT a FROM t WHERE a IS NOT UNKNOWN`, `SELECT a FROM t WHERE a IS NOT NULL`},

		{`SELECT - - 5`, `SELECT - (- 5)`},
		{`SELECT a FROM t WHERE b = - 2`, `SELECT a FROM t WHERE b = (- 2)`},
		{`SELECT a FROM t WHERE a = b AND a = c`, `SELECT a FROM t WHERE (a = b) AND (a = c)`},
		{`SELECT a FROM t WHERE a = b OR a = c`, `SELECT a FROM t WHERE (a = b) OR (a = c)`},
		{`SELECT a FROM t WHERE NOT a = b`, `SELECT a FROM t WHERE NOT (a = b)`},

		{`SELECT a FROM t WHERE a = b & c`, `SELECT a FROM t WHERE a = (b & c)`},
		{`SELECT a FROM t WHERE a = b | c`, `SELECT a FROM t WHERE a = (b | c)`},
		{`SELECT a FROM t WHERE a = b # c`, `SELECT a FROM t WHERE a = (b # c)`},
		{`SELECT a FROM t WHERE a = b ^ c`, `SELECT a FROM t WHERE a = (b ^ c)`},
		{`SELECT a FROM t WHERE a = b + c`, `SELECT a FROM t WHERE a = (b + c)`},
		{`SELECT a FROM t WHERE a = b - c`, `SELECT a FROM t WHERE a = (b - c)`},
		{`SELECT a FROM t WHERE a = b * c`, `SELECT a FROM t WHERE a = (b * c)`},
		{`SELECT a FROM t WHERE a = b / c`, `SELECT a FROM t WHERE a = (b / c)`},
		{`SELECT a FROM t WHERE a = b % c`, `SELECT a FROM t WHERE a = (b % c)`},
		{`SELECT a FROM t WHERE a = b || c`, `SELECT a FROM t WHERE a = (b || c)`},
		{`SELECT a FROM t WHERE a = + b`, `SELECT a FROM t WHERE a = (+ b)`},
		{`SELECT a FROM t WHERE a = - b`, `SELECT a FROM t WHERE a = (- b)`},
		{`SELECT a FROM t WHERE a = ~ b`, `SELECT a FROM t WHERE a = (~ b)`},

		// Escaped string literals are not always escaped the same because
		// '''' and e'\'' scan to the same token. It's more convenient to
		// prefer escaping ' and \, so we do that.
		{`SELECT 'a''a'`,
			`SELECT e'a\'a'`},
		{`SELECT 'a\a'`,
			`SELECT e'a\\a'`},
		{`SELECT 'a\n'`,
			`SELECT e'a\\n'`},
		{"SELECT '\n'",
			`SELECT e'\n'`},
		{"SELECT '\n\\'",
			`SELECT e'\n\\'`},
		{`SELECT "a'a" FROM t`,
			`SELECT "a'a" FROM t`},
		// Hexadecimal literal strings are turned into regular strings.
		{`SELECT x'61'`, `SELECT b'a'`},
		{`SELECT X'61'`, `SELECT b'a'`},
		// Comments are stripped.
		{`SELECT 1 FROM t -- hello world`,
			`SELECT 1 FROM t`},
		{`SELECT /* hello world */ 1 FROM t`,
			`SELECT 1 FROM t`},
		{`SELECT /* hello */ 1 FROM /* world */ t`,
			`SELECT 1 FROM t`},
		// Alias expressions are always output using AS.
		{`SELECT 1 FROM t t1`,
			`SELECT 1 FROM t AS t1`},
		{`SELECT 1 FROM t t1 (c1, c2)`,
			`SELECT 1 FROM t AS t1 (c1, c2)`},
		// Alternate not-equal operator.
		{`SELECT a FROM t WHERE a <> b`,
			`SELECT a FROM t WHERE a != b`},
		// OUTER is syntactic sugar.
		{`SELECT a FROM t1 LEFT OUTER JOIN t2 ON a = b`,
			`SELECT a FROM t1 LEFT JOIN t2 ON a = b`},
		{`SELECT a FROM t1 RIGHT OUTER JOIN t2 ON a = b`,
			`SELECT a FROM t1 RIGHT JOIN t2 ON a = b`},
		// Some functions are nearly keywords.
		{`SELECT CURRENT_TIMESTAMP`,
			`SELECT current_timestamp()`},
		{`SELECT CURRENT_DATE`,
			`SELECT current_date()`},
		{`SELECT POSITION(a IN b)`,
			`SELECT strpos(b, a)`},
		{`SELECT TRIM(BOTH a FROM b)`,
			`SELECT btrim(b, a)`},
		{`SELECT TRIM(LEADING a FROM b)`,
			`SELECT ltrim(b, a)`},
		{`SELECT TRIM(TRAILING a FROM b)`,
			`SELECT rtrim(b, a)`},
		{`SELECT TRIM(a, b)`,
			`SELECT btrim(a, b)`},
		// Offset has an optional ROW/ROWS keyword.
		{`SELECT a FROM t1 OFFSET a ROW`,
			`SELECT a FROM t1 OFFSET a`},
		{`SELECT a FROM t1 OFFSET a ROWS`,
			`SELECT a FROM t1 OFFSET a`},
		// We allow OFFSET before LIMIT, but always output LIMIT first.
		{`SELECT a FROM t OFFSET a LIMIT b`,
			`SELECT a FROM t LIMIT b OFFSET a`},
		// Double negation. See #1800.
		{`SELECT *,-/* comment */-5`,
			`SELECT *, - (- 5)`},
		{"SELECT -\n-5",
			`SELECT - (- 5)`},
		{`SELECT -0.-/*test*/-1`,
			`SELECT (- 0.) - (- 1)`,
		},
		// See #1948.
		{`SELECT~~+~++~bd(*)`,
			`SELECT ~ (~ (+ (~ (+ (+ (~ bd(*)))))))`},
		// See #1957.
		{`SELECT+y[array[]]`,
			`SELECT + y[ARRAY[]]`},
		{`SELECT a FROM t UNION DISTINCT SELECT 1 FROM t`,
			`SELECT a FROM t UNION SELECT 1 FROM t`},
		{`SELECT a FROM t EXCEPT DISTINCT SELECT 1 FROM t`,
			`SELECT a FROM t EXCEPT SELECT 1 FROM t`},
		{`SELECT a FROM t INTERSECT DISTINCT SELECT 1 FROM t`,
			`SELECT a FROM t INTERSECT SELECT 1 FROM t`},
		{`SET TIME ZONE pst8pdt`,
			`SET TIME ZONE 'pst8pdt'`},
		{`SET TIME ZONE "Europe/Rome"`,
			`SET TIME ZONE 'Europe/Rome'`},
		{`SET TIME ZONE INTERVAL '-7h'`,
			`SET TIME ZONE '-7h'`},
		{`SET TIME ZONE INTERVAL '-7h0m5s' HOUR TO MINUTE`,
			`SET TIME ZONE '-6h-59m'`},
		// Special substring syntax
		{`SELECT SUBSTRING('RoacH' from 2 for 3)`,
			`SELECT substring('RoacH', 2, 3)`},
		{`SELECT SUBSTRING('RoacH' for 2 from 3)`,
			`SELECT substring('RoacH', 3, 2)`},
		{`SELECT SUBSTRING('RoacH' from 2)`,
			`SELECT substring('RoacH', 2)`},
		{`SELECT SUBSTRING('RoacH' for 3)`,
			`SELECT substring('RoacH', 1, 3)`},
		{`SELECT SUBSTRING('f(oabaroob' from '\(o(.)b')`,
			`SELECT substring('f(oabaroob', e'\\(o(.)b')`},
		{`SELECT SUBSTRING('f(oabaroob' from '+(o(.)b' for '+')`,
			`SELECT substring('f(oabaroob', '+(o(.)b', '+')`},
		// Special position syntax
		{`SELECT POSITION('ig' in 'high')`,
			`SELECT strpos('high', 'ig')`},
		// Special overlay syntax
		{`SELECT OVERLAY('w33333rce' PLACING 'resou' FROM 3)`,
			`SELECT overlay('w33333rce', 'resou', 3)`},
		{`SELECT OVERLAY('w33333rce' PLACING 'resou' FROM 3 FOR 5)`,
			`SELECT overlay('w33333rce', 'resou', 3, 5)`},
		// Special extract syntax
		{`SELECT EXTRACT(second from now())`,
			`SELECT extract('second', now())`},
		// Special trim syntax
		{`SELECT TRIM('xy' from 'xyxtrimyyx')`,
			`SELECT btrim('xyxtrimyyx', 'xy')`},
		{`SELECT TRIM(both 'xy' from 'xyxtrimyyx')`,
			`SELECT btrim('xyxtrimyyx', 'xy')`},
		{`SELECT TRIM(from 'xyxtrimyyx')`,
			`SELECT btrim('xyxtrimyyx')`},
		{`SELECT TRIM(both 'xyxtrimyyx')`,
			`SELECT btrim('xyxtrimyyx')`},
		{`SELECT TRIM(both from 'xyxtrimyyx')`,
			`SELECT btrim('xyxtrimyyx')`},
		{`SELECT TRIM(leading 'xy' from 'xyxtrimyyx')`,
			`SELECT ltrim('xyxtrimyyx', 'xy')`},
		{`SELECT TRIM(leading from 'xyxtrimyyx')`,
			`SELECT ltrim('xyxtrimyyx')`},
		{`SELECT TRIM(leading 'xyxtrimyyx')`,
			`SELECT ltrim('xyxtrimyyx')`},
		{`SELECT TRIM(trailing 'xy' from 'xyxtrimyyx')`,
			`SELECT rtrim('xyxtrimyyx', 'xy')`},
		{`SELECT TRIM(trailing from 'xyxtrimyyx')`,
			`SELECT rtrim('xyxtrimyyx')`},
		{`SELECT TRIM(trailing 'xyxtrimyyx')`,
			`SELECT rtrim('xyxtrimyyx')`},
		{`SELECT a IS NAN`, `SELECT isnan(a)`},
		{`SELECT a IS NOT NAN`, `SELECT NOT isnan(a)`},
		{`SHOW INDEX FROM t`,
			`SHOW INDEXES FROM t`},
		{`SHOW CONSTRAINT FROM t`,
			`SHOW CONSTRAINTS FROM t`},
		{`SHOW KEYS FROM t`,
			`SHOW INDEXES FROM t`},
		{`BEGIN`,
			`BEGIN TRANSACTION`},
		{`START TRANSACTION`,
			`BEGIN TRANSACTION`},
		{`COMMIT`,
			`COMMIT TRANSACTION`},
		{`END`,
			`COMMIT TRANSACTION`},
		{`BEGIN TRANSACTION PRIORITY LOW, ISOLATION LEVEL SNAPSHOT`,
			`BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT, PRIORITY LOW`},
		{`SET TRANSACTION PRIORITY NORMAL, ISOLATION LEVEL SERIALIZABLE`,
			`SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, PRIORITY NORMAL`},
		{"SET CLUSTER SETTING a TO 1", "SET CLUSTER SETTING a = 1"},
		{"RELEASE foo", "RELEASE SAVEPOINT foo"},
		{"RELEASE SAVEPOINT foo", "RELEASE SAVEPOINT foo"},
		{"ROLLBACK", "ROLLBACK TRANSACTION"},
		{"ROLLBACK TRANSACTION", "ROLLBACK TRANSACTION"},
		{"ROLLBACK TO foo", "ROLLBACK TRANSACTION TO SAVEPOINT foo"},
		{"ROLLBACK TO SAVEPOINT foo", "ROLLBACK TRANSACTION TO SAVEPOINT foo"},
		{"ROLLBACK TRANSACTION TO foo", "ROLLBACK TRANSACTION TO SAVEPOINT foo"},
		{"ROLLBACK TRANSACTION TO SAVEPOINT foo", "ROLLBACK TRANSACTION TO SAVEPOINT foo"},
		{`DEALLOCATE PREPARE a`,
			`DEALLOCATE a`},
		{`DEALLOCATE PREPARE ALL`,
			`DEALLOCATE ALL`},

		{`BACKUP DATABASE foo TO bar`,
			`BACKUP DATABASE foo TO 'bar'`},
		{`BACKUP DATABASE foo TO "bar.12" INCREMENTAL FROM "baz.34"`,
			`BACKUP DATABASE foo TO 'bar.12' INCREMENTAL FROM 'baz.34'`},
		{`RESTORE DATABASE foo FROM bar`,
			`RESTORE DATABASE foo FROM 'bar'`},

		{`SHOW ALL CLUSTER SETTINGS`, `SHOW CLUSTER SETTING all`},
	}
	for _, d := range testData {
		stmts, err := parse(d.sql)
		if err != nil {
			t.Errorf("%s: expected success, but found %s", d.sql, err)
			continue
		}
		s := stmts.String()
		if d.expected != s {
			t.Errorf("%s: expected %s, but found (%d statements): %s", d.sql, d.expected, len(stmts), s)
		}
		if _, err := parse(s); err != nil {
			t.Errorf("expected string found, but not parsable: %s:\n%s", err, s)
		}
	}
}

// TestParseSyntax verifies that parsing succeeds, though the syntax tree
// likely differs. All of the test cases here should eventually be moved
// elsewhere.
func TestParseSyntax(t *testing.T) {
	testData := []struct {
		sql string
	}{
		{`SELECT '\0' FROM a`},
		{`SELECT ((1)) FROM t WHERE ((a)) IN (((1))) AND ((a, b)) IN ((((1, 1))), ((2, 2)))`},
		{`SELECT e'\'\"\b\n\r\t\\' FROM t`},
		{`SELECT '\x' FROM t`},
	}
	for _, d := range testData {
		if _, err := parse(d.sql); err != nil {
			t.Fatalf("%s: expected success, but not parsable %s", d.sql, err)
		}
	}
}

func TestParseError(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
	}{
		{`SELECT2 1`, `syntax error at or near "SELECT2"
SELECT2 1
^
`},
		{`SELECT 1 FROM (t)`, `syntax error at or near ")"
SELECT 1 FROM (t)
                ^
`},
		{`SET a = 1, b = 2`, `syntax error at or near "="
SET a = 1, b = 2
             ^
`},
		{`SET a = 1,
b = 2`, `syntax error at or near "="
SET a = 1,
b = 2
  ^
`},
		{`SET TIME ZONE INTERVAL 'foobar'`, `could not parse 'foobar' as type interval: interval: missing unit at position 0: "foobar" at or near "EOF"
SET TIME ZONE INTERVAL 'foobar'
                               ^
`},
		{`SELECT INTERVAL 'foo'`, `could not parse 'foo' as type interval: interval: missing unit at position 0: "foo" at or near "EOF"
SELECT INTERVAL 'foo'
                     ^
`},
		{`SELECT 1 /* hello`, `unterminated comment
SELECT 1 /* hello
         ^
`},
		{`SELECT '1`, `unterminated string
SELECT '1
       ^
`},
		{`SELECT * FROM t WHERE k=`,
			`syntax error at or near "EOF"
SELECT * FROM t WHERE k=
                        ^
`,
		},
		{`CREATE TABLE test (
  CONSTRAINT foo INDEX (bar)
)`, `syntax error at or near "INDEX"
CREATE TABLE test (
  CONSTRAINT foo INDEX (bar)
                 ^
`},
		{`CREATE TABLE test (
  foo BIT(0)
)`, `length for type bit must be at least 1 at or near ")"
CREATE TABLE test (
  foo BIT(0)
           ^
`},
		{`CREATE TABLE test (
  foo INT DEFAULT 1 DEFAULT 2
)`, `multiple default values specified for column "foo" at or near ")"
CREATE TABLE test (
  foo INT DEFAULT 1 DEFAULT 2
)
^
`},
		{`CREATE TABLE test (
  foo INT REFERENCES t1 REFERENCES t2
)`, `multiple foreign key constraints specified for column "foo" at or near ")"
CREATE TABLE test (
  foo INT REFERENCES t1 REFERENCES t2
)
^
`},
		{`CREATE TABLE test (
  foo INT FAMILY a FAMILY b
)`, `multiple column families specified for column "foo" at or near ")"
CREATE TABLE test (
  foo INT FAMILY a FAMILY b
)
^
`},
		{`CREATE TABLE test (
  foo INT NOT NULL NULL
)`, `conflicting NULL/NOT NULL declarations for column "foo" at or near ")"
CREATE TABLE test (
  foo INT NOT NULL NULL
)
^
`},
		{`CREATE TABLE test (
  foo INT NULL NOT NULL
)`, `conflicting NULL/NOT NULL declarations for column "foo" at or near ")"
CREATE TABLE test (
  foo INT NULL NOT NULL
)
^
`},
		{`CREATE DATABASE a b`,
			`syntax error at or near "b"
CREATE DATABASE a b
                  ^
`},
		{`CREATE DATABASE a b c`,
			`syntax error at or near "b"
CREATE DATABASE a b c
                  ^
`},
		{`CREATE INDEX ON a (b) STORING ()`,
			`syntax error at or near ")"
CREATE INDEX ON a (b) STORING ()
                               ^
`},
		{`CREATE VIEW a`,
			`syntax error at or near "EOF"
CREATE VIEW a
             ^
`},
		{`CREATE VIEW a () AS select * FROM b`,
			`syntax error at or near ")"
CREATE VIEW a () AS select * FROM b
               ^
`},
		{`SELECT FROM t`,
			`syntax error at or near "FROM"
SELECT FROM t
       ^
`},

		{"SELECT 1e-\n-1",
			`invalid floating point literal
SELECT 1e-
       ^
`},
		{"SELECT foo''",
			`syntax error at or near ""
SELECT foo''
          ^
`},
		{
			`SELECT 0x FROM t`,
			`invalid hexadecimal numeric literal
SELECT 0x FROM t
       ^
`,
		},
		{
			`SELECT x'fail' FROM t`,
			`invalid hexadecimal bytes literal
SELECT x'fail' FROM t
       ^
`,
		},
		{
			`SELECT x'AAB' FROM t`,
			`invalid hexadecimal bytes literal
SELECT x'AAB' FROM t
       ^
`,
		},
		{
			`SELECT POSITION('high', 'a')`,
			`syntax error at or near ","
SELECT POSITION('high', 'a')
                      ^
`,
		},
		{
			`SELECT a FROM foo@{FORCE_INDEX}`,
			`syntax error at or near "}"
SELECT a FROM foo@{FORCE_INDEX}
                              ^
`,
		},
		{
			`SELECT a FROM foo@{FORCE_INDEX=}`,
			`syntax error at or near "}"
SELECT a FROM foo@{FORCE_INDEX=}
                               ^
`,
		},
		{
			`SELECT a FROM foo@{FORCE_INDEX=bar,FORCE_INDEX=baz}`,
			`FORCE_INDEX specified multiple times at or near "baz"
SELECT a FROM foo@{FORCE_INDEX=bar,FORCE_INDEX=baz}
                                               ^
`,
		},
		{
			`SELECT a FROM foo@{FORCE_INDEX=bar,NO_INDEX_JOIN,FORCE_INDEX=baz}`,
			`FORCE_INDEX specified multiple times at or near "baz"
SELECT a FROM foo@{FORCE_INDEX=bar,NO_INDEX_JOIN,FORCE_INDEX=baz}
                                                             ^
`,
		},
		{
			`SELECT a FROM foo@{NO_INDEX_JOIN,NO_INDEX_JOIN}`,
			`NO_INDEX_JOIN specified multiple times at or near "NO_INDEX_JOIN"
SELECT a FROM foo@{NO_INDEX_JOIN,NO_INDEX_JOIN}
                                 ^
`,
		},
		{
			`SELECT a FROM foo@{NO_INDEX_JOIN,FORCE_INDEX=baz,NO_INDEX_JOIN}`,
			`NO_INDEX_JOIN specified multiple times at or near "NO_INDEX_JOIN"
SELECT a FROM foo@{NO_INDEX_JOIN,FORCE_INDEX=baz,NO_INDEX_JOIN}
                                                 ^
`,
		},
		{
			`INSERT INTO a@b VALUES (1, 2)`,
			`syntax error at or near "@"
INSERT INTO a@b VALUES (1, 2)
             ^
`,
		},
		{
			`ALTER TABLE t RENAME COLUMN x TO family`,
			`syntax error at or near "family"
ALTER TABLE t RENAME COLUMN x TO family
                                 ^
`,
		},
		{
			`SELECT CAST(1.2+2.3 AS notatype)`,
			`syntax error at or near "notatype"
SELECT CAST(1.2+2.3 AS notatype)
                       ^
`,
		},
		{
			`SELECT ANNOTATE_TYPE(1.2+2.3, notatype)`,
			`syntax error at or near "notatype"
SELECT ANNOTATE_TYPE(1.2+2.3, notatype)
                              ^
`,
		},
		{
			`CREATE USER foo WITH PASSWORD`,
			`syntax error at or near "EOF"
CREATE USER foo WITH PASSWORD
                             ^
`,
		},
		{
			`ALTER TABLE t RENAME TO t[TRUE]`,
			`syntax error at or near "["
ALTER TABLE t RENAME TO t[TRUE]
                         ^
`,
		},
		{
			`SELECT (1 + 2).*`,
			`syntax error at or near "."
SELECT (1 + 2).*
              ^
`,
		},
		{
			`TABLE abc[TRUE]`,
			`syntax error at or near "["
TABLE abc[TRUE]
         ^
`,
		},
		{
			`UPDATE kv SET k[0] = 9`,
			`syntax error at or near "["
UPDATE kv SET k[0] = 9
               ^
`,
		},
		{
			`SELECT (ARRAY['a', 'b', 'c']).name`,
			`syntax error at or near "."
SELECT (ARRAY['a', 'b', 'c']).name
                             ^
`,
		},
		{
			`SELECT (0) FROM y[array[]]`,
			`syntax error at or near "["
SELECT (0) FROM y[array[]]
                 ^
`,
		},
		{
			`INSERT INTO kv (k[0]) VALUES ('hello')`,
			`syntax error at or near "["
INSERT INTO kv (k[0]) VALUES ('hello')
                 ^
`,
		},
		{
			`SELECT CASE 1 = 1 WHEN true THEN ARRAY[1, 2] ELSE ARRAY[2, 3] END[1]`,
			`syntax error at or near "["
SELECT CASE 1 = 1 WHEN true THEN ARRAY[1, 2] ELSE ARRAY[2, 3] END[1]
                                                                 ^
`,
		},
		{
			`SELECT EXISTS(SELECT 1)[1]`,
			`syntax error at or near "["
SELECT EXISTS(SELECT 1)[1]
                       ^
`,
		},
		{
			`SELECT 1 + ANY ARRAY[1, 2, 3]`,
			`+ ANY <array> is invalid because "+" is not a boolean operator at or near "]"
SELECT 1 + ANY ARRAY[1, 2, 3]
                            ^
`,
		},
	}
	for _, d := range testData {
		_, err := parse(d.sql)
		if err == nil || err.Error() != d.expected {
			t.Fatalf("%s: expected\n%s, but found\n%v", d.sql, d.expected, err)
		}
	}
}

func TestParsePanic(t *testing.T) {
	// Replicates #1801.
	defer func() {
		if r := recover(); r != nil {
			t.Fatal(r)
		}
	}()
	s := "SELECT(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(T" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F((" +
		"F(0"
	_, err := parse(s)
	expected := `syntax error at or near "EOF"`
	if !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}
}

func TestParsePrecedence(t *testing.T) {
	// Precedence levels (highest first):
	//   0: - ~
	//   1: * / // %
	//   2: + -
	//   3: << >>
	//   4: &
	//   5: ^
	//   6: |
	//   7: = != > >= < <=
	//   8: NOT
	//   9: AND
	//  10: OR

	unary := func(op UnaryOperator, expr Expr) Expr {
		return &UnaryExpr{Operator: op, Expr: expr}
	}
	binary := func(op BinaryOperator, left, right Expr) Expr {
		return &BinaryExpr{Operator: op, Left: left, Right: right}
	}
	cmp := func(op ComparisonOperator, left, right Expr) Expr {
		return &ComparisonExpr{Operator: op, Left: left, Right: right}
	}
	not := func(expr Expr) Expr {
		return &NotExpr{Expr: expr}
	}
	and := func(left, right Expr) Expr {
		return &AndExpr{Left: left, Right: right}
	}
	or := func(left, right Expr) Expr {
		return &OrExpr{Left: left, Right: right}
	}
	concat := func(left, right Expr) Expr {
		return &BinaryExpr{Operator: Concat, Left: left, Right: right}
	}
	regmatch := func(left, right Expr) Expr {
		return &ComparisonExpr{Operator: RegMatch, Left: left, Right: right}
	}
	regimatch := func(left, right Expr) Expr {
		return &ComparisonExpr{Operator: RegIMatch, Left: left, Right: right}
	}

	one := &NumVal{Value: constant.MakeInt64(1), OrigString: "1"}
	two := &NumVal{Value: constant.MakeInt64(2), OrigString: "2"}
	three := &NumVal{Value: constant.MakeInt64(3), OrigString: "3"}
	a := &StrVal{s: "a"}
	b := &StrVal{s: "b"}
	c := &StrVal{s: "c"}

	testData := []struct {
		sql      string
		expected Expr
	}{
		// Unary plus and complement.
		{`~-1`, unary(UnaryComplement, unary(UnaryMinus, one))},
		{`-~1`, unary(UnaryMinus, unary(UnaryComplement, one))},

		// Mul, div, floordiv, mod combined with higher precedence.
		{`-1*2`, binary(Mult, unary(UnaryMinus, one), two)},
		{`1*-2`, binary(Mult, one, unary(UnaryMinus, two))},
		{`-1/2`, binary(Div, unary(UnaryMinus, one), two)},
		{`1/-2`, binary(Div, one, unary(UnaryMinus, two))},
		{`-1//2`, binary(FloorDiv, unary(UnaryMinus, one), two)},
		{`1//-2`, binary(FloorDiv, one, unary(UnaryMinus, two))},
		{`-1%2`, binary(Mod, unary(UnaryMinus, one), two)},
		{`1%-2`, binary(Mod, one, unary(UnaryMinus, two))},

		// Mul, div, floordiv, mod combined with self (left associative).
		{`1*2*3`, binary(Mult, binary(Mult, one, two), three)},
		{`1*2/3`, binary(Div, binary(Mult, one, two), three)},
		{`1/2*3`, binary(Mult, binary(Div, one, two), three)},
		{`1*2//3`, binary(FloorDiv, binary(Mult, one, two), three)},
		{`1//2*3`, binary(Mult, binary(FloorDiv, one, two), three)},
		{`1*2%3`, binary(Mod, binary(Mult, one, two), three)},
		{`1%2*3`, binary(Mult, binary(Mod, one, two), three)},
		{`1/2/3`, binary(Div, binary(Div, one, two), three)},
		{`1/2//3`, binary(FloorDiv, binary(Div, one, two), three)},
		{`1//2/3`, binary(Div, binary(FloorDiv, one, two), three)},
		{`1/2%3`, binary(Mod, binary(Div, one, two), three)},
		{`1%2/3`, binary(Div, binary(Mod, one, two), three)},
		{`1//2//3`, binary(FloorDiv, binary(FloorDiv, one, two), three)},
		{`1//2%3`, binary(Mod, binary(FloorDiv, one, two), three)},
		{`1%2//3`, binary(FloorDiv, binary(Mod, one, two), three)},
		{`1%2%3`, binary(Mod, binary(Mod, one, two), three)},

		// Binary plus and minus combined with higher precedence.
		{`1*2+3`, binary(Plus, binary(Mult, one, two), three)},
		{`1+2*3`, binary(Plus, one, binary(Mult, two, three))},
		{`1*2-3`, binary(Minus, binary(Mult, one, two), three)},
		{`1-2*3`, binary(Minus, one, binary(Mult, two, three))},

		// Binary plus and minus combined with self (left associative).
		{`1+2-3`, binary(Minus, binary(Plus, one, two), three)},
		{`1-2+3`, binary(Plus, binary(Minus, one, two), three)},

		// Left and right shift combined with higher precedence.
		{`1<<2+3`, binary(LShift, one, binary(Plus, two, three))},
		{`1+2<<3`, binary(LShift, binary(Plus, one, two), three)},
		{`1>>2+3`, binary(RShift, one, binary(Plus, two, three))},
		{`1+2>>3`, binary(RShift, binary(Plus, one, two), three)},

		// Left and right shift combined with self (left associative).
		{`1<<2<<3`, binary(LShift, binary(LShift, one, two), three)},
		{`1<<2>>3`, binary(RShift, binary(LShift, one, two), three)},
		{`1>>2<<3`, binary(LShift, binary(RShift, one, two), three)},
		{`1>>2>>3`, binary(RShift, binary(RShift, one, two), three)},

		// Power combined with lower precedence.
		{`1*2^3`, binary(Mult, one, binary(Pow, two, three))},
		{`1^2*3`, binary(Mult, binary(Pow, one, two), three)},

		// Bit-and combined with higher precedence.
		{`1&2<<3`, binary(Bitand, one, binary(LShift, two, three))},
		{`1<<2&3`, binary(Bitand, binary(LShift, one, two), three)},

		// Bit-and combined with self (left associative)
		{`1&2&3`, binary(Bitand, binary(Bitand, one, two), three)},

		// Bit-xor combined with higher precedence.
		{`1#2&3`, binary(Bitxor, one, binary(Bitand, two, three))},
		{`1&2#3`, binary(Bitxor, binary(Bitand, one, two), three)},

		// Bit-xor combined with self (left associative)
		{`1#2#3`, binary(Bitxor, binary(Bitxor, one, two), three)},

		// Bit-or combined with higher precedence.
		{`1|2#3`, binary(Bitor, one, binary(Bitxor, two, three))},
		{`1#2|3`, binary(Bitor, binary(Bitxor, one, two), three)},

		// Bit-or combined with self (left associative)
		{`1|2|3`, binary(Bitor, binary(Bitor, one, two), three)},

		// Equals, not-equals, greater-than, greater-than equals, less-than and
		// less-than equals combined with higher precedence.
		{`1 = 2|3`, cmp(EQ, one, binary(Bitor, two, three))},
		{`1|2 = 3`, cmp(EQ, binary(Bitor, one, two), three)},
		{`1 != 2|3`, cmp(NE, one, binary(Bitor, two, three))},
		{`1|2 != 3`, cmp(NE, binary(Bitor, one, two), three)},
		{`1 > 2|3`, cmp(GT, one, binary(Bitor, two, three))},
		{`1|2 > 3`, cmp(GT, binary(Bitor, one, two), three)},
		{`1 >= 2|3`, cmp(GE, one, binary(Bitor, two, three))},
		{`1|2 >= 3`, cmp(GE, binary(Bitor, one, two), three)},
		{`1 < 2|3`, cmp(LT, one, binary(Bitor, two, three))},
		{`1|2 < 3`, cmp(LT, binary(Bitor, one, two), three)},
		{`1 <= 2|3`, cmp(LE, one, binary(Bitor, two, three))},
		{`1|2 <= 3`, cmp(LE, binary(Bitor, one, two), three)},

		// NOT combined with higher precedence.
		{`NOT 1 = 2`, not(cmp(EQ, one, two))},
		{`NOT 1 = NOT 2 = 3`, not(cmp(EQ, one, not(cmp(EQ, two, three))))},

		// NOT combined with self.
		{`NOT NOT 1 = 2`, not(not(cmp(EQ, one, two)))},

		// AND combined with higher precedence.
		{`NOT 1 AND 2`, and(not(one), two)},
		{`1 AND NOT 2`, and(one, not(two))},

		// AND combined with self (left associative).
		{`1 AND 2 AND 3`, and(and(one, two), three)},

		// OR combined with higher precedence.
		{`1 AND 2 OR 3`, or(and(one, two), three)},
		{`1 OR 2 AND 3`, or(one, and(two, three))},

		// OR combined with self (left associative).
		{`1 OR 2 OR 3`, or(or(one, two), three)},

		// ~ and ~* should both be lower than ||.
		{`'a' || 'b' ~ 'c'`, regmatch(concat(a, b), c)},
		{`'a' || 'b' ~* 'c'`, regimatch(concat(a, b), c)},

		// Unary ~ should have highest precedence.
		{`~1+2`, binary(Plus, unary(UnaryComplement, one), two)},
	}
	for _, d := range testData {
		expr, err := ParseExpr(d.sql)
		if err != nil {
			t.Fatalf("%s: %v", d.sql, err)
		}
		if !reflect.DeepEqual(d.expected, expr) {
			t.Fatalf("%s: expected %s, but found %s", d.sql, d.expected, expr)
		}
	}
}

func BenchmarkParse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		st, err := parse(`
			BEGIN;
			UPDATE pgbench_accounts SET abalance = abalance + 77 WHERE aid = 5;
			SELECT abalance FROM pgbench_accounts WHERE aid = 5;
			INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (1, 2, 5, 77, CURRENT_TIMESTAMP);
			END`)
		if err != nil {
			b.Fatal(err)
		}
		if len(st) != 5 {
			b.Fatal("parsed wrong number of statements: ", len(st))
		}
		if _, ok := st[1].(*Update); !ok {
			b.Fatalf("unexpected statement type: %T", st[1])
		}
	}
}

func TestEncodeSQLBytes(t *testing.T) {
	testEncodeSQL(t, encodeSQLBytes, false)
}

func TestEncodeSQLString(t *testing.T) {
	testEncodeSQL(t, encodeSQLString, true)
}

func testEncodeSQL(t *testing.T, encode func(*bytes.Buffer, string), forceUTF8 bool) {
	type entry struct{ i, j int }
	seen := make(map[string]entry)
	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			bytepair := []byte{byte(i), byte(j)}
			if forceUTF8 && !utf8.Valid(bytepair) {
				continue
			}
			stmt := testEncodeString(t, bytepair, encode)
			if e, ok := seen[stmt]; ok {
				t.Fatalf("duplicate entry: %s, from %v, currently at %v, %v", stmt, e, i, j)
			}
			seen[stmt] = entry{i, j}
		}
	}
}

func TestEncodeSQLStringSpecial(t *testing.T) {
	tests := [][]byte{
		// UTF8 replacement character
		{0xEF, 0xBF, 0xBD},
	}
	for _, tc := range tests {
		testEncodeString(t, tc, encodeSQLString)
	}
}

func testEncodeString(t *testing.T, input []byte, encode func(*bytes.Buffer, string)) string {
	s := string(input)
	var buf bytes.Buffer
	encode(&buf, s)
	sql := fmt.Sprintf("SELECT %s", buf.String())
	for n := 0; n < len(sql); n++ {
		ch := sql[n]
		if ch < 0x20 || ch >= 0x7F {
			t.Fatalf("unprintable character: %v (%v): %s %v", ch, input, sql, []byte(sql))
		}
	}
	stmts, err := parse(sql)
	if err != nil {
		t.Fatalf("%s: expected success, but found %s", sql, err)
	}
	stmt := stmts.String()
	if sql != stmt {
		t.Fatalf("expected %s, but found %s", sql, stmt)
	}
	return stmt
}
