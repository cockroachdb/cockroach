SELECT 1 FROM t
SELECT .1 FROM t
SELECT 1.2e1 FROM t
SELECT 1.2e+1 FROM t
SELECT 1.2e-1 FROM t
SELECT 08.3 FROM t
SELECT -1 FROM t WHERE b = -2
SELECT 1 FROM t // aa#SELECT 1 FROM t
SELECT 1 FROM t -- aa#SELECT 1 FROM t
SELECT /* simplest */ 1 FROM t
SELECT /* double star **/ 1 FROM t
SELECT /* double */ /* comment */ 1 FROM t
SELECT /* back-quote */ 1 FROM `t`#SELECT /* back-quote */ 1 FROM t
SELECT /* back-quote keyword */ 1 FROM `FROM`#SELECT /* back-quote keyword */ 1 FROM `FROM`
SELECT /* @ */ @@a FROM b
SELECT /* \0 */ '\0' FROM a
SELECT 1 /* DROP this comment */ FROM t#SELECT 1 FROM t
SELECT /* UNION */ 1 FROM t UNION SELECT 1 FROM t
SELECT /* double UNION */ 1 FROM t UNION SELECT 1 FROM t UNION SELECT 1 FROM t
SELECT /* UNION ALL */ 1 FROM t UNION ALL SELECT 1 FROM t
SELECT /* MINUS */ 1 FROM t MINUS SELECT 1 FROM t
SELECT /* EXCEPT */ 1 FROM t EXCEPT SELECT 1 FROM t
SELECT /* INTERSECT */ 1 FROM t INTERSECT SELECT 1 FROM t
SELECT /* DISTINCT */ DISTINCT 1 FROM t
SELECT /* FOR UPDATE */ 1 FROM t FOR UPDATE
SELECT /* LOCK IN SHARE MODE */ 1 FROM t LOCK IN SHARE MODE
SELECT /* SELECT list */ 1, 2 FROM t
SELECT /* * */ * FROM t
SELECT /* column alias */ a b FROM t#SELECT /* column alias */ a AS b FROM t
SELECT /* column alias with AS */ a AS b FROM t
SELECT /* a.* */ a.* FROM t
SELECT /* SELECT with bool expr */ a = b FROM t
SELECT /* CASE_WHEN */ CASE WHEN a = b THEN c END FROM t
SELECT /* CASE_WHEN_ELSE */ CASE WHEN a = b THEN c ELSE d END FROM t
SELECT /* CASE_WHEN_WHEN_ELSE */ CASE WHEN a = b THEN c WHEN b = d THEN d ELSE d END FROM t
SELECT /* CASE */ CASE aa WHEN a = b THEN c END FROM t
SELECT /* parenthesis */ 1 FROM (t)
SELECT /* TABLE list */ 1 FROM t1, t2
SELECT /* parenthessis IN TABLE list 1 */ 1 FROM (t1), t2
SELECT /* parenthessis IN TABLE list 2 */ 1 FROM t1, (t2)
SELECT /* USE */ 1 FROM t1 USE INDEX (a) WHERE b = 1
SELECT /* IGNORE */ 1 FROM t1 AS t2 IGNORE INDEX (a), t3 USE INDEX (b) WHERE b = 1
SELECT /* USE */ 1 FROM t1 AS t2 USE INDEX (a), t3 USE INDEX (b) WHERE b = 1
SELECT /* FORCE */ 1 FROM t1 AS t2 FORCE INDEX (a), t3 FORCE INDEX (b) WHERE b = 1
SELECT /* TABLE alias */ 1 FROM t t1#SELECT /* TABLE alias */ 1 FROM t AS t1
SELECT /* TABLE alias with AS */ 1 FROM t AS t1
SELECT /* JOIN */ 1 FROM t1 JOIN t2
SELECT /* STRAIGHT_JOIN */ 1 FROM t1 STRAIGHT_JOIN t2
SELECT /* LEFT JOIN */ 1 FROM t1 LEFT JOIN t2
SELECT /* LEFT OUTER JOIN */ 1 FROM t1 LEFT OUTER JOIN t2#SELECT /* LEFT OUTER JOIN */ 1 FROM t1 LEFT JOIN t2
SELECT /* RIGHT JOIN */ 1 FROM t1 RIGHT JOIN t2
SELECT /* RIGHT OUTER JOIN */ 1 FROM t1 RIGHT OUTER JOIN t2#SELECT /* RIGHT OUTER JOIN */ 1 FROM t1 RIGHT JOIN t2
SELECT /* INNER JOIN */ 1 FROM t1 INNER JOIN t2#SELECT /* INNER JOIN */ 1 FROM t1 JOIN t2
SELECT /* CROSS JOIN */ 1 FROM t1 CROSS JOIN t2
SELECT /* NATURAL JOIN */ 1 FROM t1 NATURAL JOIN t2
SELECT /* JOIN ON */ 1 FROM t1 JOIN t2 ON a = b
SELECT /* JOIN USING */ 1 FROM t1 JOIN t2 USING (a)
SELECT /* s.t */ 1 FROM s.t
SELECT /* SELECT IN FROM */ 1 FROM (SELECT 1 FROM t)
SELECT /* WHERE */ 1 FROM t WHERE a = b
SELECT /* AND */ 1 FROM t WHERE a = b AND a = c
SELECT /* && */ 1 FROM t WHERE a = b && a = c
SELECT /* OR */ 1 FROM t WHERE a = b OR a = c
SELECT /* || */ 1 FROM t WHERE a = b || a = c
SELECT /* NOT */ 1 FROM t WHERE NOT a = b
SELECT /* ! */ 1 FROM t WHERE ! a = b
SELECT /* EXISTS */ 1 FROM t WHERE EXISTS (SELECT 1 FROM t)
SELECT /* (boolean) */ 1 FROM t WHERE NOT (a = b)
SELECT /* IN value list */ 1 FROM t WHERE a IN (b, c)
SELECT /* IN SELECT */ 1 FROM t WHERE a IN (SELECT 1 FROM t)
SELECT /* NOT IN */ 1 FROM t WHERE a NOT IN (b, c)
SELECT /* LIKE */ 1 FROM t WHERE a LIKE b
SELECT /* NOT LIKE */ 1 FROM t WHERE a NOT LIKE b
SELECT /* BETWEEN */ 1 FROM t WHERE a BETWEEN b AND c
SELECT /* NOT BETWEEN */ 1 FROM t WHERE a NOT BETWEEN b AND c
SELECT /* IS NULL */ 1 FROM t WHERE a IS NULL
SELECT /* IS NOT NULL */ 1 FROM t WHERE a IS NOT NULL
SELECT /* < */ 1 FROM t WHERE a < b
SELECT /* <= */ 1 FROM t WHERE a <= b
SELECT /* >= */ 1 FROM t WHERE a >= b
SELECT /* <> */ 1 FROM t WHERE a != b
SELECT /* <=> */ 1 FROM t WHERE a <=> b
SELECT /* != */ 1 FROM t WHERE a != b
SELECT /* single value expre list */ 1 FROM t WHERE a IN (b)
SELECT /* SELECT as a value expression */ 1 FROM t WHERE a = (SELECT a FROM t)
SELECT /* parenthesised value */ 1 FROM t WHERE a = (b)
SELECT /* over-parenthesize */ ((1)) FROM t WHERE ((a)) IN (((1))) AND ((a, b)) IN ((((1, 1))), ((2, 2)))
SELECT /* dot-parenthesize */ (a.b) FROM t WHERE (b.c) = 2
SELECT /* & */ 1 FROM t WHERE a = b&c
SELECT /* | */ 1 FROM t WHERE a = b|c
SELECT /* ^ */ 1 FROM t WHERE a = b^c
SELECT /* + */ 1 FROM t WHERE a = b+c
SELECT /* - */ 1 FROM t WHERE a = b-c
SELECT /* * */ 1 FROM t WHERE a = b*c
SELECT /* / */ 1 FROM t WHERE a = b/c
SELECT /* % */ 1 FROM t WHERE a = b%c
SELECT /* u+ */ 1 FROM t WHERE a = +b
SELECT /* u- */ 1 FROM t WHERE a = -b
SELECT /* u~ */ 1 FROM t WHERE a = ~b
SELECT /* empty function */ 1 FROM t WHERE a = B()
SELECT /* function with 1 param */ 1 FROM t WHERE a = B(c)
SELECT /* function with many params */ 1 FROM t WHERE a = B(c, d)
SELECT /* if as func */ 1 FROM t WHERE a = IF(b)
SELECT /* function with DISTINCT */ COUNT(DISTINCT a) FROM t
SELECT /* a */ a FROM t
SELECT /* a.b */ a.b FROM t
SELECT /* string */ 'a' FROM t
SELECT /* double quoted string */ "a" FROM t#SELECT /* double quoted string */ 'a' FROM t
SELECT /* quote quote IN string */ 'a''a' FROM t#SELECT /* quote quote IN string */ 'a\'a' FROM t
SELECT /* double quote quote in string */ "a""a" FROM t#SELECT /* double quote quote in string */ 'a\"a' FROM t
SELECT /* quote in double quoted string */ "a'a" FROM t#SELECT /* quote in double quoted string */ 'a\'a' FROM t
SELECT /* backslash quote in string */ 'a\'a' FROM t
SELECT /* literal backslash in string */ 'a\\na' FROM t
SELECT /* all escapes */ '\0\'\"\b\n\r\t\Z\\' FROM t
SELECT /* non-escape */ '\x' FROM t#SELECT /* non-escape */ 'x' FROM t
SELECT /* unescaped backslash */ '\n' FROM t
SELECT /* value argument */ :a FROM t
SELECT /* value argument with dot */ :a.b FROM t
SELECT /* positional argument */ ? FROM t#SELECT /* positional argument */ :v1 FROM t
SELECT /* multiple positional arguments */ ?, ? FROM t#SELECT /* multiple positional arguments */ :v1, :v2 FROM t
SELECT /* NULL */ NULL FROM t
SELECT /* octal */ 010 FROM t
SELECT /* hex */ 0xf0 FROM t
SELECT /* hex caps */ 0xF0 FROM t
SELECT /* float */ 0.1 FROM t
SELECT /* GROUP BY */ 1 FROM t GROUP BY a
SELECT /* HAVING */ 1 FROM t HAVING a = b
SELECT /* simple ORDER BY */ 1 FROM t ORDER BY a#SELECT /* simple ORDER BY */ 1 FROM t ORDER BY a ASC
SELECT /* ORDER BY ASC */ 1 FROM t ORDER BY a ASC
SELECT /* ORDER BY DESC */ 1 FROM t ORDER BY a DESC
SELECT /* LIMIT a */ 1 FROM t LIMIT a
SELECT /* LIMIT a,b */ 1 FROM t LIMIT a, b
SELECT /* LIMIT a OFFSET b */ 1 FROM t LIMIT a OFFSET b#SELECT /* LIMIT a OFFSET b */ 1 FROM t LIMIT b, a
INSERT /* simple */ INTO a VALUES (1)
INSERT /* a.b */ INTO a.b VALUES (1)
INSERT /* multi-value */ INTO a VALUES (1, 2)
INSERT /* multi-value list */ INTO a VALUES (1, 2), (3, 4)
INSERT /* SET */ INTO a SET a = 1, a.b = 2#INSERT /* SET */ INTO a(a, a.b) VALUES (1, 2)
INSERT /* value expression list */ INTO a VALUES (a+1, 2*3)
INSERT /* column list */ INTO a(a, b) VALUES (1, 2)
INSERT /* qualified column list */ INTO a(a, a.b) VALUES (1, 2)
INSERT /* SELECT */ INTO a SELECT b, c FROM d
INSERT /* ON DUPLICATE */ INTO a VALUES (1, 2) ON DUPLICATE KEY UPDATE b = VALUES(a), c = d
UPDATE /* simple */ a SET b = 3
UPDATE /* a.b */ a.b SET b = 3
UPDATE /* b.c */ a SET b.c = 3
UPDATE /* list */ a SET b = 3, c = 4
UPDATE /* expression */ a SET b = 3+4
UPDATE /* WHERE */ a SET b = 3 WHERE a = b
UPDATE /* ORDER */ a SET b = 3 ORDER BY c DESC
UPDATE /* LIMIT */ a SET b = 3 LIMIT c
DELETE /* simple */ FROM a
DELETE /* a.b */ FROM a.b
DELETE /* WHERE */ FROM a WHERE a = b
DELETE /* ORDER */ FROM a ORDER BY b DESC
DELETE /* LIMIT */ FROM a LIMIT b
SET /* simple */ a = 3
SET /* list */ a = 3, b = 4
USE /* list */ a
ALTER IGNORE TABLE a ADD foo#ALTER TABLE a
ALTER TABLE a ADD foo#ALTER TABLE a
ALTER TABLE a ALTER foo#ALTER TABLE a
ALTER TABLE a CHANGE foo#ALTER TABLE a
ALTER TABLE a MODIFY foo#ALTER TABLE a
ALTER TABLE a DROP foo#ALTER TABLE a
ALTER TABLE a DISABLE foo#ALTER TABLE a
ALTER TABLE a ENABLE foo#ALTER TABLE a
ALTER TABLE a ORDER foo#ALTER TABLE a
ALTER TABLE a DEFAULT foo#ALTER TABLE a
ALTER TABLE a DISCARD foo#ALTER TABLE a
ALTER TABLE a IMPORT foo#ALTER TABLE a
ALTER TABLE a RENAME b#RENAME TABLE a b
ALTER TABLE a RENAME to b#RENAME TABLE a b
CREATE DATABASE a
CREATE DATABASE IF NOT EXISTS a
CREATE TABLE a (b INT)
CREATE TABLE IF NOT EXISTS a (b INT)
CREATE INDEX a ON b
CREATE UNIQUE INDEX a ON b
CREATE UNIQUE INDEX a using foo ON b#CREATE UNIQUE INDEX a ON b
CREATE VIEW a#CREATE VIEW a
ALTER VIEW a#ALTER VIEW a
DROP DATABASE a
DROP DATABASE IF EXISTS a
DROP VIEW a
DROP VIEW IF EXISTS a
DROP TABLE a
DROP TABLE IF EXISTS a
DROP INDEX b ON a
TRUNCATE TABLE a
SHOW DATABASES
SHOW TABLES
SHOW TABLES FROM a
SHOW COLUMNS FROM a
SHOW FULL COLUMNS FROM a
SHOW INDEX FROM a
CREATE TABLE a (b INT, c TEXT, INDEX d (b, c))
CREATE TABLE a (b INT, UNIQUE INDEX c (b))
CREATE TABLE a (b INT UNIQUE)
CREATE TABLE a (b INT UNIQUE KEY)#CREATE TABLE a (b INT UNIQUE)
CREATE TABLE a (b INT KEY)#CREATE TABLE a (b INT PRIMARY KEY)
CREATE TABLE a (b INT PRIMARY KEY)
CREATE TABLE a (b INT NULL)
CREATE TABLE a (b INT NOT NULL)
CREATE TABLE a (b INT NULL PRIMARY KEY)
