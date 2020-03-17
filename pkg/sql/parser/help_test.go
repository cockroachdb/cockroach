// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parser

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

func TestHelpMessagesDefined(t *testing.T) {
	var emptyBody HelpMessageBody
	// Note: expectedHelpStrings is generated externally
	// from the grammar by help_gen_test.sh.
	for _, expKey := range expectedHelpStrings {
		expectedMsg := HelpMessages[expKey]
		if expectedMsg == emptyBody {
			t.Errorf("no message defined for %q", expKey)
		}
	}
}

func TestContextualHelp(t *testing.T) {
	testData := []struct {
		input string
		key   string
	}{
		{`ALTER ??`, `ALTER`},

		{`ALTER TABLE IF ??`, `ALTER TABLE`},
		{`ALTER TABLE blah ??`, `ALTER TABLE`},
		{`ALTER TABLE blah ADD ??`, `ALTER TABLE`},
		{`ALTER TABLE blah ALTER x DROP ??`, `ALTER TABLE`},
		{`ALTER TABLE blah RENAME TO ??`, `ALTER TABLE`},
		{`ALTER TABLE blah RENAME TO blih ??`, `ALTER TABLE`},
		{`ALTER TABLE blah SPLIT AT (SELECT 1) ??`, `ALTER TABLE`},

		{`ALTER INDEX foo@bar RENAME ??`, `ALTER INDEX`},
		{`ALTER INDEX foo@bar RENAME TO blih ??`, `ALTER INDEX`},
		{`ALTER INDEX foo@bar SPLIT ??`, `ALTER INDEX`},
		{`ALTER INDEX foo@bar SPLIT AT (SELECT 1) ??`, `ALTER INDEX`},

		{`ALTER DATABASE foo ??`, `ALTER DATABASE`},
		{`ALTER DATABASE foo RENAME ??`, `ALTER DATABASE`},
		{`ALTER DATABASE foo RENAME TO bar ??`, `ALTER DATABASE`},

		{`ALTER VIEW IF ??`, `ALTER VIEW`},
		{`ALTER VIEW blah ??`, `ALTER VIEW`},
		{`ALTER VIEW blah RENAME ??`, `ALTER VIEW`},
		{`ALTER VIEW blah RENAME TO blih ??`, `ALTER VIEW`},

		{`ALTER SEQUENCE IF ??`, `ALTER SEQUENCE`},
		{`ALTER SEQUENCE blah ??`, `ALTER SEQUENCE`},
		{`ALTER SEQUENCE blah RENAME ??`, `ALTER SEQUENCE`},
		{`ALTER SEQUENCE blah RENAME TO blih ??`, `ALTER SEQUENCE`},

		{`ALTER USER IF ??`, `ALTER ROLE`},
		{`ALTER USER foo WITH PASSWORD ??`, `ALTER ROLE`},

		{`ALTER ROLE bleh ?? WITH NOCREATEROLE`, `ALTER ROLE`},

		{`ALTER RANGE foo CONFIGURE ??`, `ALTER RANGE`},
		{`ALTER RANGE ??`, `ALTER RANGE`},

		{`ALTER PARTITION ??`, `ALTER PARTITION`},
		{`ALTER PARTITION p OF INDEX tbl@idx ??`, `ALTER PARTITION`},

		{`CANCEL ??`, `CANCEL`},
		{`CANCEL JOB ??`, `CANCEL JOBS`},
		{`CANCEL JOBS ??`, `CANCEL JOBS`},
		{`CANCEL QUERY ??`, `CANCEL QUERIES`},
		{`CANCEL QUERY IF ??`, `CANCEL QUERIES`},
		{`CANCEL QUERY IF EXISTS ??`, `CANCEL QUERIES`},
		{`CANCEL QUERIES ??`, `CANCEL QUERIES`},
		{`CANCEL QUERIES IF ??`, `CANCEL QUERIES`},
		{`CANCEL QUERIES IF EXISTS ??`, `CANCEL QUERIES`},
		{`CANCEL SESSION ??`, `CANCEL SESSIONS`},
		{`CANCEL SESSION IF ??`, `CANCEL SESSIONS`},
		{`CANCEL SESSION IF EXISTS ??`, `CANCEL SESSIONS`},
		{`CANCEL SESSIONS ??`, `CANCEL SESSIONS`},
		{`CANCEL SESSIONS IF ??`, `CANCEL SESSIONS`},
		{`CANCEL SESSIONS IF EXISTS ??`, `CANCEL SESSIONS`},

		{`CREATE UNIQUE ??`, `CREATE`},
		{`CREATE UNIQUE INDEX ??`, `CREATE INDEX`},
		{`CREATE INDEX IF NOT ??`, `CREATE INDEX`},
		{`CREATE INDEX blah ??`, `CREATE INDEX`},
		{`CREATE INDEX blah ON bloh (??`, `CREATE INDEX`},
		{`CREATE INDEX blah ON bloh (x,y) STORING ??`, `CREATE INDEX`},
		{`CREATE INDEX blah ON bloh (x) ??`, `CREATE INDEX`},

		{`CREATE DATABASE IF ??`, `CREATE DATABASE`},
		{`CREATE DATABASE IF NOT ??`, `CREATE DATABASE`},
		{`CREATE DATABASE blih ??`, `CREATE DATABASE`},

		{`CREATE USER blih ??`, `CREATE ROLE`},
		{`CREATE USER blih WITH ??`, `CREATE ROLE`},

		{`CREATE ROLE bleh ??`, `CREATE ROLE`},
		{`CREATE ROLE bleh ?? WITH CREATEROLE`, `CREATE ROLE`},

		{`CREATE VIEW blah (??`, `CREATE VIEW`},
		{`CREATE VIEW blah AS (SELECT c FROM x) ??`, `CREATE VIEW`},
		{`CREATE VIEW blah AS SELECT c FROM x ??`, `SELECT`},
		{`CREATE VIEW blah AS (??`, `<SELECTCLAUSE>`},

		{`CREATE SEQUENCE ??`, `CREATE SEQUENCE`},

		{`CREATE STATISTICS ??`, `CREATE STATISTICS`},

		{`CREATE TABLE blah (??`, `CREATE TABLE`},
		{`CREATE TABLE IF NOT ??`, `CREATE TABLE`},
		{`CREATE TABLE blah (x, y) AS ??`, `CREATE TABLE`},
		{`CREATE TABLE blah (x INT) ??`, `CREATE TABLE`},
		{`CREATE TABLE blah AS ??`, `CREATE TABLE`},
		{`CREATE TABLE blah AS (SELECT 1) ??`, `CREATE TABLE`},
		{`CREATE TABLE blah AS SELECT 1 ??`, `SELECT`},

		{`CREATE SCHEMA IF ??`, `CREATE SCHEMA`},
		{`CREATE SCHEMA IF NOT ??`, `CREATE SCHEMA`},
		{`CREATE SCHEMA bli ??`, `CREATE SCHEMA`},

		{`DELETE FROM ??`, `DELETE`},
		{`DELETE FROM blah ??`, `DELETE`},
		{`DELETE FROM blah WHERE ??`, `DELETE`},
		{`DELETE FROM blah WHERE x > 3 ??`, `DELETE`},

		{`DISCARD ALL ??`, `DISCARD`},
		{`DISCARD ??`, `DISCARD`},

		{`DROP ??`, `DROP`},

		{`DROP DATABASE IF ??`, `DROP DATABASE`},
		{`DROP DATABASE IF EXISTS blah ??`, `DROP DATABASE`},

		{`DROP INDEX blah, ??`, `DROP INDEX`},
		{`DROP INDEX blah@blih ??`, `DROP INDEX`},

		{`DROP USER ??`, `DROP ROLE`},
		{`DROP USER IF ??`, `DROP ROLE`},
		{`DROP USER IF EXISTS bluh ??`, `DROP ROLE`},

		{`DROP ROLE ??`, `DROP ROLE`},
		{`DROP ROLE IF ??`, `DROP ROLE`},
		{`DROP ROLE IF EXISTS bluh ??`, `DROP ROLE`},

		{`DROP SEQUENCE blah ??`, `DROP SEQUENCE`},
		{`DROP SEQUENCE IF ??`, `DROP SEQUENCE`},
		{`DROP SEQUENCE IF EXISTS blih, bloh ??`, `DROP SEQUENCE`},

		{`DROP TABLE blah ??`, `DROP TABLE`},
		{`DROP TABLE IF ??`, `DROP TABLE`},
		{`DROP TABLE IF EXISTS blih, bloh ??`, `DROP TABLE`},

		{`DROP VIEW blah ??`, `DROP VIEW`},
		{`DROP VIEW IF ??`, `DROP VIEW`},
		{`DROP VIEW IF EXISTS blih, bloh ??`, `DROP VIEW`},

		{`EXPLAIN (??`, `EXPLAIN`},
		{`EXPLAIN SELECT 1 ??`, `SELECT`},
		{`EXPLAIN INSERT INTO xx (SELECT 1) ??`, `INSERT`},
		{`EXPLAIN UPSERT INTO xx (SELECT 1) ??`, `UPSERT`},
		{`EXPLAIN DELETE FROM xx ??`, `DELETE`},
		{`EXPLAIN UPDATE xx SET x = y ??`, `UPDATE`},
		{`SELECT * FROM [EXPLAIN ??`, `EXPLAIN`},

		{`PREPARE foo ??`, `PREPARE`},
		{`PREPARE foo (??`, `PREPARE`},
		{`PREPARE foo AS SELECT 1 ??`, `SELECT`},
		{`PREPARE foo AS (SELECT 1) ??`, `PREPARE`},
		{`PREPARE foo AS INSERT INTO xx (SELECT 1) ??`, `INSERT`},
		{`PREPARE foo AS UPSERT INTO xx (SELECT 1) ??`, `UPSERT`},
		{`PREPARE foo AS DELETE FROM xx ??`, `DELETE`},
		{`PREPARE foo AS UPDATE xx SET x = y ??`, `UPDATE`},

		{`EXECUTE foo ??`, `EXECUTE`},
		{`EXECUTE foo (??`, `EXECUTE`},

		{`DEALLOCATE foo ??`, `DEALLOCATE`},
		{`DEALLOCATE ALL ??`, `DEALLOCATE`},
		{`DEALLOCATE PREPARE ??`, `DEALLOCATE`},

		{`INSERT INTO ??`, `INSERT`},
		{`INSERT INTO blah (??`, `<SELECTCLAUSE>`},
		{`INSERT INTO blah VALUES (1) RETURNING ??`, `INSERT`},
		{`INSERT INTO blah (VALUES (1)) ??`, `INSERT`},
		{`INSERT INTO blah VALUES (1) ??`, `VALUES`},
		{`INSERT INTO blah TABLE foo ??`, `TABLE`},

		{`UPSERT INTO ??`, `UPSERT`},
		{`UPSERT INTO blah (??`, `<SELECTCLAUSE>`},
		{`UPSERT INTO blah VALUES (1) RETURNING ??`, `UPSERT`},
		{`UPSERT INTO blah (VALUES (1)) ??`, `UPSERT`},
		{`UPSERT INTO blah VALUES (1) ??`, `VALUES`},
		{`UPSERT INTO blah TABLE foo ??`, `TABLE`},

		{`UPDATE blah ??`, `UPDATE`},
		{`UPDATE blah SET ??`, `UPDATE`},
		{`UPDATE blah SET x = 3 WHERE true ??`, `UPDATE`},
		{`UPDATE blah SET x = 3 ??`, `UPDATE`},
		{`UPDATE blah SET x = 3 WHERE ??`, `UPDATE`},

		{`GRANT ALL ??`, `GRANT`},
		{`GRANT ALL ON foo TO ??`, `GRANT`},
		{`GRANT ALL ON foo TO bar ??`, `GRANT`},

		{`PAUSE ??`, `PAUSE JOBS`},

		{`RESUME ??`, `RESUME JOBS`},

		{`REVOKE ALL ??`, `REVOKE`},
		{`REVOKE ALL ON foo FROM ??`, `REVOKE`},
		{`REVOKE ALL ON foo FROM bar ??`, `REVOKE`},

		{`SELECT * FROM ??`, `<SOURCE>`},
		{`SELECT * FROM (??`, `<SOURCE>`}, // not <selectclause>! joins are allowed.
		{`SELECT * FROM [SHOW ??`, `SHOW`},

		{`SHOW blah ??`, `SHOW SESSION`},
		{`SHOW database ??`, `SHOW SESSION`},
		{`SHOW TIME ??`, `SHOW SESSION`},
		{`SHOW all ??`, `SHOW SESSION`},
		{`SHOW SESSION_USER ??`, `SHOW SESSION`},
		{`SHOW SESSION blah ??`, `SHOW SESSION`},
		{`SHOW SESSION database ??`, `SHOW SESSION`},
		{`SHOW SESSION TIME ZONE ??`, `SHOW SESSION`},
		{`SHOW SESSION all ??`, `SHOW SESSION`},
		{`SHOW SESSION SESSION_USER ??`, `SHOW SESSION`},

		{`SHOW SESSIONS ??`, `SHOW SESSIONS`},
		{`SHOW LOCAL SESSIONS ??`, `SHOW SESSIONS`},

		{`SHOW STATISTICS ??`, `SHOW STATISTICS`},
		{`SHOW STATISTICS FOR TABLE ??`, `SHOW STATISTICS`},

		{`SHOW HISTOGRAM ??`, `SHOW HISTOGRAM`},

		{`SHOW QUERIES ??`, `SHOW QUERIES`},
		{`SHOW LOCAL QUERIES ??`, `SHOW QUERIES`},

		{`SHOW TRACE ??`, `SHOW TRACE`},
		{`SHOW TRACE FOR SESSION ??`, `SHOW TRACE`},
		{`SHOW TRACE FOR ??`, `SHOW TRACE`},

		{`SHOW JOB ??`, `SHOW JOBS`},
		{`SHOW JOBS ??`, `SHOW JOBS`},
		{`SHOW AUTOMATIC JOBS ??`, `SHOW JOBS`},

		{`SHOW BACKUP 'foo' ??`, `SHOW BACKUP`},

		{`SHOW CLUSTER SETTING all ??`, `SHOW CLUSTER SETTING`},
		{`SHOW ALL CLUSTER ??`, `SHOW CLUSTER SETTING`},

		{`SHOW COLUMNS FROM ??`, `SHOW COLUMNS`},
		{`SHOW COLUMNS FROM foo ??`, `SHOW COLUMNS`},

		{`SHOW CONSTRAINTS FROM ??`, `SHOW CONSTRAINTS`},
		{`SHOW CONSTRAINTS FROM foo ??`, `SHOW CONSTRAINTS`},

		{`SHOW CREATE ??`, `SHOW CREATE`},
		{`SHOW CREATE TABLE blah ??`, `SHOW CREATE`},
		{`SHOW CREATE VIEW blah ??`, `SHOW CREATE`},
		{`SHOW CREATE SEQUENCE blah ??`, `SHOW CREATE`},

		{`SHOW DATABASES ??`, `SHOW DATABASES`},

		{`SHOW GRANTS ON ??`, `SHOW GRANTS`},
		{`SHOW GRANTS ON foo FOR ??`, `SHOW GRANTS`},
		{`SHOW GRANTS ON foo FOR bar ??`, `SHOW GRANTS`},

		{`SHOW GRANTS ON ROLE ??`, `SHOW GRANTS`},
		{`SHOW GRANTS ON ROLE foo FOR ??`, `SHOW GRANTS`},
		{`SHOW GRANTS ON ROLE foo FOR bar ??`, `SHOW GRANTS`},

		{`SHOW KEYS ??`, `SHOW INDEXES`},
		{`SHOW INDEX ??`, `SHOW INDEXES`},
		{`SHOW INDEXES FROM ??`, `SHOW INDEXES`},
		{`SHOW INDEXES FROM blah ??`, `SHOW INDEXES`},

		{`SHOW PARTITIONS FROM ??`, `SHOW PARTITIONS`},

		{`SHOW ROLES ??`, `SHOW ROLES`},

		{`SHOW SCHEMAS FROM ??`, `SHOW SCHEMAS`},
		{`SHOW SCHEMAS FROM blah ??`, `SHOW SCHEMAS`},

		{`SHOW SEQUENCES FROM ??`, `SHOW SEQUENCES`},
		{`SHOW SEQUENCES FROM blah ??`, `SHOW SEQUENCES`},

		{`SHOW TABLES FROM ??`, `SHOW TABLES`},
		{`SHOW TABLES FROM blah ??`, `SHOW TABLES`},

		{`SHOW TRANSACTION PRIORITY ??`, `SHOW TRANSACTION`},
		{`SHOW TRANSACTION STATUS ??`, `SHOW TRANSACTION`},
		{`SHOW TRANSACTION ISOLATION ??`, `SHOW TRANSACTION`},
		{`SHOW TRANSACTION ISOLATION LEVEL ??`, `SHOW TRANSACTION`},
		{`SHOW SYNTAX ??`, `SHOW SYNTAX`},
		{`SHOW SYNTAX 'foo' ??`, `SHOW SYNTAX`},
		{`SHOW SAVEPOINT STATUS ??`, `SHOW SAVEPOINT`},

		{`SHOW RANGE ??`, `SHOW RANGE`},

		{`SHOW RANGES ??`, `SHOW RANGES`},

		{`SHOW USERS ??`, `SHOW USERS`},

		{`TRUNCATE foo ??`, `TRUNCATE`},
		{`TRUNCATE foo, ??`, `TRUNCATE`},

		{`SELECT 1 ??`, `SELECT`},
		{`SELECT * FROM ??`, `<SOURCE>`},
		{`SELECT 1 FROM foo ??`, `SELECT`},
		{`SELECT 1 FROM foo WHERE ??`, `SELECT`},
		{`SELECT 1 FROM (SELECT ??`, `SELECT`},
		{`SELECT 1 FROM (VALUES ??`, `VALUES`},
		{`SELECT 1 FROM (TABLE ??`, `TABLE`},
		{`SELECT 1 FROM (SELECT 2 ??`, `SELECT`},
		{`SELECT 1 FROM (??`, `<SOURCE>`},

		{`TABLE blah ??`, `TABLE`},

		{`VALUES (??`, `VALUES`},

		{`VALUES (1) ??`, `VALUES`},

		{`SET SESSION TRANSACTION ??`, `SET TRANSACTION`},
		{`SET SESSION TRANSACTION ISOLATION LEVEL SNAPSHOT ??`, `SET TRANSACTION`},
		{`SET SESSION TIME ??`, `SET SESSION`},
		{`SET SESSION TIME ZONE 'UTC' ??`, `SET SESSION`},
		{`SET SESSION blah TO ??`, `SET SESSION`},
		{`SET SESSION blah TO 42 ??`, `SET SESSION`},

		{`SET TRANSACTION ??`, `SET TRANSACTION`},
		{`SET TRANSACTION ISOLATION LEVEL SNAPSHOT ??`, `SET TRANSACTION`},
		{`SET TIME ??`, `SET SESSION`},
		{`SET TIME ZONE 'UTC' ??`, `SET SESSION`},
		{`SET blah TO ??`, `SET SESSION`},
		{`SET blah TO 42 ??`, `SET SESSION`},

		{`SET CLUSTER ??`, `SET CLUSTER SETTING`},
		{`SET CLUSTER SETTING blah = 42 ??`, `SET CLUSTER SETTING`},

		{`USE ??`, `USE`},

		{`RESET blah ??`, `RESET`},
		{`RESET SESSION ??`, `RESET`},
		{`RESET CLUSTER SETTING ??`, `RESET CLUSTER SETTING`},

		{`BEGIN TRANSACTION ??`, `BEGIN`},
		{`BEGIN TRANSACTION ISOLATION ??`, `BEGIN`},
		{`BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT, ??`, `BEGIN`},
		{`START ??`, `BEGIN`},

		{`COMMIT TRANSACTION ??`, `COMMIT`},
		{`END ??`, `COMMIT`},

		{`ROLLBACK TRANSACTION ??`, `ROLLBACK`},
		{`ROLLBACK TO ??`, `ROLLBACK`},

		{`SAVEPOINT blah ??`, `SAVEPOINT`},

		{`RELEASE blah ??`, `RELEASE`},
		{`RELEASE SAVEPOINT blah ??`, `RELEASE`},

		{`EXPERIMENTAL SCRUB ??`, `SCRUB`},
		{`EXPERIMENTAL SCRUB TABLE ??`, `SCRUB TABLE`},
		{`EXPERIMENTAL SCRUB DATABASE ??`, `SCRUB DATABASE`},

		{`BACKUP foo TO 'bar' ??`, `BACKUP`},
		{`BACKUP DATABASE ??`, `BACKUP`},
		{`BACKUP foo TO 'bar' AS OF ??`, `BACKUP`},

		{`RESTORE foo FROM 'bar' ??`, `RESTORE`},
		{`RESTORE DATABASE ??`, `RESTORE`},

		{`IMPORT TABLE foo CREATE USING 'foo.sql' CSV DATA ('foo') ??`, `IMPORT`},
		{`IMPORT TABLE ??`, `IMPORT`},

		{`EXPORT ??`, `EXPORT`},
		{`EXPORT INTO CSV 'a' ??`, `EXPORT`},
		{`EXPORT INTO CSV 'a' FROM SELECT a ??`, `SELECT`},
	}

	// The following checks that the test definition above exercises all
	// the help texts mentioned in the grammar.
	t.Run("coverage", func(t *testing.T) {
		testedStrings := make(map[string]struct{})
		for _, test := range testData {
			testedStrings[test.key] = struct{}{}
		}
		// Note: expectedHelpStrings is generated externally
		// from the grammar by help_gen_test.sh.
		for _, expKey := range expectedHelpStrings {
			if _, ok := testedStrings[expKey]; !ok {
				t.Errorf("test missing for: %q", expKey)
			}
		}
	})

	// The following checks that the grammar rules properly report help.
	for _, test := range testData {
		t.Run(test.input, func(t *testing.T) {
			_, err := Parse(test.input)
			if err == nil {
				t.Fatalf("parser didn't trigger error")
			}

			if !strings.HasPrefix(err.Error(), "help token in input") {
				t.Fatal(err)
			}
			pgerr := pgerror.Flatten(err)
			help := pgerr.Hint
			msg := HelpMessage{Command: test.key, HelpMessageBody: HelpMessages[test.key]}
			expected := msg.String()
			if help != expected {
				t.Errorf("unexpected help message: got:\n%s\nexpected:\n%s", help, expected)
			}
		})
	}
}

func TestHelpKeys(t *testing.T) {
	// This test checks that if a help key is a valid prefix for '?',
	// then it is also present in the rendered help message.  It also
	// checks that the parser renders the correct help message.
	for key, body := range HelpMessages {
		t.Run(key, func(t *testing.T) {
			_, err := Parse(key + " ??")
			if err == nil {
				t.Errorf("parser didn't trigger error")
				return
			}
			help := err.Error()
			if !strings.HasPrefix(help, "help: ") {
				// Not a valid help prefix -- e.g. "<source>"
				return
			}

			msg := HelpMessage{Command: key, HelpMessageBody: body}
			expected := msg.String()
			if help != expected {
				t.Errorf("unexpected help message: got:\n%s\nexpected:\n%s", help, expected)
				return
			}
			if !strings.Contains(help, " "+key+"\n") {
				t.Errorf("help text does not contain key %q:\n%s", key, help)
			}
		})
	}
}
