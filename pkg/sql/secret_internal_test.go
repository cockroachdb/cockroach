// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestStmtMayHaveSecret locks down which statement shapes are classified as
// possibly carrying a secret. It covers the wrapper forms (PREPARE, EXPLAIN,
// EXPLAIN ANALYZE) that the in-flight redaction test cannot reach when the
// wrapper does not execute its inner statement, using
// server.oidc_authentication.client_secret (sensitive) and
// sql.defaults.distsql (not sensitive) as representative real settings.
func TestStmtMayHaveSecret(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		sql  string
		want bool
	}{
		// Sensitive SET CLUSTER SETTING forms.
		{"SET CLUSTER SETTING server.oidc_authentication.client_secret = 'x'", true},
		{"SET CLUSTER SETTING server.oidc_authentication.client_secret = $1", true},
		{"ALTER TENANT ALL SET CLUSTER SETTING server.oidc_authentication.client_secret = 'x'", true},
		{"PREPARE p AS SET CLUSTER SETTING server.oidc_authentication.client_secret = $1", true},
		{"EXPLAIN SET CLUSTER SETTING server.oidc_authentication.client_secret = 'x'", true},
		{"EXPLAIN ANALYZE SET CLUSTER SETTING server.oidc_authentication.client_secret = 'x'", true},
		{"EXPLAIN (VERBOSE) SET CLUSTER SETTING server.oidc_authentication.client_secret = 'x'", true},

		// Nested forms are classified too: the walk reaches a sensitive SET
		// in any statement position - a CTE body, a [...] statement source
		// (even inside a subquery), or those under a PREPARE. Most of these
		// fail during build, but their raw text and errors still land on the
		// classifier-keyed surfaces; a nested plain EXPLAIN SET plans without
		// executing.
		{"WITH x AS (SET CLUSTER SETTING server.oidc_authentication.client_secret = 'x') SELECT 1", true},
		{"WITH x AS (EXPLAIN SET CLUSTER SETTING server.oidc_authentication.client_secret = 'x') SELECT * FROM x", true},
		{"SELECT * FROM [EXPLAIN SET CLUSTER SETTING server.oidc_authentication.client_secret = 'x']", true},
		{"SELECT (SELECT count(*) FROM [EXPLAIN SET CLUSTER SETTING server.oidc_authentication.client_secret = 'x'])", true},
		{"PREPARE p AS WITH x AS (EXPLAIN SET CLUSTER SETTING server.oidc_authentication.client_secret = $1) SELECT * FROM x", true},
		{"WITH x AS (EXPLAIN SET CLUSTER SETTING sql.defaults.distsql = 'off') SELECT * FROM x", false},

		// A name with no registry match is conservatively classified: it is
		// most likely a typo - possibly of a sensitive setting's name, with
		// the intended secret as its value - and the statement will fail
		// resolution against this same registry, so nothing useful is
		// redacted in exchange.
		{"SET CLUSTER SETTING server.oidc_authentication.client_secrett = 'x'", true},
		{"SET CLUSTER SETTING no.such.setting = 'x'", true},

		// Password-bearing role statements: the raw text or a bound
		// placeholder may carry the password.
		{"CREATE ROLE foo WITH PASSWORD 'x'", true},
		{"CREATE ROLE foo WITH LOGIN PASSWORD 'x'", true},
		{"ALTER ROLE foo WITH PASSWORD 'x'", true},
		{"ALTER ROLE foo WITH PASSWORD $1", true},
		{"CREATE ROLE foo WITH PASSWORD NULL", true},
		{"PREPARE p AS ALTER ROLE foo WITH PASSWORD $1", true},
		{"CREATE ROLE foo", false},
		{"ALTER ROLE foo WITH LOGIN", false},
		// A statement can embed secret-carrying nodes of different kinds in
		// different branches; any one of them classifies it.
		{"WITH a AS (EXPLAIN SET CLUSTER SETTING sql.defaults.distsql = $1), " +
			"b AS (EXPLAIN ALTER ROLE foo WITH PASSWORD $2) SELECT * FROM a, b", true},

		// EXECUTE forms.
		{"EXECUTE p('x')", true},
		{"EXECUTE p", true},
		{"EXPLAIN EXECUTE p('x')", true},
		{"EXPLAIN ANALYZE EXECUTE p('x')", true},
		{"EXPLAIN (VERBOSE) EXECUTE p('x')", true},
		// A PREPARE wrapping an EXPLAIN EXECUTE parses (it always fails during
		// build), and its argument list must be treated the same way. PREPARE
		// directly wrapping an EXECUTE is a parse error, so it needs no arm.
		{"PREPARE pw AS EXPLAIN EXECUTE p('x')", true},
		{"PREPARE pw AS EXPLAIN ANALYZE EXECUTE p('x')", true},

		// Nested EXECUTE forms are classified too: the classifier walks the
		// full AST. These all fail during build (a nested EXPLAIN EXECUTE is
		// rejected), but their raw text - including the argument list - still
		// lands on the classifier-keyed surfaces.
		{"WITH x AS (EXPLAIN EXECUTE p('x')) SELECT * FROM x", true},
		{"SELECT * FROM [EXPLAIN EXECUTE p('x')]", true},
		{"PREPARE pw AS WITH x AS (EXPLAIN EXECUTE p('x')) SELECT * FROM x", true},

		// Bulk-I/O and replication statements reference an external URI, KMS
		// URI, connection string, or encryption passphrase that may embed
		// credentials. They are recognized by statement type (see
		// statementNodeMayHaveSecret).
		{"BACKUP INTO 'x'", true},
		{"BACKUP TABLE foo INTO 'x'", true},
		{"RESTORE TABLE foo FROM 'abc' IN 'x'", true},
		{"SHOW BACKUPS IN 'x'", true},
		{"SHOW BACKUP LATEST IN 'x' WITH ENCRYPTION_PASSPHRASE = 'secret'", true},
		{"ALTER BACKUP 'foo' ADD NEW_KMS = 'a' WITH OLD_KMS = 'b'", true},
		{"CREATE SCHEDULE FOR BACKUP TABLE foo INTO 'x' RECURRING '@hourly'", true},
		{"ALTER BACKUP SCHEDULE 123 SET RECURRING '@daily'", true},
		{"IMPORT INTO foo(id) CSV DATA ('x')", true},
		{"EXPORT INTO CSV 'x' FROM TABLE a", true},
		{"CREATE CHANGEFEED FOR TABLE foo INTO 'sink'", true},
		{"CREATE CHANGEFEED INTO 'sink' AS SELECT * FROM foo", true},
		{"ALTER CHANGEFEED 123 SET sink = 'x'", true},
		{"CREATE SCHEDULE FOR CHANGEFEED TABLE foo INTO 'sink' RECURRING '@hourly'", true},
		{"CREATE EXTERNAL CONNECTION 'foo' AS 'bar'", true},
		{"ALTER EXTERNAL CONNECTION 'foo' AS 'bar'", true},
		{"CHECK EXTERNAL CONNECTION 'foo'", true},
		{"CREATE VIRTUAL CLUSTER destination FROM REPLICATION OF source ON 'pgurl'", true},
		{"ALTER VIRTUAL CLUSTER 'foo' START REPLICATION OF 'bar' ON 'baz'", true},
		{"CREATE LOGICAL REPLICATION STREAM FROM TABLE foo ON 'uri' INTO TABLE foo", true},
		{"CREATE LOGICALLY REPLICATED TABLE foo FROM TABLE foo ON 'uri'", true},

		// Wrapped and nested bulk-I/O forms are classified via the same walk.
		{"EXPLAIN BACKUP TABLE foo INTO 'x'", true},
		{"PREPARE p AS BACKUP TABLE foo INTO 'x'", true},
		{"SELECT * FROM [SHOW BACKUPS IN 'x']", true},

		// COPY carries a secret only in the WITH (destination = ...) file-upload
		// form; COPY ... FROM STDIN / TO STDOUT does not.
		{"COPY t (a, b, c) FROM STDIN WITH CSV destination = 'filename'", true},
		{"COPY t FROM STDIN", false},
		{"COPY t (a, b) FROM STDIN", false},
		{"COPY t TO STDOUT", false},
		{"COPY (SELECT * FROM t) TO STDOUT", false},

		// Failover forms of ALTER VIRTUAL CLUSTER ... REPLICATION name no source
		// URI and are not classified; plain virtual cluster statements carry no
		// secret either.
		{"ALTER VIRTUAL CLUSTER 'foo' COMPLETE REPLICATION TO LATEST", false},
		{"CREATE VIRTUAL CLUSTER bar", false},

		// Non-sensitive settings and unrelated statements are not classified.
		{"SELECT 1", false},
		{"PREPARE p AS SELECT $1::INT", false},
		{"EXPLAIN SELECT 1", false},
		{"SET CLUSTER SETTING sql.defaults.distsql = 'off'", false},
		{"EXPLAIN SET CLUSTER SETTING sql.defaults.distsql = 'off'", false},
		{"SET application_name = 'x'", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parser.ParseOne(tc.sql)
			require.NoError(t, err)
			require.Equal(t, tc.want, stmtMayHaveSecret(stmt.AST))
		})
	}
}

// BenchmarkStmtMayHaveSecret measures the AST walk that classifies a
// statement as possibly carrying a secret. It runs once per statement
// construction (see makeStatement); executions of a prepared statement reuse
// the PREPARE-time result. The interesting cases are the secret-free ones,
// where the walk visits the full AST without short-circuiting.
func BenchmarkStmtMayHaveSecret(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"select1", "SELECT 1"},
		{"pointSelect", "SELECT c FROM sbtest1 WHERE id = $1"},
		{"insert", "INSERT INTO t (a, b, c) VALUES ($1, $2, $3), ($4, $5, $6)"},
		{"threeWayJoin", `SELECT a.id, b.name, sum(c.total)
			FROM orders AS a
			JOIN customers AS b ON a.cust_id = b.id
			JOIN line_items AS c ON c.order_id = a.id
			WHERE a.placed > now() - '1 day'::INTERVAL AND b.region = $1
			GROUP BY a.id, b.name
			ORDER BY sum(c.total) DESC
			LIMIT 10`},
		{"execute", "EXECUTE p('x')"},
		{"sensitiveSet", "SET CLUSTER SETTING server.oidc_authentication.client_secret = 'x'"},
		{"nonSensitiveSet", "SET CLUSTER SETTING sql.defaults.distsql = 'off'"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			stmt, err := parser.ParseOne(tc.sql)
			require.NoError(b, err)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = stmtMayHaveSecret(stmt.AST)
			}
		})
	}
}
