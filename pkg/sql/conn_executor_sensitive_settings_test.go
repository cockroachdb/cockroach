// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

// rawActiveQuery reads the raw ActiveQuery of the session tagged with appName
// from the ListSessions RPC - the surface the DB Console renders directly,
// which (unlike crdb_internal.node_queries, whose `query` column is re-parsed
// and re-formatted by formatActiveQuery) exposes ActiveQuery.Sql and
// ActiveQuery.Placeholders verbatim. It returns them joined so a single
// assertion covers both fields.
func rawActiveQuery(
	ctx context.Context, t *testing.T, statusServer serverpb.StatusServer, appName string,
) string {
	resp, err := statusServer.ListSessions(ctx, &serverpb.ListSessionsRequest{})
	require.NoError(t, err)
	for _, sess := range resp.Sessions {
		if sess.ApplicationName != appName {
			continue
		}
		for _, q := range sess.ActiveQueries {
			return q.Sql + " | placeholders=[" + strings.Join(q.Placeholders, ",") + "]"
		}
	}
	return ""
}

// TestSensitiveSettingInFlightQueryRedaction verifies that a sensitive
// SET CLUSTER SETTING running in a session does not expose its value through
// the live active-query surfaces (crdb_internal.node_queries). It covers the
// direct form (raw statement text bypasses AST formatting - fixed in
// serialize) and the prepared EXECUTE form (registered with constants hidden,
// un-redacted only if the resolved statement proves non-sensitive).
func TestSensitiveSettingInFlightQueryRedaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// The knob blocks the target statement inside execution, after the active
	// query has been registered, so a concurrent observer can inspect it. We
	// match the resolved SET CLUSTER SETTING (its String() is already value-
	// substituted, so we key on the setting name) but not the PREPARE that
	// wraps it, so the EXECUTE - not the PREPARE - is what blocks. cluster.label
	// is matched as the non-sensitive control.
	var mu struct {
		syncutil.Mutex
		armed   bool
		reached chan struct{}
		unblock chan struct{}
	}
	knobs := &sql.ExecutorTestingKnobs{
		BeforeExecute: func(_ context.Context, stmt string, _ *descs.Collection) {
			mu.Lock()
			armed := mu.armed
			reached, unblock := mu.reached, mu.unblock
			if armed && strings.Contains(stmt, "SET CLUSTER SETTING") &&
				(strings.Contains(stmt, "client_secret") || strings.Contains(stmt, "cluster.label")) &&
				!strings.Contains(stmt, "PREPARE") {
				mu.armed = false
				mu.Unlock()
				close(reached)
				<-unblock
				return
			}
			mu.Unlock()
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{SQLExecutor: knobs},
	})
	defer s.Stopper().Stop(ctx)

	const secret = "hunter2-in-flight"
	statusServer := s.StatusServer().(serverpb.StatusServer)

	// observeInFlightExec arms the knob, runs exec on a dedicated session
	// (tagged with a unique application_name), waits until the target
	// statement blocks, reads its raw active query, then unblocks and returns
	// the text.
	observeInFlightExec := func(t *testing.T, appName string, exec func(conn *gosql.Conn) error) string {
		mu.Lock()
		mu.reached = make(chan struct{})
		mu.unblock = make(chan struct{})
		mu.armed = true
		reached, unblock := mu.reached, mu.unblock
		mu.Unlock()

		conn, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()
		_, err = conn.ExecContext(ctx, "SET application_name = $1", appName)
		require.NoError(t, err)

		errCh := make(chan error, 1)
		go func() {
			errCh <- exec(conn)
		}()

		<-reached
		query := rawActiveQuery(ctx, t, statusServer, appName)
		close(unblock)
		require.NoError(t, <-errCh)
		return query
	}

	// observeInFlight is observeInFlightExec for plain statement strings.
	observeInFlight := func(t *testing.T, appName string, stmts ...string) string {
		return observeInFlightExec(t, appName, func(conn *gosql.Conn) error {
			for _, stmt := range stmts {
				if _, err := conn.ExecContext(ctx, stmt); err != nil {
					return err
				}
			}
			return nil
		})
	}

	t.Run("direct SET", func(t *testing.T) {
		query := observeInFlight(t, "direct_set",
			"SET CLUSTER SETTING server.oidc_authentication.client_secret = '"+secret+"'")
		require.NotContains(t, query, secret, "in-flight query text leaked the secret: %s", query)
		require.Contains(t, query, "*****", "expected substituted value, got: %s", query)
	})

	// Note: a typo'd (unregistered) sensitive setting name cannot be observed
	// in flight with this knob - the statement fails during planning, before
	// BeforeExecute fires. Its lingering surface, last_active_query, is
	// covered by TestSensitiveSettingLastActiveQueryRedaction.

	t.Run("driver-bound SET", func(t *testing.T) {
		// The extended protocol - the bind path every SQL driver uses - keeps
		// the statement text in placeholder form and carries the bound secret
		// in ActiveQuery.Placeholders, which must be redacted.
		query := observeInFlightExec(t, "bound_set", func(conn *gosql.Conn) error {
			_, err := conn.ExecContext(ctx,
				"SET CLUSTER SETTING server.oidc_authentication.client_secret = $1", secret)
			return err
		})
		require.NotContains(t, query, secret, "in-flight query leaked the bound secret: %s", query)
		require.Contains(t, query, "placeholders=[<redacted>]", "expected redacted placeholder values, got: %s", query)
	})

	t.Run("driver-bound non-sensitive SET keeps its placeholder values", func(t *testing.T) {
		query := observeInFlightExec(t, "bound_nonsensitive_set", func(conn *gosql.Conn) error {
			_, err := conn.ExecContext(ctx,
				"SET CLUSTER SETTING cluster.label = $1", "not-a-secret")
			return err
		})
		require.Contains(t, query, "not-a-secret", "expected raw placeholder values, got: %s", query)
	})

	t.Run("prepared EXECUTE", func(t *testing.T) {
		query := observeInFlight(t, "prepared_execute",
			"PREPARE p AS SET CLUSTER SETTING server.oidc_authentication.client_secret = $1",
			"EXECUTE p('"+secret+"')")
		require.NotContains(t, query, secret, "in-flight query text leaked the secret: %s", query)
		// The EXECUTE was registered with its constants hidden (its argument is
		// the secret without the setting name), and its target is sensitive, so
		// the raw text is never restored.
		require.Contains(t, query, "EXECUTE", "expected the EXECUTE form preserved, got: %s", query)
		require.Contains(t, query, "'_'", "expected hidden constants, got: %s", query)
	})

	t.Run("non-sensitive EXECUTE keeps its arguments", func(t *testing.T) {
		// Once the prepared statement resolves to a non-sensitive target, the
		// raw client text - including literal arguments - is restored.
		query := observeInFlight(t, "nonsensitive_execute",
			"PREPARE pn AS SET CLUSTER SETTING cluster.label = $1",
			"EXECUTE pn('not-a-secret')")
		require.Contains(t, query, "EXECUTE", "expected the EXECUTE form preserved, got: %s", query)
		require.Contains(t, query, "not-a-secret", "expected raw arguments restored, got: %s", query)
	})

	t.Run("EXPLAIN ANALYZE direct SET", func(t *testing.T) {
		// EXPLAIN ANALYZE executes its inner statement, so the sensitive SET is
		// genuinely in flight and observable. (Plain EXPLAIN does not execute the
		// inner statement, so it never blocks here; its classification is
		// covered by TestStmtMayHaveSecret.) EXPLAIN and EXPLAIN ANALYZE
		// are distinct AST nodes (tree.Explain vs tree.ExplainAnalyze) that both
		// must be unwrapped, or serialize falls back to the raw SQL text.
		query := observeInFlight(t, "explain_direct_set",
			"EXPLAIN ANALYZE SET CLUSTER SETTING server.oidc_authentication.client_secret = '"+secret+"'")
		require.NotContains(t, query, secret, "in-flight query text leaked the secret: %s", query)
		require.Contains(t, query, "*****", "expected substituted value, got: %s", query)
		require.Contains(t, query, "EXPLAIN", "expected the EXPLAIN wrapper preserved, got: %s", query)
	})

	t.Run("EXPLAIN ANALYZE EXECUTE", func(t *testing.T) {
		// An EXECUTE wrapped in EXPLAIN ANALYZE cannot be classified by name
		// (the prepared statement is resolved only at execution time), so it is
		// registered with constants hidden like a bare EXECUTE, and stays that
		// way since its target is sensitive.
		query := observeInFlight(t, "explain_execute",
			"PREPARE pe AS SET CLUSTER SETTING server.oidc_authentication.client_secret = $1",
			"EXPLAIN ANALYZE EXECUTE pe('"+secret+"')")
		require.NotContains(t, query, secret, "in-flight query text leaked the secret: %s", query)
		require.Contains(t, query, "EXECUTE", "expected the EXECUTE form preserved, got: %s", query)
		require.Contains(t, query, "'_'", "expected hidden constants, got: %s", query)
	})

	t.Run("non-sensitive EXPLAIN ANALYZE EXECUTE keeps its arguments", func(t *testing.T) {
		// Once the EXECUTE under the EXPLAIN ANALYZE resolves and its target
		// proves non-sensitive, the raw client text is restored there too.
		query := observeInFlight(t, "nonsensitive_explain_execute",
			"PREPARE pne AS SET CLUSTER SETTING cluster.label = $1",
			"EXPLAIN ANALYZE EXECUTE pne('not-a-secret')")
		require.Contains(t, query, "EXECUTE", "expected the EXECUTE form preserved, got: %s", query)
		require.Contains(t, query, "not-a-secret", "expected raw arguments restored, got: %s", query)
	})

	// Note: plain EXPLAIN EXECUTE subtests are omitted on this branch. They
	// rely on maybeRewriteExplainExecute, which only exists on master; without
	// it a plain EXPLAIN EXECUTE never matches the BeforeExecute knob, so it
	// cannot be observed in flight. It registers with constants hidden and
	// conservatively stays that way (see TestStmtMayHaveSecret).
}

// TestSensitiveSettingWireBoundExecuteRedaction verifies that an EXECUTE that
// is itself prepared over the wire protocol with a placeholder argument -
// Parse "EXECUTE p($1)", Bind 'secret' - does not expose the bound value
// through the live query surfaces while registered. The statement is always
// rejected during resolution (EXECUTE arguments must be variable-free), but
// it sits on the active-query list until then, and its bound argument may be
// a sensitive setting value with no setting name to key redaction on: like
// the statement text, the placeholder list must render redacted. The
// BeforeExecute knob cannot observe this window - the statement fails before
// dispatch - so the test synchronizes on AfterActiveQueryAdded instead.
func TestSensitiveSettingWireBoundExecuteRedaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var mu struct {
		syncutil.Mutex
		armed   bool
		reached chan struct{}
		unblock chan struct{}
	}
	knobs := &sql.ExecutorTestingKnobs{
		AfterActiveQueryAdded: func(sql string) {
			mu.Lock()
			armed := mu.armed
			reached, unblock := mu.reached, mu.unblock
			if armed && strings.Contains(sql, "EXECUTE") && strings.Contains(sql, "wire_bound") {
				mu.armed = false
				mu.Unlock()
				close(reached)
				<-unblock
				return
			}
			mu.Unlock()
		},
	}

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{SQLExecutor: knobs},
	})
	defer s.Stopper().Stop(ctx)

	statusServer := s.StatusServer().(serverpb.StatusServer)

	pgURL, cleanup := s.PGUrl(t, serverutils.User(username.RootUser))
	defer cleanup()
	conf, err := pgx.ParseConfig(pgURL.String())
	require.NoError(t, err)
	conf.RuntimeParams["application_name"] = "wire_bound_execute"
	conn, err := pgx.ConnectConfig(ctx, conf)
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()

	// observeWireBoundExecute arms the knob, runs execStmt with value bound as
	// a wire-protocol placeholder, reads the raw active query while the
	// statement is blocked at registration, and returns it along with the
	// statement's rejection error. The placeholder must be typed by an
	// explicit parameter OID: EXECUTE argument expressions are never
	// type-checked during prepare, so a cast in the statement text cannot
	// type it (which is why an ordinary driver never produces this form).
	observeWireBoundExecute := func(t *testing.T, execStmt, value string) (string, error) {
		mu.Lock()
		mu.reached = make(chan struct{})
		mu.unblock = make(chan struct{})
		mu.armed = true
		reached, unblock := mu.reached, mu.unblock
		mu.Unlock()

		errCh := make(chan error, 1)
		go func() {
			res := conn.PgConn().ExecParams(ctx, execStmt,
				[][]byte{[]byte(value)}, []uint32{uint32(oid.T_text)}, nil, nil).Read()
			errCh <- res.Err
		}()

		select {
		case <-reached:
		case err := <-errCh:
			t.Fatalf("statement finished before it was observed in flight: %v", err)
		}
		query := rawActiveQuery(ctx, t, statusServer, "wire_bound_execute")
		close(unblock)
		return query, <-errCh
	}

	t.Run("sensitive", func(t *testing.T) {
		const secret = "hunter2-wire-bound"
		_, err := conn.Exec(ctx,
			"PREPARE wire_bound_secret AS SET CLUSTER SETTING server.oidc_authentication.client_secret = $1")
		require.NoError(t, err)

		query, execErr := observeWireBoundExecute(
			t, "EXECUTE wire_bound_secret($1)", secret)
		// The rejection is what bounds the exposure window: the statement
		// never executes.
		require.ErrorContains(t, execErr, "variable sub-expressions are not allowed")
		require.NotContains(t, query, secret, "in-flight query leaked the bound secret: %s", query)
		require.Contains(t, query, "EXECUTE", "expected the EXECUTE form, got: %s", query)
		require.Contains(t, query, "placeholders=[<redacted>]", "expected redacted placeholders, got: %s", query)
	})

	t.Run("non-sensitive placeholders are redacted too", func(t *testing.T) {
		// Whether an unresolved EXECUTE wraps a sensitive setting is
		// unknowable during session serialization (the session's prepared
		// statements belong to its own goroutine), so the bound arguments of
		// every unresolved EXECUTE are redacted. Nothing observable is lost:
		// this statement form is always rejected before executing.
		_, err := conn.Exec(ctx,
			"PREPARE wire_bound_label AS SET CLUSTER SETTING cluster.label = $1")
		require.NoError(t, err)

		query, execErr := observeWireBoundExecute(
			t, "EXECUTE wire_bound_label($1)", "not-a-secret")
		require.ErrorContains(t, execErr, "variable sub-expressions are not allowed")
		require.NotContains(t, query, "not-a-secret", "expected redacted placeholders, got: %s", query)
		require.Contains(t, query, "placeholders=[<redacted>]", "expected redacted placeholders, got: %s", query)
	})
}

// TestSensitiveSettingLastActiveQueryRedaction verifies that the persisted
// last_active_query slot - which lingers on an idle session until its next
// statement - does not expose a sensitive setting value.
func TestSensitiveSettingLastActiveQueryRedaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	const secret = "hunter2-last-active"
	obs := sqlutils.MakeSQLRunner(sqlDB)

	lastActiveQueryFor := func(t *testing.T, appName string, expectedErr string, stmts ...string) string {
		conn, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()
		_, err = conn.ExecContext(ctx, "SET application_name = $1", appName)
		require.NoError(t, err)
		for i, stmt := range stmts {
			_, err = conn.ExecContext(ctx, stmt)
			if expectedErr != "" && i == len(stmts)-1 {
				require.ErrorContains(t, err, expectedErr)
			} else {
				require.NoError(t, err)
			}
		}
		// The connection is now idle; the sensitive statement is its
		// last_active_query.
		var lastActive string
		obs.QueryRow(t,
			"SELECT last_active_query FROM crdb_internal.node_sessions WHERE application_name = $1",
			appName).Scan(&lastActive)
		return lastActive
	}

	for _, tc := range []struct {
		name  string
		stmts []string
		// expectedErr, if set, is required of the last statement.
		expectedErr string
		// expectedMark is the substitution expected in last_active_query in
		// place of the secret.
		expectedMark string
	}{
		{name: "direct", stmts: []string{
			"SET CLUSTER SETTING server.oidc_authentication.client_secret = '" + secret + "'"},
			expectedMark: "*****"},
		{name: "prepare", stmts: []string{
			"PREPARE q AS SET CLUSTER SETTING server.oidc_authentication.client_secret = '" + secret + "'"},
			expectedMark: "*****"},
		{name: "execute", stmts: []string{
			"PREPARE r AS SET CLUSTER SETTING server.oidc_authentication.client_secret = $1",
			"EXECUTE r('" + secret + "')"},
			expectedMark: "*****"},
		// A typo'd setting name fails resolution, but the value it carries is
		// still the intended secret; the unrecognized name is conservatively
		// treated as sensitive, and the failed statement lingers here until
		// the session's next statement.
		{name: "typo'd name", stmts: []string{
			"SET CLUSTER SETTING server.oidc_authentication.client_secrett = '" + secret + "'"},
			expectedErr:  "unknown cluster setting",
			expectedMark: "*****"},
		// A failed EXECUTE never resolves to its prepared statement, so the
		// unresolved EXECUTE node - whose argument may be a secret with no
		// setting name to key redaction on - is what lands in
		// last_active_query. It must be rendered with constants hidden.
		{name: "failed execute", stmts: []string{
			"EXECUTE missing('" + secret + "')"},
			expectedErr:  "does not exist",
			expectedMark: "'_'"},
		// A PREPARE wrapping an EXPLAIN EXECUTE parses but always fails during
		// build, so like a failed EXECUTE its argument - a possible secret with
		// no setting name attached - lands here in unresolved form and must be
		// rendered with constants hidden.
		{name: "failed prepared EXPLAIN EXECUTE", stmts: []string{
			"PREPARE inner_stmt AS SELECT $1::STRING",
			"PREPARE wrapper AS EXPLAIN EXECUTE inner_stmt('" + secret + "')"},
			expectedErr:  "EXPLAIN EXECUTE is not supported",
			expectedMark: "'_'"},
		// Nested statement positions are classified by the same AST walk (see
		// stmtMayHaveSecret), so a sensitive SET or an EXECUTE inside a CTE
		// keeps the whole statement's text hidden here too. A CTE-nested plain
		// EXPLAIN SET is the one nested form that plans successfully (planning
		// an EXPLAIN never executes its inner statement).
		{name: "nested EXPLAIN SET", stmts: []string{
			"WITH x AS (EXPLAIN SET CLUSTER SETTING server.oidc_authentication.client_secret = '" +
				secret + "') SELECT count(*) >= 0 FROM x"},
			expectedMark: "*****"},
		// A CTE-nested bare SET parses but fails during planning; its raw text
		// lingers here until the session's next statement.
		{name: "nested failed SET", stmts: []string{
			"WITH x AS (SET CLUSTER SETTING server.oidc_authentication.client_secret = '" +
				secret + "') SELECT 1"},
			expectedErr:  "WITH clause",
			expectedMark: "*****"},
		// A CTE-nested EXPLAIN EXECUTE parses but is rejected during build, so
		// like a failed top-level EXECUTE its argument - a possible secret with
		// no setting name attached - lands here unresolved and must be rendered
		// with constants hidden.
		{name: "nested failed EXPLAIN EXECUTE", stmts: []string{
			"PREPARE nested_inner AS SELECT $1::STRING",
			"WITH x AS (EXPLAIN EXECUTE nested_inner('" + secret + "')) SELECT * FROM x"},
			expectedErr:  "EXPLAIN EXECUTE is not supported",
			expectedMark: "'_'"},
		// Role passwords are classified by the same walk: the raw statement
		// text carries the password, so it lingers here constants-hidden.
		{name: "role password", stmts: []string{
			"CREATE ROLE pw_direct WITH PASSWORD '" + secret + "'"},
			expectedMark: "*****"},
		// An EXECUTE resolving to a password-bearing prepared statement is
		// never marked secret-free, so the unwrapped statement lands here
		// with its password substituted.
		{name: "execute role password", stmts: []string{
			"CREATE ROLE pw_exec",
			"PREPARE pw_prep AS ALTER ROLE pw_exec WITH PASSWORD $1",
			"EXECUTE pw_prep('" + secret + "')"},
			expectedMark: "*****"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			appName := "last_active_" + strings.ReplaceAll(tc.name, " ", "_")
			lastActive := lastActiveQueryFor(t, appName, tc.expectedErr, tc.stmts...)
			require.NotContains(t, lastActive, secret,
				"last_active_query leaked the secret: %s", lastActive)
			require.Contains(t, lastActive, tc.expectedMark,
				"expected substituted value, got: %s", lastActive)
		})
	}
}

// TestSensitiveSettingFailedSetErrorRecording verifies that the error of a
// failed SET CLUSTER SETTING targeting a sensitive setting is reduced to its
// redaction-safe parts on the recording surfaces that unredacted debug zips
// collect: SQL stats last_error and transaction execution insights. Setting
// validation errors can quote the offending input (HBA config parse errors
// echo config tokens, which can include an LDAP bind password). The
// client-visible error deliberately keeps its full text - it is an
// interactive privileged surface, and the client supplied the value.
func TestSensitiveSettingFailedSetErrorRecording(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	runner := sqlutils.MakeSQLRunner(db)

	const secret = "hunter2-validation-secret"
	_, err := db.Exec(
		"SET CLUSTER SETTING server.host_based_authentication.configuration = " +
			"'host all all all ldap " + secret + "'")
	require.Error(t, err)
	// The client keeps the full error, including the echoed value.
	require.ErrorContains(t, err, secret)

	// The recorded error in SQL statement statistics must be the reduced
	// form: `last_error` is dumped raw by unredacted debug zips. Statement
	// stats are ingested asynchronously, so poll for the row.
	testutils.SucceedsSoon(t, func() error {
		rows := runner.QueryStr(t,
			`SELECT last_error FROM crdb_internal.node_statement_statistics
			 WHERE key LIKE '%host_based_authentication%' AND last_error IS NOT NULL`)
		if len(rows) == 0 {
			return errors.New("no statement statistics row for the failed SET yet")
		}
		for _, row := range rows {
			require.NotContains(t, row[0], secret)
		}
		return nil
	})

	// The failed implicit transaction is recorded as a transaction insight
	// carrying the statement's error; it must be the reduced form as well.
	// Note that the failed SET is not itself a statement insight (SET
	// statements are ignored by the insights detector), which is why the
	// transaction-level error is the surface to check.
	testutils.SucceedsSoon(t, func() error {
		rows := runner.QueryStr(t,
			`SELECT last_error_redactable FROM crdb_internal.node_txn_execution_insights
			 WHERE last_error_redactable IS NOT NULL`)
		if len(rows) == 0 {
			return errors.New("no transaction insight for the failed SET yet")
		}
		for _, row := range rows {
			require.NotContains(t, row[0], secret)
		}
		return nil
	})

	// A non-sensitive setting keeps its full recorded error, including the
	// echoed offending value.
	_, err = db.Exec("SET CLUSTER SETTING sql.defaults.distsql = 'bogus-mode'")
	require.Error(t, err)
	require.ErrorContains(t, err, "bogus-mode")
	testutils.SucceedsSoon(t, func() error {
		rows := runner.QueryStr(t,
			`SELECT last_error FROM crdb_internal.node_statement_statistics
			 WHERE key LIKE '%sql.defaults.distsql%' AND last_error IS NOT NULL`)
		if len(rows) == 0 {
			return errors.New("no statement statistics row for the failed non-sensitive SET yet")
		}
		require.Contains(t, rows[0][0], "bogus-mode")
		return nil
	})
}

// TestSensitiveSettingFailedSetLogRedaction verifies that the error of a failed
// SET CLUSTER SETTING targeting a sensitive setting is reduced to its
// redaction-safe parts in the statement execution log event (query_execute,
// which backs failed_query). Its ErrorText is dumped raw by unredacted debug
// zips, and setting validation errors can quote the offending input. A failed
// non-sensitive SET keeps its full error, echoed value included.
func TestSensitiveSettingFailedSetLogRedaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	spy := logtestutils.NewStructuredLogSpy(
		t,
		[]logpb.Channel{logpb.Channel_SQL_EXEC},
		[]string{"query_execute"},
		logtestutils.FromLogEntry[eventpb.QueryExecute],
		func(_ logpb.Entry, qe eventpb.QueryExecute) bool {
			return qe.ErrorText != "" && qe.Tag == "SET CLUSTER SETTING"
		},
	)
	cleanup := log.InterceptWith(ctx, spy)
	defer cleanup()

	runner := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))
	runner.Exec(t, "SET CLUSTER SETTING sql.trace.log_statement_execute = true")

	const secret = "hunter2-exec-log-secret"
	db := s.ApplicationLayer().SQLConn(t)
	_, err := db.Exec(
		"SET CLUSTER SETTING server.host_based_authentication.configuration = " +
			"'host all all all ldap " + secret + "'")
	require.Error(t, err)
	// The client keeps the full error, including the echoed value.
	require.ErrorContains(t, err, secret)

	// A non-sensitive setting keeps its full error as a control.
	_, err = db.Exec("SET CLUSTER SETTING sql.defaults.distsql = 'bogus-mode'")
	require.Error(t, err)
	require.ErrorContains(t, err, "bogus-mode")

	log.FlushAllSync()

	var sawSensitive, sawControl bool
	for _, qe := range spy.GetLogs(logpb.Channel_SQL_EXEC) {
		stmt := qe.Statement.StripMarkers()
		errText := string(qe.ErrorText)
		switch {
		case strings.Contains(stmt, "host_based_authentication"):
			require.NotContains(t, errText, secret)
			sawSensitive = true
		case strings.Contains(stmt, "sql.defaults.distsql"):
			require.Contains(t, errText, "bogus-mode")
			sawControl = true
		}
	}
	require.True(t, sawSensitive, "no query_execute log for the failed sensitive SET")
	require.True(t, sawControl, "no query_execute log for the failed non-sensitive SET")
}
