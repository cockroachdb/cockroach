// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package unsafesql_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/unsafesql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

func checkUnsafeErr(t *testing.T, err error) {
	var pqErr *pq.Error

	if errors.Is(err, sqlerrors.ErrUnsafeTableAccess) {
		return
	}

	if errors.As(err, &pqErr) &&
		pqErr.Code == "42501" &&
		strings.Contains(pqErr.Message, "Access to crdb_internal and system is restricted") {
		return
	}

	t.Fatal("expected unsafe access error, got", err)
}

func TestAccessCheckServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	pool := s.SQLConn(t)
	defer pool.Close()

	runner := sqlutils.MakeSQLRunner(pool)
	// override the log limiter so that the tests can run without pauses.
	defer unsafesql.TestRemoveLimiters()()

	// create a user table to test user table access.
	runner.Exec(t, "CREATE TABLE foo (id INT PRIMARY KEY)")

	// create and register a log accessedSpy to see the unsafe access logs
	accessedSpy := logtestutils.NewStructuredLogSpy[eventpb.UnsafeInternalsAccessed](
		t, []logpb.Channel{logpb.Channel_SENSITIVE_ACCESS}, []string{"unsafe_internals_accessed"}, logtestutils.FromLogEntry,
	)
	// create and register a log deniedSpy to see the denied access logs
	deniedSpy := logtestutils.NewStructuredLogSpy[eventpb.UnsafeInternalsDenied](
		t, []logpb.Channel{logpb.Channel_SENSITIVE_ACCESS}, []string{"unsafe_internals_denied"}, logtestutils.FromLogEntry,
	)
	accessedCleanup := log.InterceptWith(ctx, accessedSpy)
	deniedCleanup := log.InterceptWith(ctx, deniedSpy)
	defer accessedCleanup()
	defer deniedCleanup()

	// helper function for sending queries in different ways.
	sendQuery := func(allowUnsafe bool, internal bool, query string) error {
		if internal {
			idb := s.InternalDB().(isql.DB)
			err := idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				txn.SessionData().LocalOnlySessionData.AllowUnsafeInternals = allowUnsafe

				_, err := txn.QueryBuffered(ctx, "internal-query", txn.KV(), query)
				return err
			})
			if err != nil {
				return err
			}
		} else {
			conn, err := pool.Conn(ctx)
			if err != nil {
				return err
			}
			_, err = conn.ExecContext(ctx, "SET allow_unsafe_internals = $1", allowUnsafe)
			if err != nil {
				return err
			}
			_, err = conn.QueryContext(ctx, query)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, test := range []struct {
		Query                string
		AppName              string
		Internal             bool
		AllowUnsafeInternals bool
		Passes               bool
		LogsAccessed         bool
		LogsDenied           bool
	}{
		// Regular tables aren't considered unsafe.
		{
			Query:  "SELECT * FROM foo",
			Passes: true,
		},
		// Tests on the system objects.
		{
			Query:                "SELECT * FROM system.namespace",
			Internal:             true,
			AllowUnsafeInternals: false,
			Passes:               true,
		},
		{
			Query:                "SELECT * FROM system.namespace",
			Internal:             true,
			AllowUnsafeInternals: true,
			Passes:               true,
		},
		{
			Query:                "SELECT * FROM system.namespace",
			Internal:             false,
			AllowUnsafeInternals: false,
			Passes:               false,
			LogsDenied:           true,
		},
		{
			Query:                "SELECT * FROM system.namespace",
			Internal:             false,
			AllowUnsafeInternals: true,
			Passes:               true,
			LogsAccessed:         true,
		},
		{
			Query:                "SELECT * FROM system.namespace",
			AppName:              "$ internal app",
			Internal:             false,
			AllowUnsafeInternals: false,
			Passes:               true,
			LogsAccessed:         false,
		},
		// Tests on unsupported crdb_internal objects.
		{
			Query:                "SELECT * FROM crdb_internal.gossip_alerts",
			AllowUnsafeInternals: false,
			Passes:               false,
			LogsDenied:           true,
		},
		{
			Query:                "SELECT * FROM crdb_internal.gossip_alerts",
			AllowUnsafeInternals: true,
			Passes:               true,
			LogsAccessed:         true,
		},
		// Tests on supported crdb_internal objects.
		{
			Query:                "SELECT * FROM crdb_internal.zones",
			AllowUnsafeInternals: false,
			Passes:               true,
		},
		{
			Query:                "SELECT * FROM crdb_internal.zones",
			AllowUnsafeInternals: true,
			Passes:               true,
		},
		// Non-crdb_internal functions pass
		{
			Query:  "SELECT * FROM generate_series(1, 5)",
			Passes: true,
		},
		// Crdb_internal functions require the override.
		{
			Query:      "SELECT * FROM crdb_internal.tenant_span_stats()",
			Passes:     false,
			LogsDenied: true,
		},
		{
			Query:                "SELECT * FROM crdb_internal.tenant_span_stats()",
			AllowUnsafeInternals: true,
			Passes:               true,
			LogsAccessed:         true,
		},
		// Tests on delegate behavior.
		{
			Query:  "SHOW GRANTS",
			Passes: true,
		},
		{
			// this query is what show grants is using under the hood.
			Query:      "SELECT * FROM crdb_internal.privilege_name('SELECT')",
			Passes:     false,
			LogsDenied: true,
		},
		{
			Query:  "SHOW DATABASES",
			Passes: true,
		},
		{
			// this query is what show databases is using under the hood.
			Query:      "SELECT * FROM crdb_internal.databases",
			Passes:     false,
			LogsDenied: true,
		},
		{
			// test nested delegates
			Query:  "SHOW JOBS WHEN COMPLETE SELECT job_id FROM [SHOW JOBS]",
			Passes: true,
		},
		{
			// unexpected unsafe builtin access
			Query:  "SELECT col_description(0, 0)",
			Passes: true,
		},
	} {
		t.Run(fmt.Sprintf("query=%s,internal=%t,allowUnsafe=%t", test.Query, test.Internal, test.AllowUnsafeInternals), func(t *testing.T) {
			accessedSpy.Reset()
			deniedSpy.Reset()
			runner.Exec(t, "SET application_name = $1", test.AppName)
			err := sendQuery(test.AllowUnsafeInternals, test.Internal, test.Query)
			if test.Passes {
				require.NoError(t, err)
			} else {
				checkUnsafeErr(t, err)
			}

			if test.LogsAccessed {
				require.Equal(t, 1, accessedSpy.Count(), "expected exactly one log entry for unsafe internals access")
			} else {
				require.Equal(t, 0, accessedSpy.Count(), "expected no log entries")
			}

			if test.LogsDenied {
				require.Equal(t, 1, deniedSpy.Count(), "expected exactly one log entry for unsafe internals access")
			} else {
				require.Equal(t, 0, deniedSpy.Count(), "expected no log entries")
			}
		})
	}
}

// panickingStatement is a mock Statement that panics during formatting
type panickingStatement struct{}

func (ps panickingStatement) String() string {
	return "panicking statement"
}

func (ps panickingStatement) Format(ctx *tree.FmtCtx) {
	panic("deliberate panic for testing")
}

func (ps panickingStatement) StatementReturnType() tree.StatementReturnType {
	return tree.Unknown
}

func (ps panickingStatement) StatementType() tree.StatementType {
	return tree.TypeDML
}

func (ps panickingStatement) StatementTag() string {
	return "TEST"
}

func TestPanickingSQLFormat(t *testing.T) {
	result := unsafesql.SafeFormatQuery(panickingStatement{}, nil, &settings.Values{})
	require.Equal(
		t,
		"<panicked query format>",
		string(result),
	)
}

// TestBuiltinAccessingCRDBInternalWithMemoStaleness verifies that builtins
// which internally access crdb_internal tables work correctly when:
// 1. allow_unsafe_internals is false (default for external users)
// 2. The query is executed multiple times (triggering memo staleness checks)
//
// This specifically tests the fix in util.go which adds BUILTIN_UNSAFE_ALLOWED
// to dependencies when inside a builtin context, so that memo staleness
// checking knows to bypass unsafe internal checks.
func TestBuiltinAccessingCRDBInternalWithMemoStaleness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	pool := s.SQLConn(t)
	defer pool.Close()

	runner := sqlutils.MakeSQLRunner(pool)

	// Set allow_unsafe_internals to false - this is the scenario where
	// external users can't directly access crdb_internal, but builtins should
	// still work.
	runner.Exec(t, "SET allow_unsafe_internals = false")

	// Create a UDF so pg_get_functiondef has something to work with.
	// pg_get_functiondef internally accesses crdb_internal.create_function_statements.
	runner.Exec(t, "CREATE FUNCTION test_udf_for_memo() RETURNS INT LANGUAGE SQL AS $$ SELECT 42 $$")

	// Get the OID of the function.
	var funcOID int
	runner.QueryRow(t, "SELECT oid FROM pg_proc WHERE proname = 'test_udf_for_memo'").Scan(&funcOID)

	// Execute pg_get_functiondef multiple times. The first execution builds
	// the plan and caches the memo. Subsequent executions trigger memo
	// staleness checking via CheckDependencies -> checkDataSourcePrivileges.
	//
	// Without the fix in util.go, the staleness check would fail because it
	// doesn't know the crdb_internal access was from a builtin context.
	for i := 0; i < 3; i++ {
		var result string
		err := runner.DB.QueryRowContext(ctx, "SELECT pg_get_functiondef($1::oid)", funcOID).Scan(&result)
		require.NoError(t, err, "iteration %d: pg_get_functiondef should work even with allow_unsafe_internals=false", i)
		require.Contains(t, result, "test_udf_for_memo", "iteration %d: expected function definition", i)
	}
}
