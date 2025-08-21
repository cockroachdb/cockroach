// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package unsafesql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/unsafesql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

func TestCheckUnsafeInternalsAccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("returns the right response with the right session data", func(t *testing.T) {
		for _, test := range []struct {
			Internal             bool
			AllowUnsafeInternals bool
			Passes               bool
		}{
			{Internal: true, AllowUnsafeInternals: true, Passes: true},
			{Internal: true, AllowUnsafeInternals: false, Passes: true},
			{Internal: false, AllowUnsafeInternals: true, Passes: true},
			{Internal: false, AllowUnsafeInternals: false, Passes: false},
		} {
			t.Run(fmt.Sprintf("%t", test), func(t *testing.T) {
				err := unsafesql.CheckInternalsAccess(&sessiondata.SessionData{
					SessionData: sessiondatapb.SessionData{
						Internal: test.Internal,
					},
					LocalOnlySessionData: sessiondatapb.LocalOnlySessionData{
						AllowUnsafeInternals: test.AllowUnsafeInternals,
					},
				})

				if test.Passes {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, sqlerrors.ErrUnsafeTableAccess)
				}
			})
		}
	})
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

	_, err := pool.Exec("CREATE TABLE foo (id INT PRIMARY KEY)")
	require.NoError(t, err)

	// helper func for setting a safe connection.
	safeConn := func(t *testing.T) *gosql.Conn {
		conn, err := pool.Conn(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "SET allow_unsafe_internals = false")
		require.NoError(t, err)
		return conn
	}

	t.Run("regular user activity is unaffected", func(t *testing.T) {
		conn := safeConn(t)
		_, err := conn.QueryContext(ctx, "SELECT * FROM foo")
		require.NoError(t, err)
	})

	t.Run("accessing the system database", func(t *testing.T) {
		q := "SELECT * FROM system.namespace"

		t.Run("as an external querier", func(t *testing.T) {
			for _, test := range []struct {
				AllowUnsafeInternals bool
				Passes               bool
			}{
				{AllowUnsafeInternals: false, Passes: false},
				{AllowUnsafeInternals: true, Passes: true},
			} {
				t.Run(fmt.Sprintf("%t", test), func(t *testing.T) {
					conn := s.SQLConn(t)
					defer conn.Close()
					_, err := conn.ExecContext(ctx, "SET allow_unsafe_internals = $1", test.AllowUnsafeInternals)
					require.NoError(t, err)

					_, err = conn.QueryContext(ctx, q)
					if test.Passes {
						require.NoError(t, err)
					} else {
						checkUnsafeErr(t, err)
					}
				})
			}
		})

		t.Run("as an internal querier", func(t *testing.T) {
			for _, test := range []struct {
				AllowUnsafeInternals bool
				Passes               bool
			}{
				{AllowUnsafeInternals: false, Passes: true},
				{AllowUnsafeInternals: true, Passes: true},
			} {
				t.Run(fmt.Sprintf("%t", test), func(t *testing.T) {
					idb := s.InternalDB().(isql.DB)
					err := idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
						txn.SessionData().LocalOnlySessionData.AllowUnsafeInternals = test.AllowUnsafeInternals

						_, err := txn.QueryBuffered(ctx, "internal-query", txn.KV(), q)
						return err
					})

					require.NoError(t, err)

					if test.Passes {
						require.NoError(t, err)
					} else {
						checkUnsafeErr(t, err)
					}
				})
			}
		})
	})

	t.Run("accessing the crdb_internal schema", func(t *testing.T) {
		t.Run("supported table allowed", func(t *testing.T) {
			conn := safeConn(t)

			// Supported crdb_internal tables should be allowed even when allow_unsafe_internals = false
			_, err := conn.QueryContext(ctx, "SELECT * FROM crdb_internal.zones")
			require.NoError(t, err, "supported crdb_internal table (zones) should be accessible when allow_unsafe_internals = false")
		})

		t.Run("unsupported table denied", func(t *testing.T) {
			conn := safeConn(t)

			// Unsupported crdb_internal tables should be denied when allow_unsafe_internals = false
			_, err := conn.QueryContext(ctx, "SELECT * FROM crdb_internal.gossip_alerts")
			checkUnsafeErr(t, err)
		})
	})

	// The functionality for this lies in the optbuilder package file,
	// but it is tested here as that package does not setup a test server.
	t.Run("accessing crdb_internal builtins", func(t *testing.T) {
		t.Run("non crdb_internal builtin allowed", func(t *testing.T) {
			conn := safeConn(t)

			// Non crdb_internal tables should be allowed.
			_, err := conn.QueryContext(ctx, "SELECT * FROM generate_series(1,5)")
			require.NoError(t, err)
		})

		t.Run("crdb_internal builtin not allowed", func(t *testing.T) {
			conn := safeConn(t)

			// Unsupported crdb_internal builtins should be denied.
			_, err := conn.QueryContext(ctx, "SELECT * FROM crdb_internal.tenant_span_stats()")
			checkUnsafeErr(t, err)
		})
	})

	// The functionality for this check also lives in the optbuilder package
	// but is tested here.
	t.Run("skips delegation", func(t *testing.T) {
		t.Run("delegation is allowed", func(t *testing.T) {
			conn := safeConn(t)

			// tests delegation to builtins
			_, err := conn.ExecContext(ctx, "show grants")
			require.NoError(t, err)

			// tests delegation to crdb_internal tables
			_, err = conn.ExecContext(ctx, "show databases")
			require.NoError(t, err)
		})

		t.Run("underlying tables which delegates rely on are not", func(t *testing.T) {
			conn := safeConn(t)

			// tests delegation to builtins
			_, err := conn.ExecContext(ctx, "SELECT * FROM crdb_internal.privilege_name('DELETE')")
			checkUnsafeErr(t, err)

			// tests delegation to crdb_internal tables
			_, err = conn.ExecContext(ctx, "SELECT * FROM crdb_internal.databases")
			checkUnsafeErr(t, err)
		})
	})
}
