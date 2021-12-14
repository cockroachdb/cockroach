// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvtenantccl_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

// TestTenantTracesAreRedacted is an end-to-end version of
// `kvserver.TestMaybeRedactRecording`.
func TestTenantTracesAreRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunTrueAndFalse(t, "redactable", func(t *testing.T, redactable bool) {
		testTenantTracesAreRedactedImpl(t, redactable)
	})
}

func testTenantTracesAreRedactedImpl(t *testing.T, redactable bool) {
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	const (
		sensitiveString = "super-secret-stuff"
		visibleString   = "tenant-can-see-this"
	)

	recCh := make(chan tracing.Recording, 1)

	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
						TestingEvalFilter: func(args kvserverbase.FilterArgs) *roachpb.Error {
							log.Eventf(args.Ctx, "%v", sensitiveString)
							log.Eventf(args.Ctx, "%v", log.Safe(visibleString))
							return nil
						},
					},
				},
				SQLExecutor: &sql.ExecutorTestingKnobs{
					WithStatementTrace: func(trace tracing.Recording, stmt string) {
						if stmt == "CREATE TABLE kv(k STRING PRIMARY KEY, v STRING)" {
							recCh <- trace
						}
					},
				},
			},
		},
	}
	tc := serverutils.StartNewTestCluster(t, 1, args)
	tc.Server(0).TracerI().(*tracing.Tracer).SetRedactable(redactable)
	defer tc.Stopper().Stop(ctx)

	// Queries from the system tenant will receive unredacted traces
	// since the tracer will not have the redactable flag set.
	t.Run("system-tenant", func(t *testing.T) {
		db := tc.ServerConn(0)
		defer db.Close()
		runner := sqlutils.MakeSQLRunner(db)
		runner.Exec(t, `CREATE TABLE kv(k STRING PRIMARY KEY, v STRING)`)
		trace := <-recCh

		require.NotEmpty(t, trace)
		var found bool
		for _, rs := range trace {
			for _, s := range rs.Logs {
				if strings.Contains(s.Msg().StripMarkers(), sensitiveString) {
					found = true
				}
			}
		}
		require.True(t, found, "did not find '%q' in trace:\n%s",
			sensitiveString, trace,
		)
	})

	t.Run("regular-tenant", func(t *testing.T) {
		_, tenDB := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
			TenantID:     roachpb.MakeTenantID(security.EmbeddedTenantIDs()[0]),
			TestingKnobs: args.ServerArgs.Knobs,
		})
		defer tenDB.Close()
		runner := sqlutils.MakeSQLRunner(tenDB)
		runner.Exec(t, `CREATE TABLE kv(k STRING PRIMARY KEY, v STRING)`)
		trace := <-recCh

		require.NotEmpty(t, trace)
		var found bool
		var foundRedactedMarker bool
		for _, rs := range trace {
			for _, s := range rs.Logs {
				if strings.Contains(s.Msg().StripMarkers(), sensitiveString) {
					t.Fatalf(
						"trace for tenant contained KV-level trace message '%q':\n%s",
						sensitiveString, trace,
					)
				}
				if strings.Contains(s.Msg().StripMarkers(), visibleString) {
					found = true
				}
				if strings.Contains(s.Msg().StripMarkers(), string(server.TraceRedactedMarker)) {
					foundRedactedMarker = true
				}
			}
		}

		// In both cases we don't expect to see the `TraceRedactedMarker`
		// since that's only shown when the server is in an inconsistent
		// state or if there's a version mismatch between client and server.
		if redactable {
			// If redaction was on, we expect the tenant to see safe information in its
			// trace.
			require.True(t, found, "did not see expected trace message '%q':\n%s",
				visibleString, trace)
			require.False(t, foundRedactedMarker, "unexpectedly found '%q':\n%s",
				string(server.TraceRedactedMarker), trace)
		} else {
			// Otherwise, expect the opposite: not even safe information makes it through,
			// because it gets replaced with foundRedactedMarker.
			require.False(t, found, "unexpectedly saw message '%q':\n%s",
				visibleString, trace)
			require.False(t, foundRedactedMarker, "unexpectedly found '%q':\n%s",
				string(server.TraceRedactedMarker), trace)
		}
	})
}
