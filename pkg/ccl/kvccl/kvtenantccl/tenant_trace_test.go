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
	gosql "database/sql"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
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

	getTrace := func(t *testing.T, db *gosql.DB) [][]string {
		runner := sqlutils.MakeSQLRunner(db)
		runner.Exec(t, `CREATE TABLE kv(k STRING PRIMARY KEY, v STRING)`)
		runner.Exec(t, `
SET tracing = on;
INSERT INTO kv VALUES('k', 'v');
SELECT * FROM kv;
SET tracing = off;
`)
		sl := runner.QueryStr(t, `SELECT * FROM [ SHOW TRACE FOR SESSION ]`)
		t.Log(sqlutils.MatrixToStr(sl))
		return sl
	}

	knobs := &kvserver.StoreTestingKnobs{}
	knobs.EvalKnobs.TestingEvalFilter = func(args kvserverbase.FilterArgs) *roachpb.Error {
		log.Eventf(args.Ctx, "%v", sensitiveString)
		log.Eventf(args.Ctx, "%v", log.Safe(visibleString))
		return nil
	}
	var args base.TestClusterArgs
	args.ServerArgs.Knobs.Store = knobs
	tc := serverutils.StartNewTestCluster(t, 1, args)
	tc.Server(0).TracerI().(*tracing.Tracer).SetRedactable(redactable)
	defer tc.Stopper().Stop(ctx)

	// Queries from the system tenant will receive unredacted traces
	// since the tracer will not have the redactable flag set.
	t.Run("system-tenant", func(t *testing.T) {
		db := tc.ServerConn(0)
		defer db.Close()
		results := getTrace(t, db)

		var found bool
		for _, sl := range results {
			for _, s := range sl {
				if strings.Contains(s, sensitiveString) {
					found = true
				}
			}
		}
		require.True(t, found, "did not find '%q' in trace:\n%s",
			sensitiveString, sqlutils.MatrixToStr(results),
		)
	})

	t.Run("regular-tenant", func(t *testing.T) {
		_, tenDB := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
			TenantID: roachpb.MakeTenantID(security.EmbeddedTenantIDs()[0]),
		})
		defer tenDB.Close()
		results := getTrace(t, tenDB)

		var found bool
		var foundRedactedMarker bool
		for _, sl := range results {
			for _, s := range sl {
				if strings.Contains(s, sensitiveString) {
					t.Fatalf(
						"trace for tenant contained KV-level trace message '%q':\n%s",
						sensitiveString, sqlutils.MatrixToStr(results),
					)
				}
				if strings.Contains(s, visibleString) {
					found = true
				}
				if strings.Contains(s, string(server.TraceRedactedMarker)) {
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
				visibleString, sqlutils.MatrixToStr(results))
			require.False(t, foundRedactedMarker, "unexpectedly found '%q':\n%s",
				string(server.TraceRedactedMarker), sqlutils.MatrixToStr(results))
		} else {
			// Otherwise, expect the opposite: not even safe information makes it through,
			// because it gets replaced with foundRedactedMarker.
			require.False(t, found, "unexpectedly saw message '%q':\n%s",
				visibleString, sqlutils.MatrixToStr(results))
			require.False(t, foundRedactedMarker, "unexpectedly found '%q':\n%s",
				string(server.TraceRedactedMarker), sqlutils.MatrixToStr(results))
		}
	})
}
