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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestTenantTracesAreRedacted is an end-to-end version of
// `kvserver.TestMaybeRedactRecording`.
func TestTenantTracesAreRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
	defer tc.Stopper().Stop(ctx)

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
			}
		}
		// TODO(obs-inf): when traces are properly redactable, `visibleString` should make
		// it into the recording.
		// require.True(t, found, "trace for tenant missing trace message '%q':\n%s",
		// 	visibleString, sqlutils.MatrixToStr(results))
		_ = found
	})
}
