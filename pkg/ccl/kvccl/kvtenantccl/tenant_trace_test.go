// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

func TestTenantCannotGetVerboseTraces(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	const magicString = "magic91295919"

	getTrace := func(t *testing.T, db *gosql.DB) [][]string {
		runner := sqlutils.MakeSQLRunner(db)
		runner.Exec(t, `CREATE TABLE kv(k STRING PRIMARY KEY, v STRING)`)
		runner.Exec(t, `
SET tracing = on;
INSERT INTO kv VALUES('k', 'v');
SELECT * FROM kv;
SET tracing = off;
`)
		return runner.QueryStr(t, `SHOW TRACE FOR SESSION`)
	}

	knobs := &kvserver.StoreTestingKnobs{}
	knobs.EvalKnobs.TestingEvalFilter = func(args kvserverbase.FilterArgs) *roachpb.Error {
		log.Eventf(args.Ctx, magicString)
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
				if strings.Contains(s, magicString) {
					found = true
				}
			}
		}
		require.True(t, found, "did not find '%q' in trace:\n%s",
			magicString, sqlutils.MatrixToStr(results),
		)
	})

	t.Run("regular-tenant", func(t *testing.T) {
		_, tenDB := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
			TenantID: roachpb.MakeTenantID(security.EmbeddedTenantIDs()[0]),
		})
		defer tenDB.Close()
		results := getTrace(t, tenDB)

		for _, sl := range results {
			for _, s := range sl {
				if strings.Contains(s, magicString) {
					t.Fatalf(
						"trace for tenant contained KV-level trace message '%q':\n%s",
						magicString, sqlutils.MatrixToStr(results),
					)
				}
			}
		}
	})
}
