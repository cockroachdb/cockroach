// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestIsAtLeastVersionBuiltin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V21_2),
				},
			},
		},
	}

	var (
		ctx   = context.Background()
		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		conn  = tc.ServerConn(0)
		sqlDB = sqlutils.MakeSQLRunner(conn)
	)
	defer tc.Stopper().Stop(ctx)

	// Check that the builtin returns false when comparing against 21.2 version
	// because we are still on 21.1.
	sqlDB.CheckQueryResults(t, "SELECT crdb_internal.is_at_least_version('21.2-10')", [][]string{{"false"}})

	// Run the migration.
	sqlDB.Exec(t, "SET CLUSTER SETTING version = $1", clusterversion.ByKey(clusterversion.TraceIDDoesntImplyStructuredRecording).String())

	// It should now return true.
	sqlDB.CheckQueryResultsRetry(t, "SELECT crdb_internal.is_at_least_version('21.2-10')", [][]string{{"true"}})
}
