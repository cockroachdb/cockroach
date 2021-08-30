// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestVersionGating(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.Server = &server.TestingKnobs{
		DisableAutomaticVersionUpgrade: 1,
		BinaryVersionOverride:          clusterversion.ByKey(clusterversion.SQLStatsCompactionScheduledJob - 1),
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: params,
	})

	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0 /* idx */))
	sqlDB.CheckQueryResults(t,
		"SELECT count(*) FROM [SHOW SCHEDULES FOR SQL STATISTICS]",
		[][]string{{"0"}})

	sqlDB.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.SQLStatsCompactionScheduledJob).String())

	// Change the recurrence cluster setting to force job monitor to create the
	// sql stats compaction schedule.
	sqlDB.Exec(t, "SET CLUSTER SETTING sql.stats.cleanup.recurrence = '@daily'")

	// Wait for the change to be picked up by the job monitor.
	sqlDB.CheckQueryResultsRetry(t,
		"SELECT recurrence FROM [SHOW SCHEDULES FOR SQL STATISTICS]",
		[][]string{{"@daily"}})

	sqlDB.CheckQueryResults(t,
		"SELECT count(*) FROM  [SHOW SCHEDULES FOR SQL STATISTICS]",
		[][]string{{"1"}})
}
