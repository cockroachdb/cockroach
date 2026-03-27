// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrtestutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTransactionalModeUnimplemented(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestDoesNotWorkWithExternalProcessMode(134857),
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}

	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	s := tc.Server(0).ApplicationLayer()

	for _, dbName := range []string{"a", "b"} {
		_, err := tc.Conns[0].Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
		require.NoError(t, err)
	}

	dbA := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("a")))
	dbB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("b")))
	sysDB := sqlutils.MakeSQLRunner(tc.SystemLayer(0).SQLConn(t))
	ldrtestutils.ApplyLowLatencyReplicationSettings(t, sysDB, dbA)

	dbA.Exec(t, "CREATE TABLE tab (pk INT PRIMARY KEY, payload STRING)")
	dbB.Exec(t, "CREATE TABLE tab (pk INT PRIMARY KEY, payload STRING)")

	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

	var jobID jobspb.JobID
	dbA.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH MODE = 'transactional'",
		dbBURL.String(),
	).Scan(&jobID)

	jobutils.WaitForJobToFail(t, dbA, jobID)
	payload := jobutils.GetJobPayload(t, dbA, jobID)
	require.Contains(t, payload.Error, "transactional replication mode is not yet implemented")
}
