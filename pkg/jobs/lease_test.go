// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestJobsTableSqllivenessFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	var table, schema string
	sqlutils.MakeSQLRunner(db).QueryRow(t, `SHOW CREATE system.jobs`).Scan(&table, &schema)
	if !strings.Contains(schema, `FAMILY sqlliveness (sqlliveness_name, sqlliveness_epoch)`) {
		t.Fatalf("expected sqlliveness family, got %q", schema)
	}
}

func TestJobLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("normal success", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()
		cm := rts.registry.GetClaimManager()

		j, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		require.NoError(t, err)
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		// At this point job is running; resume is blocked waiting on rts.resumeCh.
		claim, err := cm.CreateClaim(context.Background(), "nodeID", time.Minute)
		require.NoError(t, err)
		_ = rts.sqlDB.Query(
			t, `UPDATE system.jobs SET sqlliveness_name = $1, sqlliveness_epoch = $2 WHERE id = $3`,
			claim.Name, claim.Epoch, *j.ID(),
		)

		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true
		rts.check(t, jobs.StatusSucceeded)

		var Name string
		var Epoch int64
		const stmt = "SELECT sqlliveness_name, sqlliveness_epoch FROM system.jobs WHERE id = $1"
		rts.sqlDB.QueryRow(t, stmt, *j.ID()).Scan(&Name, &Epoch)

		require.Equal(t, claim.Name, Name)
		require.Equal(t, claim.Epoch, Epoch)
	})
}
