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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations/leasemanager"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestJobsTableLeaseFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	var table, schema string
	sqlutils.MakeSQLRunner(db).QueryRow(t, `SHOW CREATE system.jobs`).Scan(&table, &schema)
	if !strings.Contains(schema, `FAMILY lease (lease)`) {
		t.Fatalf("expected lease family, got %q", schema)
	}
}

func TestJobLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("normal success", func(t *testing.T) {
		rts := registryTestSuite{}
		rts.setUp(t)
		defer rts.tearDown()

		j, _, err := rts.registry.CreateAndStartJob(rts.ctx, nil, rts.mockJob)
		require.NoError(t, err)
		rts.job = j

		rts.mu.e.ResumeStart = true
		rts.resumeCheckCh <- struct{}{}
		rts.check(t, jobs.StatusRunning)

		// At this point the job is running - the resume is blocked waiting on
		// rts.resumeCh.
		lease, err := rts.registry.AcquireLease(context.Background(), *j.ID())
		require.NoError(t, err)

		var buf []byte
		var id int64
		const stmt = "SELECT id, lease FROM system.jobs WHERE id = $1"
		rts.sqlDB.QueryRow(t, stmt, *j.ID()).Scan(&id, &buf)

		require.Equal(t, *j.ID(), id)
		require.Greater(t, len(buf), 0)

		var leaseVal leasemanager.LeaseVal
		require.NoError(t, protoutil.Unmarshal(buf, &leaseVal))
		require.Equal(t, *lease.GetLeaseVal(), leaseVal)

		rts.resumeCh <- nil
		rts.mu.e.ResumeExit++
		rts.mu.e.Success = true
		rts.check(t, jobs.StatusSucceeded)
	})
}
