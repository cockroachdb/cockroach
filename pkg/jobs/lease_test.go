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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestJobsTableClaimFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	var table, schema string
	sqlDB.QueryRow(t, `SHOW CREATE system.jobs`).Scan(&table, &schema)
	if !strings.Contains(
		schema, `FAMILY claim (claim_session_id, claim_instance_id, num_runs, last_run)`,
	) {
		t.Fatalf("expected claim family, got %q", schema)
	}

	now := timeutil.Now()
	_ = sqlDB.Query(t, `
INSERT INTO system.jobs (id, status, payload, claim_session_id, claim_instance_id, num_runs, last_run)
VALUES (1, 'running', '@!%$%45', 'foo', 101, 100, $1)`, now)
	var status, sessionID string
	var instanceID, numRuns int64
	var lastRun time.Time
	const stmt = "SELECT status, claim_session_id, claim_instance_id, num_runs, last_run FROM system.jobs WHERE id = $1"
	sqlDB.QueryRow(t, stmt, 1).Scan(&status, &sessionID, &instanceID, &numRuns, &lastRun)

	require.Equal(t, "running", status)
	require.Equal(t, "foo", sessionID)
	require.Equal(t, int64(101), instanceID)
	require.Equal(t, int64(100), numRuns)
	require.Equal(t, timeutil.ToUnixMicros(now), timeutil.ToUnixMicros(lastRun))
}
