// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingtest"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestPartitionedStreamReplicationClient(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer log.Scope(t).Close(t)
	h, cleanup := streamingtest.NewReplicationHelper(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer cleanup()

	ctx := context.Background()
	// Makes sure source cluster producer job does not time out within test timeout
	h.SysDB.Exec(t, "SET CLUSTER SETTING stream_replication.job_liveness_timeout = '500s'")
	h.Tenant.SQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
CREATE TABLE d.t2(i int primary key);
INSERT INTO d.t1 (i) VALUES (42);
INSERT INTO d.t2 VALUES (2);
`)

	client := &partitionedStreamClient{remote: &h.PGUrl}

	id, err := client.Create(ctx, h.Tenant.ID)
	require.NoError(t, err)
	// We can create multiple replication streams for the same tenant.
	_, err = client.Create(ctx, h.Tenant.ID)
	require.NoError(t, err)

	top, err := client.Plan(ctx, id)
	require.NoError(t, err)
	require.Equal(t, 1, len(top))
	// Plan for a non-existent stream
	_, err = client.Plan(ctx, 999)
	require.Errorf(t, err, "Replication stream %d not found", 999)

	// TODO(casper): add tests for client.Subscribe()
	h.SysDB.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", id),
		[][]string{{"running"}})
	require.NoError(t, client.Heartbeat(ctx, id, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}))

	// Pause the underlying producer job of the replication stream
	h.SysDB.Exec(t, `PAUSE JOB $1`, id)
	h.SysDB.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", id),
		[][]string{{"paused"}})
	require.Errorf(t, client.Heartbeat(ctx, id, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}),
		"Replication stream %d is paused in the source cluster", id)

	// Cancel the underlying producer job of the replication stream
	h.SysDB.Exec(t, `CANCEL JOB $1`, id)
	h.SysDB.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", id),
		[][]string{{"canceled"}})
	require.Errorf(t, client.Heartbeat(ctx, id, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}),
		"Replication stream %d is not active in the source cluster", id)

	// Non-existent stream is not active in the source cluster.
	require.Errorf(t, client.Heartbeat(ctx, 999, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}),
		"Replication stream %d not found", 999)
}
