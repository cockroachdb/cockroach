// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func registerCDCSessionFiltering(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "cdc-filtering",
		Owner:            registry.OwnerCDC,
		Cluster:          r.MakeClusterSpec(3),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Tags:             registry.Tags("default", "aws"),
		RequiresLicense:  true,
		Run:              runCDCSessionFiltering,
	})
}

func runCDCSessionFiltering(ctx context.Context, t test.Test, c cluster.Cluster) {
	t.Status("starting cluster")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	// kv.rangefeed.enabled is required for changefeeds to run
	_, err := conn.ExecContext(ctx, "SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)

	t.Status("creating and seeding table with initial values")
	_, err = conn.ExecContext(ctx, `CREATE TABLE events (id VARCHAR PRIMARY KEY, revision INT)`)
	require.NoError(t, err)

	// Create a changefeed over the table, with a nodelocal sink on n1. Capture
	// the job ID so that we can wait on it, below.
	t.Status("creating changefeed")
	var jobID int
	require.NoError(
		t, conn.QueryRowContext(
			ctx, `CREATE CHANGEFEED FOR TABLE events INTO 'nodelocal://1/events' WITH updated, resolved, diff, min_checkpoint_frequency='1s'`).
			Scan(&jobID))

	// The event table initially contains A@1, B@1, C@1.
	tx, err := conn.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO events VALUES ('A', 1);
INSERT INTO events VALUES ('B', 1);
INSERT INTO events VALUES ('C', 1);`)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	type replicate int
	const (
		replicationEnabled replicate = iota
		replicationDisabled
	)
	execWithChangefeedReplication := func(conn *gosql.DB, r replicate, stmts []string) {
		_, err = conn.Exec(fmt.Sprintf("SET disable_changefeed_replication = %v;", r == replicationDisabled))
		require.NoError(t, err)
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		for _, stmt := range stmts {
			_, err = tx.ExecContext(ctx, stmt)
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())
	}

	// Write a set of events to the table that will not be filtered out, as the
	// session variable is not set.
	t.Status("producing events")

	// Session 1:
	// - Disable replication in the current session.
	// - Update A -> @2. We expect that this event WILL NOT be in the sink.
	// - Re-enable filtering in the current session.
	// - Update B -> @2. We expect that this event WILL be in the sink.
	// - Update C -> @2. We expect that this event WILL be in the sink.
	s1, err := c.ConnE(ctx, t.L(), 1)
	require.NoError(t, err)
	defer s1.Close()
	execWithChangefeedReplication(s1, replicationDisabled, []string{
		"UPDATE events SET revision = 2 WHERE id = 'A';",
	})
	execWithChangefeedReplication(s1, replicationEnabled, []string{
		"UPDATE events SET revision = 2 WHERE id = 'B';",
		"UPDATE events SET revision = 2 WHERE id = 'C';",
	})

	// Session 2:
	// - Disable replication in the current session.
	// - Update A -> @3. We expect that this event WILL NOT be in the sink.
	// - Update B -> @3. We expect that this event WILL NOT be in the sink.
	// - Re-enable filtering in the current session.
	// - Update C -> @3. We expect that this event WILL be in the sink.
	s2, err := c.ConnE(ctx, t.L(), 1)
	require.NoError(t, err)
	defer s2.Close()
	execWithChangefeedReplication(s2, replicationDisabled, []string{
		"UPDATE events SET revision = 3 WHERE id = 'A';",
		"UPDATE events SET revision = 3 WHERE id = 'B';",
	})
	execWithChangefeedReplication(s2, replicationEnabled, []string{
		"UPDATE events SET revision = 3 WHERE id = 'C';",
	})

	// Session 3:
	// - Update A -> @4. We expect that this event WILL be in the sink, and its
	// diff value will reference the previous value, which did not appear in the
	// changefeed.
	s3, err := c.ConnE(ctx, t.L(), 1)
	require.NoError(t, err)
	defer s3.Close()
	execWithChangefeedReplication(s2, replicationEnabled, []string{
		"UPDATE events SET revision = 4 WHERE id = 'A';",
	})

	// Wait for the changefeed to reach the current time.
	t.Status("waiting for changefeed")
	now := timeutil.Now()
	t.L().Printf("waiting for changefeed watermark to reach current time (%s)",
		now.Format(time.RFC3339))
	_, err = waitForChangefeed(ctx, conn, jobID, t.L(), func(info changefeedInfo) (bool, error) {
		switch jobs.Status(info.status) {
		case jobs.StatusPending, jobs.StatusRunning:
			return info.highwaterTime.After(now), nil
		default:
			return false, errors.Errorf("unexpected changefeed status %s", info.status)
		}
	})
	require.NoError(t, err)

	// Collect the events from the file-based sink on n1.
	cmd := "find {store-dir}/extern/events -name '*.ndjson' | xargs cat"
	d, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(1), cmd)
	require.NoError(t, err)

	// Parse the JSON events into an internal representation.
	type event struct {
		ID       string `json:"id"`
		Revision int    `json:"revision"`
	}
	type changefeedSinkEvent struct {
		After   event    `json:"after"`
		Before  *event   `json:"before"`
		Key     []string `json:"key"`
		Updated string   `json:"updated"`
	}

	var events []changefeedSinkEvent
	for _, line := range strings.Split(d.Stdout, "\n") {
		// Skip empty lines.
		if line == "" {
			continue
		}
		var e changefeedSinkEvent
		require.NoError(t, json.Unmarshal([]byte(line), &e))
		events = append(events, e)
	}

	// Sort the events by (updated, id) to yeild a total ordering.
	sort.Slice(events, func(i, j int) bool {
		idA, idB := events[i].Key[0], events[j].Key[0]
		tsA, err := hlc.ParseHLC(events[i].Updated)
		require.NoError(t, err)
		tsB, err := hlc.ParseHLC(events[j].Updated)
		require.NoError(t, err)
		if tsA.Equal(tsB) {
			return idA < idB
		}
		return tsA.Less(tsB)
	})

	// Produce a canonical format that we can assert on. The format is of the
	// form: id@rev (before: id@rev)[, id@rev (before: id@rev), ...]
	var sb strings.Builder
	for i, e := range events {
		if i > 0 {
			sb.WriteString(", ")
		}
		var before string
		if e.Before != nil {
			before = fmt.Sprintf(" (before: %s@%d)", e.Before.ID, e.Before.Revision)
		}
		sb.WriteString(fmt.Sprintf("%s@%d%s", e.After.ID, e.After.Revision, before))
	}

	// We expect to see the following sequence:
	want := "A@1, B@1, C@1, " +
		"B@2 (before: B@1), C@2 (before: C@1), C@3 (before: C@2), A@4 (before: A@3)"
	require.Equal(t, want, sb.String())
}
