// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func registerCDCFiltering(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "cdc/filtering/session",
		Owner:            registry.OwnerCDC,
		Cluster:          r.MakeClusterSpec(3),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		RequiresLicense:  true,
		Run:              runCDCSessionFiltering,
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/filtering/ttl",
		Owner:            registry.OwnerCDC,
		Cluster:          r.MakeClusterSpec(3),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		RequiresLicense:  true,
		Run:              runCDCTTLFiltering,
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

	// Session 4:
	// - Insert D@1. We expect this event WILL be in the sink.
	s4, err := c.ConnE(ctx, t.L(), 1)
	require.NoError(t, err)
	defer s4.Close()
	execWithChangefeedReplication(s4, replicationEnabled, []string{
		"INSERT INTO events VALUES ('D', 1);",
	})

	// Session 5:
	// - Delete D. We expect this event WILL NOT be in the sink.
	s5, err := c.ConnE(ctx, t.L(), 1)
	require.NoError(t, err)
	defer s5.Close()
	execWithChangefeedReplication(s5, replicationDisabled, []string{
		"DELETE FROM events WHERE id = 'D'",
	})

	// We expect to see the following sequence of events:
	expectedEvents := []string{
		// initial
		"A@1", "B@1", "C@1",
		// session 1
		"B@2 (before: B@1)", "C@2 (before: C@1)",
		// session 2
		"C@3 (before: C@2)",
		// session 3
		"A@4 (before: A@3)",
		// session 4
		"D@1",
		// session 5 (no events)
	}
	type state struct {
		ID       string `json:"id"`
		Revision int    `json:"revision"`
	}
	err = checkCDCEvents[state](ctx, t, c, conn, jobID, "events",
		// Produce a canonical format that we can assert on. The format is of the
		// form: id@rev (before: id@rev)[, id@rev (before: id@rev), ...]
		func(before *state, after *state) string {
			var s string
			if after == nil {
				s += "<deleted>"
			} else {
				s += fmt.Sprintf("%s@%d", after.ID, after.Revision)
			}
			if before != nil {
				s += fmt.Sprintf(" (before: %s@%d)", before.ID, before.Revision)
			}
			return s
		},
		expectedEvents,
	)
	require.NoError(t, err)
}

type changefeedSinkEvent[S any] struct {
	After   *S       `json:"after"`
	Before  *S       `json:"before"`
	Key     []string `json:"key"`
	Updated string   `json:"updated"`
}

func checkCDCEvents[S any](
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	conn *gosql.DB,
	jobID int,
	nodeLocalSinkDir string,
	eventToString func(before *S, after *S) string,
	expectedEvents []string,
) error {
	// Wait for the changefeed to reach the current time.
	t.Status("waiting for changefeed")
	now := timeutil.Now()
	t.L().Printf("waiting for changefeed watermark to reach current time (%s)",
		now.Format(time.RFC3339))
	_, err := waitForChangefeed(ctx, conn, jobID, t.L(), func(info changefeedInfo) (bool, error) {
		switch jobs.Status(info.status) {
		case jobs.StatusPending, jobs.StatusRunning:
			return info.highwaterTime.After(now), nil
		default:
			return false, errors.Errorf("unexpected changefeed status %s", info.status)
		}
	})
	require.NoError(t, err)

	// Collect the events from the file-based sink on n1.
	cmd := fmt.Sprintf("find {store-dir}/extern/%s -name '*.ndjson' | xargs cat", nodeLocalSinkDir)
	d, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(1), cmd)
	require.NoError(t, err)

	var events []changefeedSinkEvent[S]
	for _, line := range strings.Split(d.Stdout, "\n") {
		// Skip empty lines.
		if line == "" {
			continue
		}
		var e changefeedSinkEvent[S]
		require.NoError(t, json.Unmarshal([]byte(line), &e))
		events = append(events, e)
	}

	// Sort the events by (updated, id) to yield a total ordering.
	slices.SortFunc(events, func(a, b changefeedSinkEvent[S]) bool {
		idA, idB := a.Key[0], b.Key[0]
		tsA, err := hlc.ParseHLC(a.Updated)
		require.NoError(t, err)
		tsB, err := hlc.ParseHLC(b.Updated)
		require.NoError(t, err)
		if tsA.Equal(tsB) {
			return idA < idB
		}
		return tsA.Less(tsB)
	})

	// Convert actual events to strings and compare to expected events.
	var actualEvents []string
	for _, e := range events {
		actualEvents = append(actualEvents, eventToString(e.Before, e.After))
	}
	// We remove duplicates since the at-least-once delivery guarantee permits
	// the same event to be emitted multiple times.
	actualEvents = slices.Compact(actualEvents)
	require.Equal(t, expectedEvents, actualEvents)

	return nil
}

func runCDCTTLFiltering(ctx context.Context, t test.Test, c cluster.Cluster) {
	t.Status("starting cluster")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	// kv.rangefeed.enabled is required for changefeeds to run
	_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	require.NoError(t, err)

	t.Status("creating table with TTL")
	_, err = conn.ExecContext(ctx, `CREATE TABLE events (
	id STRING PRIMARY KEY,
	expired_at TIMESTAMPTZ
) WITH (ttl_expiration_expression = 'expired_at', ttl_job_cron = '* * * * *')`)
	require.NoError(t, err)

	t.Status("creating changefeed")
	var jobID int
	err = conn.QueryRowContext(ctx, `CREATE CHANGEFEED FOR TABLE events
INTO 'nodelocal://1/events'
WITH diff, updated, min_checkpoint_frequency = '1s'`).Scan(&jobID)
	require.NoError(t, err)

	const (
		expiredTime    = "2000-01-01"
		notExpiredTime = "2200-01-01"
	)

	t.Status("insert initial table data")
	_, err = conn.Exec(`INSERT INTO events VALUES ('A', $1), ('B', $2)`, expiredTime, notExpiredTime)
	require.NoError(t, err)

	t.Status("wait for TTL to run and delete rows")
	err = waitForTTL(ctx, conn, "defaultdb.public.events", timeutil.Now())
	require.NoError(t, err)

	t.Status("check that rows are deleted")
	var countA int
	err = conn.QueryRow(`SELECT count(*) FROM events WHERE id = 'A'`).Scan(&countA)
	require.NoError(t, err)
	require.Equal(t, countA, 0)

	t.Status("set sql.ttl.changefeed_replication.disabled")
	_, err = conn.ExecContext(ctx, `SET CLUSTER SETTING sql.ttl.changefeed_replication.disabled = true`)
	require.NoError(t, err)

	t.Status("update remaining rows to be expired")
	_, err = conn.Exec(`UPDATE events SET expired_at = $1 WHERE id = 'B'`, expiredTime)
	require.NoError(t, err)

	t.Status("wait for TTL to run and delete rows")
	err = waitForTTL(ctx, conn, "defaultdb.public.events", timeutil.Now().Add(time.Minute))
	require.NoError(t, err)

	t.Status("check that rows are deleted")
	var countB int
	err = conn.QueryRow(`SELECT count(*) FROM events WHERE id = 'B'`).Scan(&countB)
	require.NoError(t, err)
	require.Equal(t, countB, 0)

	expectedEvents := []string{
		// initial
		"A@2000-01-01T00:00:00Z", "B@2200-01-01T00:00:00Z",
		// TTL deletes A
		"<deleted> (before: A@2000-01-01T00:00:00Z)",
		// update B to be expired
		"B@2000-01-01T00:00:00Z (before: B@2200-01-01T00:00:00Z)",
		// TTL deletes B (no events)
	}
	type state struct {
		ID        string `json:"id"`
		ExpiredAt string `json:"expired_at"`
	}
	err = checkCDCEvents[state](ctx, t, c, conn, jobID, "events",
		// Produce a canonical format that we can assert on. The format is of the
		// form: id@exp_at (before: id@exp_at)[, id@exp_at (before: id@exp_at), ...]
		func(before *state, after *state) string {
			var s string
			if after == nil {
				s += "<deleted>"
			} else {
				s += fmt.Sprintf("%s@%s", after.ID, after.ExpiredAt)
			}
			if before != nil {
				s += fmt.Sprintf(" (before: %s@%s)", before.ID, before.ExpiredAt)
			}
			return s
		},
		expectedEvents,
	)
	require.NoError(t, err)
}

// waitForTTL waits until the row-level TTL job for a given table has run
// and succeeded at least once after the specified time.
func waitForTTL(ctx context.Context, conn *gosql.DB, table string, t time.Time) error {
	retryOpts := retry.Options{
		InitialBackoff: 1 * time.Minute,
		MaxBackoff:     5 * time.Minute,
	}
	// We add an extra buffer to account for the TTL job's AOST duration.
	minJobCreateTime := t.Add(-2 * ttlbase.DefaultAOSTDuration)
	return retry.WithMaxAttempts(ctx, retryOpts, 5, func() error {
		var count int
		err := conn.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT count(*) FROM [SHOW JOBS]
WHERE description ILIKE 'ttl for %s%%'
AND status = 'succeeded'
AND created > $1`, table),
			minJobCreateTime,
		).Scan(&count)
		if err != nil {
			return err
		}
		if count == 0 {
			return errors.Newf("ttl has not run yet")
		}
		return nil
	})
}
