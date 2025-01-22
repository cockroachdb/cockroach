// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"cmp"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"slices"
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
)

func registerCDCFiltering(r registry.Registry) {
	// ignoreFiltering configures whether a test should set the
	// ignore_disable_changefeed_replication option on the changefeed.
	for _, ignoreFiltering := range []bool{false, true} {
		r.Add(registry.TestSpec{
			Name:             fmt.Sprintf("cdc/filtering/session/ignore-filtering=%t", ignoreFiltering),
			Owner:            registry.OwnerCDC,
			Cluster:          r.MakeClusterSpec(3),
			CompatibleClouds: registry.AllClouds,
			Suites:           registry.Suites(registry.Nightly),
			Run:              runCDCSessionFiltering(ignoreFiltering),
		})
		r.Add(registry.TestSpec{
			Name:             fmt.Sprintf("cdc/filtering/ttl/cluster/ignore-filtering=%t", ignoreFiltering),
			Owner:            registry.OwnerCDC,
			Cluster:          r.MakeClusterSpec(3),
			CompatibleClouds: registry.AllClouds,
			Suites:           registry.Suites(registry.Nightly),
			Run:              runCDCTTLFiltering(ttlFilteringClusterSetting, ignoreFiltering),
		})
		r.Add(registry.TestSpec{
			Name:             fmt.Sprintf("cdc/filtering/ttl/table/ignore-filtering=%t", ignoreFiltering),
			Owner:            registry.OwnerCDC,
			Cluster:          r.MakeClusterSpec(3),
			CompatibleClouds: registry.AllClouds,
			Suites:           registry.Suites(registry.Nightly),
			Run:              runCDCTTLFiltering(ttlFilteringTableStorageParam, ignoreFiltering),
		})
	}
}

func runCDCSessionFiltering(
	ignoreFiltering bool,
) func(ctx context.Context, t test.Test, c cluster.Cluster) {
	return func(ctx context.Context, t test.Test, c cluster.Cluster) {
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

		type state struct {
			ID       string `json:"id"`
			Revision int    `json:"revision"`
		}
		stateToString := func(s state) string {
			return fmt.Sprintf("%s@%d", s.ID, s.Revision)
		}
		eventCollector := newEventCollector(stateToString)

		// Create a changefeed over the table, with a nodelocal sink on n1. Capture
		// the job ID so that we can wait on it, below.
		t.Status("creating changefeed")
		createChangefeedStmt := `CREATE CHANGEFEED FOR TABLE events
INTO 'nodelocal://1/events'
WITH updated, resolved, diff, min_checkpoint_frequency='1s'`
		if ignoreFiltering {
			createChangefeedStmt += `, ignore_disable_changefeed_replication`
		}
		var jobID int
		require.NoError(t, conn.QueryRowContext(ctx, createChangefeedStmt).Scan(&jobID))

		// The event table initially contains A@1, B@1, C@1.
		tx, err := conn.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO events VALUES ('A', 1);
INSERT INTO events VALUES ('B', 1);
INSERT INTO events VALUES ('C', 1);`)
		require.NoError(t, err)
		err = tx.Commit()
		require.NoError(t, err)
		eventCollector.recordNewState("A", &state{ID: "A", Revision: 1}, false /* shouldFilter */)
		eventCollector.recordNewState("B", &state{ID: "B", Revision: 1}, false /* shouldFilter */)
		eventCollector.recordNewState("C", &state{ID: "C", Revision: 1}, false /* shouldFilter */)

		type disableReplicationMode bool
		const (
			disableReplicationOff disableReplicationMode = false
			disableReplicationOn  disableReplicationMode = true
		)
		execWithChangefeedReplication := func(conn *gosql.DB, r disableReplicationMode, stmts []string) {
			_, err = conn.Exec(fmt.Sprintf("SET disable_changefeed_replication = %v;", r))
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
		execWithChangefeedReplication(s1, disableReplicationOn, []string{
			"UPDATE events SET revision = 2 WHERE id = 'A';",
		})
		eventCollector.recordNewState("A", &state{ID: "A", Revision: 2}, !ignoreFiltering)
		execWithChangefeedReplication(s1, disableReplicationOff, []string{
			"UPDATE events SET revision = 2 WHERE id = 'B';",
			"UPDATE events SET revision = 2 WHERE id = 'C';",
		})
		eventCollector.recordNewState("B", &state{ID: "B", Revision: 2}, false /* shouldFilter */)
		eventCollector.recordNewState("C", &state{ID: "C", Revision: 2}, false /* shouldFilter */)

		// Session 2:
		// - Disable replication in the current session.
		// - Update A -> @3. We expect that this event WILL NOT be in the sink.
		// - Update B -> @3. We expect that this event WILL NOT be in the sink.
		// - Re-enable filtering in the current session.
		// - Update C -> @3. We expect that this event WILL be in the sink.
		s2, err := c.ConnE(ctx, t.L(), 1)
		require.NoError(t, err)
		defer s2.Close()
		execWithChangefeedReplication(s2, disableReplicationOn, []string{
			"UPDATE events SET revision = 3 WHERE id = 'A';",
			"UPDATE events SET revision = 3 WHERE id = 'B';",
		})
		eventCollector.recordNewState("A", &state{ID: "A", Revision: 3}, !ignoreFiltering)
		eventCollector.recordNewState("B", &state{ID: "B", Revision: 3}, !ignoreFiltering)
		execWithChangefeedReplication(s2, disableReplicationOff, []string{
			"UPDATE events SET revision = 3 WHERE id = 'C';",
		})
		eventCollector.recordNewState("C", &state{ID: "C", Revision: 3}, false /* shouldFilter */)

		// Session 3:
		// - Update A -> @4. We expect that this event WILL be in the sink, and its
		// diff value will reference the previous value, which did not appear in the
		// changefeed.
		s3, err := c.ConnE(ctx, t.L(), 1)
		require.NoError(t, err)
		defer s3.Close()
		execWithChangefeedReplication(s2, disableReplicationOff, []string{
			"UPDATE events SET revision = 4 WHERE id = 'A';",
		})
		eventCollector.recordNewState("A", &state{ID: "A", Revision: 4}, false /* shouldFilter */)

		// Session 4:
		// - Insert D@1. We expect this event WILL be in the sink.
		s4, err := c.ConnE(ctx, t.L(), 1)
		require.NoError(t, err)
		defer s4.Close()
		execWithChangefeedReplication(s4, disableReplicationOff, []string{
			"INSERT INTO events VALUES ('D', 1);",
		})
		eventCollector.recordNewState("D", &state{ID: "D", Revision: 1}, false /* shouldFilter */)

		// Session 5:
		// - Delete D. We expect this event WILL NOT be in the sink.
		s5, err := c.ConnE(ctx, t.L(), 1)
		require.NoError(t, err)
		defer s5.Close()
		execWithChangefeedReplication(s5, disableReplicationOn, []string{
			"DELETE FROM events WHERE id = 'D'",
		})
		eventCollector.recordNewState("D", nil, !ignoreFiltering)

		expectedEvents := eventCollector.collectedEvents
		t.Status(fmt.Sprintf("expected events: %#v", expectedEvents))
		err = checkCDCEvents[state](ctx, t, c, conn, jobID, "events", stateToString, expectedEvents)
		require.NoError(t, err)
	}
}

// ttlFilteringType is the type of TTL filtering to use in the test function
// returned by runCDCTTLFiltering.
type ttlFilteringType bool

const (
	// ttlFilteringClusterSetting denotes TTL filtering enabled via setting the
	// sql.ttl.changefeed_replication.disabled cluster setting.
	ttlFilteringClusterSetting ttlFilteringType = false
	// ttlFilteringTableStorageParam denotes TTL filtering enabled via setting the
	// ttl_disable_changefeed_replication storage parameter on the table.
	ttlFilteringTableStorageParam ttlFilteringType = true
)

func runCDCTTLFiltering(
	filteringType ttlFilteringType, ignoreFiltering bool,
) func(ctx context.Context, t test.Test, c cluster.Cluster) {
	return func(ctx context.Context, t test.Test, c cluster.Cluster) {
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

		type state struct {
			ID        string `json:"id"`
			ExpiredAt string `json:"expired_at"`
		}
		stateToString := func(s state) string {
			return fmt.Sprintf("%s@%s", s.ID, s.ExpiredAt)
		}
		eventCollector := newEventCollector(stateToString)

		t.Status("creating changefeed")
		createChangefeedStmt := `CREATE CHANGEFEED FOR TABLE events
INTO 'nodelocal://1/events'
WITH updated, resolved, diff, min_checkpoint_frequency='1s'`
		if ignoreFiltering {
			createChangefeedStmt += `, ignore_disable_changefeed_replication`
		}
		var jobID int
		err = conn.QueryRowContext(ctx, createChangefeedStmt).Scan(&jobID)
		require.NoError(t, err)

		const (
			expiredTime    = "2000-01-01T00:00:00Z"
			notExpiredTime = "2200-01-01T00:00:00Z"
		)

		t.Status("insert initial table data")
		_, err = conn.Exec(`INSERT INTO events VALUES ('A', $1), ('B', $2)`, expiredTime, notExpiredTime)
		require.NoError(t, err)
		eventCollector.recordNewState("A", &state{ID: "A", ExpiredAt: expiredTime}, false /* shouldFilter */)
		eventCollector.recordNewState("B", &state{ID: "B", ExpiredAt: notExpiredTime}, false /* shouldFilter */)

		t.Status("wait for TTL to run and delete rows")
		err = waitForTTL(ctx, conn, "defaultdb.public.events", timeutil.Now())
		require.NoError(t, err)

		t.Status("check that rows are deleted")
		var countA int
		err = conn.QueryRow(`SELECT count(*) FROM events WHERE id = 'A'`).Scan(&countA)
		require.NoError(t, err)
		require.Equal(t, countA, 0)
		eventCollector.recordNewState("A", nil, false /* shouldFilter */)

		t.Status("enable TTL filtering")
		switch filteringType {
		case ttlFilteringClusterSetting:
			t.Status("set sql.ttl.changefeed_replication.disabled cluster setting to true")
			_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING sql.ttl.changefeed_replication.disabled = true`)
			require.NoError(t, err)
		case ttlFilteringTableStorageParam:
			t.Status("set ttl_disable_changefeed_replication storage param on events table to true")
			_, err := conn.ExecContext(ctx, `ALTER TABLE events SET (ttl_disable_changefeed_replication = true)`)
			require.NoError(t, err)
		default:
			panic("unknown TTL filtering type")
		}

		t.Status("update remaining rows to be expired")
		_, err = conn.Exec(`UPDATE events SET expired_at = $1 WHERE id = 'B'`, expiredTime)
		require.NoError(t, err)
		eventCollector.recordNewState("B", &state{ID: "B", ExpiredAt: expiredTime}, false /* shouldFilter */)

		t.Status("wait for TTL to run and delete rows")
		err = waitForTTL(ctx, conn, "defaultdb.public.events", timeutil.Now().Add(time.Minute))
		require.NoError(t, err)

		t.Status("check that rows are deleted")
		var countB int
		err = conn.QueryRow(`SELECT count(*) FROM events WHERE id = 'B'`).Scan(&countB)
		require.NoError(t, err)
		require.Equal(t, countB, 0)
		eventCollector.recordNewState("B", nil, !ignoreFiltering)

		expectedEvents := eventCollector.collectedEvents
		t.Status(fmt.Sprintf("expected events: %#v", expectedEvents))
		err = checkCDCEvents[state](ctx, t, c, conn, jobID, "events", stateToString, expectedEvents)
		require.NoError(t, err)
	}
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

// stateToStringFunc is a function that returns a string representation for a state.
type stateToStringFunc[S any] func(S) string

// eventCollector collects a slice of expected events for a map of ids to states.
type eventCollector[S any] struct {
	states          map[string]*S
	stateToString   stateToStringFunc[S]
	collectedEvents []string
}

func newEventCollector[S any](stateToString stateToStringFunc[S]) *eventCollector[S] {
	return &eventCollector[S]{
		states:        make(map[string]*S),
		stateToString: stateToString,
	}
}

func (ec *eventCollector[S]) recordNewState(id string, after *S, shouldFilter bool) {
	before := ec.states[id]
	if !shouldFilter {
		ec.collectedEvents = append(ec.collectedEvents, makeEventString(before, after, ec.stateToString))
	}
	ec.states[id] = after
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
	stateToString stateToStringFunc[S],
	expectedEvents []string,
) error {
	// Wait for the changefeed to reach the current time.
	t.Status("waiting for changefeed")
	now := timeutil.Now()
	t.L().Printf("waiting for changefeed watermark to reach current time (%s)",
		now.Format(time.RFC3339))
	_, err := waitForChangefeed(ctx, conn, jobID, t.L(), func(info changefeedInfo) (bool, error) {
		switch jobs.State(info.status) {
		case jobs.StatePending, jobs.StateRunning:
			return info.GetHighWater().After(now), nil
		default:
			return false, errors.Errorf("unexpected changefeed status %s", info.status)
		}
	})
	require.NoError(t, err)

	// Collect the events from the file-based sink on n1.
	cmd := fmt.Sprintf("find {store-dir}/extern/%s -name '*.ndjson' | xargs cat", nodeLocalSinkDir)
	d, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(1)), cmd)
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
	slices.SortFunc(events, func(a, b changefeedSinkEvent[S]) int {
		idA, idB := a.Key[0], b.Key[0]
		tsA, err := hlc.ParseHLC(a.Updated)
		require.NoError(t, err)
		tsB, err := hlc.ParseHLC(b.Updated)
		require.NoError(t, err)
		return cmp.Or(
			tsA.Compare(tsB),
			cmp.Compare(idA, idB),
		)
	})

	// Convert actual events to strings and compare to expected events.
	var actualEvents []string
	for _, e := range events {
		actualEvents = append(actualEvents, makeEventString(e.Before, e.After, stateToString))
	}
	// We remove duplicates since the at-least-once delivery guarantee permits
	// the same event to be emitted multiple times.
	actualEvents = slices.Compact(actualEvents)
	require.Equal(t, expectedEvents, actualEvents)

	return nil
}

// makeEventString produces a canonical event string that we can assert on
// given a before and after state.
func makeEventString[S any](before *S, after *S, stateToString stateToStringFunc[S]) string {
	var s string
	if after == nil {
		s += "<deleted>"
	} else {
		s += stateToString(*after)
	}
	if before != nil {
		s += fmt.Sprintf(" (before: %s)", stateToString(*before))
	}
	return s
}
