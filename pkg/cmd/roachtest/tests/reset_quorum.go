// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/require"
)

func runResetQuorum(ctx context.Context, t test.Test, c cluster.Cluster) {
	skip.WithIssue(t, 58165)
	args := func(attr string) option.Option {
		return option.StartArgs(
			"-a=--attrs="+attr,
			"--env=COCKROACH_SCAN_MAX_IDLE_TIME=5ms", // speed up replication
		)
	}
	// n1-n5 will be in locality A, n6-n8 in B. We'll pin a single table to B and
	// let the the nodes in B fail permanently.
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, c.Range(1, 5), args("A"))
	db := c.Conn(ctx, 1)
	defer db.Close()

	rows, err := db.QueryContext(ctx, `SELECT target FROM crdb_internal.zones`)
	require.NoError(t, err)
	for rows.Next() {
		var target string
		require.NoError(t, rows.Scan(&target))
		_, err = db.ExecContext(ctx, `ALTER `+target+` CONFIGURE ZONE USING constraints = '{"-B"}'`)
		require.NoError(t, err)
	}
	require.NoError(t, rows.Err())

	c.Start(ctx, c.Range(6, 8), args("B"))
	_, err = db.Exec(`CREATE TABLE lostrange (id INT PRIMARY KEY, v STRING)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO lostrange VALUES(1, 'foo')`)
	require.NoError(t, err)

	_, err = db.Exec(`ALTER TABLE lostrange CONFIGURE ZONE USING constraints = '{"+B"}'`)
	require.NoError(t, err)

	var lostRangeIDs map[int64]struct{} // in practice there will be just one
	for i := 0; i < 100; i++ {
		lostRangeIDs = map[int64]struct{}{}
		rows, err := db.QueryContext(ctx, `
SELECT
	*
FROM
	[
		SELECT
			range_id, table_name, unnest(replicas) AS r
		FROM
			crdb_internal.ranges_no_leases
	]
WHERE
	(r IN (6, 7, 8)) -- intentionally do not exclude lostrange (to populate lostRangeIDs)
OR
	(r NOT IN (6, 7, 8) AND table_name = 'lostrange')
`)
		require.NoError(t, err)
		var buf strings.Builder
		for rows.Next() {
			var rangeID int64
			var tableName string
			var storeID int
			require.NoError(t, rows.Scan(&rangeID, &tableName, &storeID))
			if tableName == "lostrange" && storeID >= 6 {
				lostRangeIDs[rangeID] = struct{}{}
			} else {
				fmt.Fprintf(&buf, "r%d still has a replica on s%d (table %q)\n", rangeID, storeID, tableName)
			}
		}
		require.NoError(t, rows.Err())
		if buf.Len() == 0 {
			break
		}
		t.L().Printf("still waiting:\n" + buf.String())
		time.Sleep(5 * time.Second)
	}

	require.NotEmpty(t, lostRangeIDs)

	// Now 'lostrange' is on n6-n8 and nothing else is. The nodes go down
	// permanently (the wiping prevents the test runner from failing the
	// test after it has passed - we cannot restart those nodes).
	c.Stop(ctx, c.Range(6, 8))
	c.Wipe(ctx, c.Range(6, 8))

	// Should not be able to read from it even (generously) after a lease timeout.
	_, err = db.QueryContext(ctx, `SET statement_timeout = '15s'; SELECT * FROM lostrange;`)
	require.Error(t, err)
	t.L().Printf("table is now unavailable, as planned")

	const nodeID = 1 // where to put the replica, matches node number in roachtest
	for rangeID := range lostRangeIDs {
		c.Run(ctx, c.Node(nodeID), "./cockroach", "debug", "reset-quorum",
			fmt.Sprint(rangeID), "--insecure",
		)
	}

	// Table should come back to life (though empty).
	var n int
	err = db.QueryRowContext(
		ctx, `SET statement_timeout = '120s'; SELECT count(*) FROM lostrange;`,
	).Scan(&n)
	require.NoError(t, err)
	require.Zero(t, n)

	// Replica should be on the right node (according to meta2).
	for rangeID := range lostRangeIDs {
		var actNodeID int32
		// NB: this errors if there is more than one row.
		err = db.QueryRowContext(ctx,
			`
SELECT
	r
FROM
	[
		SELECT
			range_id, unnest(replicas) AS r
		FROM
			crdb_internal.ranges_no_leases
	]
WHERE
	range_id = $1
`, rangeID).Scan(&actNodeID)
		require.NoError(t, err)
		require.EqualValues(t, nodeID, actNodeID)
	}
}
