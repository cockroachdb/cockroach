// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/stretchr/testify/require"
)

func runEventLog(ctx context.Context, t test.Test, c cluster.Cluster) {
	type nodeEventInfo struct {
		NodeID roachpb.NodeID
	}

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

	// Verify that "node joined" and "node restart" events are recorded whenever
	// a node starts and contacts the cluster.
	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()
	err := roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
	require.NoError(t, err)

	err = retry.ForDuration(10*time.Second, func() error {
		rows, err := db.Query(
			`SELECT "reportingID", info FROM system.eventlog WHERE "eventType" = 'node_join'`,
		)
		if err != nil {
			t.Fatal(err)
		}
		seenIds := make(map[int64]struct{})
		for rows.Next() {
			var reportingID int64
			var infoStr gosql.NullString
			if err := rows.Scan(&reportingID, &infoStr); err != nil {
				t.Fatal(err)
			}

			// Verify the stored node descriptor.
			if !infoStr.Valid {
				t.Fatalf("info not recorded for node join, node %d", reportingID)
			}
			var info nodeEventInfo
			if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
				t.Fatal(err)
			}
			if a, e := int64(info.NodeID), reportingID; a != e {
				t.Fatalf("Node join with reportingID %d had descriptor for wrong node %d", e, a)
			}

			// Verify that all NodeIDs are different.
			if _, ok := seenIds[reportingID]; ok {
				t.Fatalf("Node ID %d seen in two different node join messages", reportingID)
			}
			seenIds[reportingID] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if c.Spec().NodeCount != len(seenIds) {
			return fmt.Errorf("expected %d node join messages, found %d: %v",
				c.Spec().NodeCount, len(seenIds), seenIds)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Stop and Start Node 3, and verify the node restart message.
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(3))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(3))

	err = retry.ForDuration(10*time.Second, func() error {
		// Query all node restart events. There should only be one.
		rows, err := db.Query(
			`SELECT "reportingID", info FROM system.eventlog WHERE "eventType" = 'node_restart'`,
		)
		if err != nil {
			return err
		}

		seenCount := 0
		for rows.Next() {
			var reportingID int64
			var infoStr gosql.NullString
			if err := rows.Scan(&reportingID, &infoStr); err != nil {
				return err
			}

			// Verify the stored node descriptor.
			if !infoStr.Valid {
				t.Fatalf("info not recorded for node join, node %d", reportingID)
			}
			var info nodeEventInfo
			if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
				t.Fatal(err)
			}
			if a, e := int64(info.NodeID), reportingID; a != e {
				t.Fatalf("node join with reportingID %d had descriptor for wrong node %d", e, a)
			}

			seenCount++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if seenCount != 1 {
			return fmt.Errorf("expected one node restart event, found %d", seenCount)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
