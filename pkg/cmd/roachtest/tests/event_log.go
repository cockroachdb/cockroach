// Copyright 2018 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

func runEventLog(ctx context.Context, t test.Test, c cluster.Cluster) {
	type nodeEventInfo struct {
		NodeID roachpb.NodeID
	}

	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx)

	// Verify that "node joined" and "node restart" events are recorded whenever
	// a node starts and contacts the cluster.
	db := c.Conn(ctx, 1)
	defer db.Close()
	WaitFor3XReplication(t, db)

	err := retry.ForDuration(10*time.Second, func() error {
		rows, err := db.Query(
			`SELECT "targetID", info FROM system.eventlog WHERE "eventType" = 'node_join'`,
		)
		if err != nil {
			t.Fatal(err)
		}
		seenIds := make(map[int64]struct{})
		for rows.Next() {
			var targetID int64
			var infoStr gosql.NullString
			if err := rows.Scan(&targetID, &infoStr); err != nil {
				t.Fatal(err)
			}

			// Verify the stored node descriptor.
			if !infoStr.Valid {
				t.Fatalf("info not recorded for node join, target node %d", targetID)
			}
			var info nodeEventInfo
			if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
				t.Fatal(err)
			}
			if a, e := int64(info.NodeID), targetID; a != e {
				t.Fatalf("Node join with targetID %d had descriptor for wrong node %d", e, a)
			}

			// Verify that all NodeIDs are different.
			if _, ok := seenIds[targetID]; ok {
				t.Fatalf("Node ID %d seen in two different node join messages", targetID)
			}
			seenIds[targetID] = struct{}{}
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
	c.Stop(ctx, c.Node(3))
	c.Start(ctx, c.Node(3))

	err = retry.ForDuration(10*time.Second, func() error {
		// Query all node restart events. There should only be one.
		rows, err := db.Query(
			`SELECT "targetID", info FROM system.eventlog WHERE "eventType" = 'node_restart'`,
		)
		if err != nil {
			return err
		}

		seenCount := 0
		for rows.Next() {
			var targetID int64
			var infoStr gosql.NullString
			if err := rows.Scan(&targetID, &infoStr); err != nil {
				return err
			}

			// Verify the stored node descriptor.
			if !infoStr.Valid {
				t.Fatalf("info not recorded for node join, target node %d", targetID)
			}
			var info nodeEventInfo
			if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
				t.Fatal(err)
			}
			if a, e := int64(info.NodeID), targetID; a != e {
				t.Fatalf("node join with targetID %d had descriptor for wrong node %d", e, a)
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
