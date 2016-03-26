// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package acceptance

import (
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/roachpb"
	csql "github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/uuid"
)

// TestEventLog verifies that "node joined" and "node restart" events are
// recorded whenever a node starts and contacts the cluster.
func TestEventLog(t *testing.T) {
	runTestOnConfigs(t, testEventLogInner)
}

func testEventLogInner(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
	num := c.NumNodes()
	if num <= 0 {
		t.Fatalf("%d nodes in cluster", num)
	}

	var confirmedClusterID uuid.UUID
	type nodeEventInfo struct {
		Descriptor roachpb.NodeDescriptor
		ClusterID  uuid.UUID
	}

	// Verify that a node_join message was logged for each node in the cluster.
	// We expect there to eventually be one such message for each node in the
	// cluster, and each message must be correctly formatted.
	util.SucceedsSoon(t, func() error {
		db := makePGClient(t, c.PGUrl(0))
		defer db.Close()

		// Query all node join events. There should be one for each node in the
		// cluster.
		rows, err := db.Query(
			"SELECT targetID, info FROM system.eventlog WHERE eventType = $1",
			string(csql.EventLogNodeJoin))
		if err != nil {
			return err
		}
		seenIds := make(map[int64]struct{})
		var clusterID uuid.UUID
		for rows.Next() {
			var targetID int64
			var infoStr sql.NullString
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
			if a, e := int64(info.Descriptor.NodeID), targetID; a != e {
				t.Fatalf("Node join with targetID %d had descriptor for wrong node %d", e, a)
			}

			// Verify cluster ID is recorded, and is the same for all nodes.
			if uuid.Equal(info.ClusterID, *uuid.EmptyUUID) {
				t.Fatalf("Node join recorded nil cluster id, info: %v", info)
			}
			if uuid.Equal(clusterID, *uuid.EmptyUUID) {
				clusterID = info.ClusterID
			} else if !uuid.Equal(clusterID, info.ClusterID) {
				t.Fatalf(
					"Node join recorded different cluster ID than earlier node. Expected %s, got %s. Info: %v",
					clusterID, info.ClusterID, info)
			}

			// Verify that all NodeIDs are different.
			if _, ok := seenIds[targetID]; ok {
				t.Fatalf("Node ID %d seen in two different node join messages", targetID)
			}
			seenIds[targetID] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return err
		}

		if a, e := len(seenIds), c.NumNodes(); a != e {
			return util.Errorf("expected %d node join messages, found %d: %v", e, a, seenIds)
		}

		confirmedClusterID = clusterID
		return nil
	})

	// Stop and Start Node 0, and verify the node restart message.
	if err := c.Kill(0); err != nil {
		t.Fatal(err)
	}
	if err := c.Restart(0); err != nil {
		t.Fatal(err)
	}

	util.SucceedsSoon(t, func() error {
		db := makePGClient(t, c.PGUrl(0))
		defer db.Close()

		// Query all node restart events. There should only be one.
		rows, err := db.Query(
			"SELECT targetID, info FROM system.eventlog WHERE eventType = $1",
			string(csql.EventLogNodeRestart))
		if err != nil {
			return err
		}

		seenCount := 0
		for rows.Next() {
			var targetID int64
			var infoStr sql.NullString
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
			if a, e := int64(info.Descriptor.NodeID), targetID; a != e {
				t.Fatalf("Node join with targetID %d had descriptor for wrong node %d", e, a)
			}

			// Verify cluster ID is recorded, and is the same for all nodes.
			if !uuid.Equal(confirmedClusterID, info.ClusterID) {
				t.Fatalf(
					"Node restart recorded different cluster ID than earlier join. Expected %s, got %s. Info: %v",
					confirmedClusterID, info.ClusterID, info)
			}

			seenCount++
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if seenCount != 1 {
			return util.Errorf("Expected only one node restart event, found %d", seenCount)
		}
		return nil
	})
}
