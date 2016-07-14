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
// Author: David Taylor (david@cockroachlabs.com)
// Author: Andrei Matei (andrei@cockroachlabs.com)

package testcluster

import (
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

func TestTestClusterWithManualReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := StartTestCluster(t, 3,
		ClusterArgs{
			ReplicationMode: ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "t",
			},
		})
	defer tc.Stopper().Stop()

	if _, err := tc.Conns[0].Exec(`CREATE DATABASE t`); err != nil {
		t.Fatal(err)
	}

	if _, err := tc.Conns[0].Exec(`CREATE TABLE test (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}

	if _, err := tc.Conns[0].Exec(`INSERT INTO test VALUES (5, 1), (4, 2), (1, 2)`); err != nil {
		t.Fatal(err)
	}

	if r, err := tc.Conns[1].Query(`SELECT * FROM test WHERE k = 5`); err != nil {
		t.Fatal(err)
	} else if !r.Next() {
		t.Fatal("no rows")
	}

	if r, err := tc.Conns[2].Exec(`DELETE FROM test`); err != nil {
		t.Fatal(err)
	} else if rows, err := r.RowsAffected(); err != nil {
		t.Fatal(err)
	} else if expected, actual := int64(3), rows; expected != actual {
		t.Fatalf("wrong row count deleted: expected %d actual %d", expected, actual)
	}

	// Split the table to a new range.
	kvDB := tc.Servers[0].DB()
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	tableStartKey := keys.MakeRowSentinelKey(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	leftRangeDesc, tableRangeDesc, err := tc.SplitRange(tableStartKey)
	if err != nil {
		t.Fatal(err)
	}
	log.Infof("After split got ranges: %+v and %+v.", leftRangeDesc, tableRangeDesc)
	if len(tableRangeDesc.Replicas) == 0 {
		t.Fatalf(
			"expected replica on node 1, got no replicas: %+v", tableRangeDesc.Replicas)
	}
	if tableRangeDesc.Replicas[0].NodeID != 1 {
		t.Fatalf(
			"expected replica on node 1, got replicas: %+v", tableRangeDesc.Replicas)
	}

	// Replicate the table's range to all the nodes.
	tableRangeDesc, err = tc.AddReplicas(
		tableRangeDesc,
		ReplicationTarget{
			NodeID:  tc.Servers[1].GetNode().Descriptor.NodeID,
			StoreID: tc.Servers[1].GetFirstStoreID(),
		},
		ReplicationTarget{
			NodeID:  tc.Servers[2].GetNode().Descriptor.NodeID,
			StoreID: tc.Servers[2].GetFirstStoreID(),
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(tableRangeDesc.Replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %+v", tableRangeDesc.Replicas)
	}
	for i := 0; i < 3; i++ {
		if _, ok := tableRangeDesc.GetReplicaDescriptor(
			tc.Servers[i].GetFirstStoreID()); !ok {
			t.Fatalf("expected replica on store %d, got %+v",
				tc.Servers[i].GetFirstStoreID(), tableRangeDesc.Replicas)
		}
	}
}

func TestWaitForFullReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := StartTestCluster(t, 3, ClusterArgs{ReplicationMode: ReplicationFull})
	defer tc.Stopper().Stop()
	if err := tc.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}
}
