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

package testutils

import (
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server/testingshim"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracing"
)

func TestTestCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer tracing.Disable()()

	params := testingshim.TestServerParams{}
	tc, cleanup := StartTestCluster(t, 3, "t", params)
	defer cleanup()

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
	tableStartKey := roachpb.Key(
		sqlbase.MakeIndexKeyPrefix(tableDesc.ID, sqlbase.IndexID(0)))
	leftRangeDesc, tableRangeDesc := tc.SplitRange(tableStartKey)
	log.Infof("After split got ranges: %+v and %+v.", leftRangeDesc, tableRangeDesc)
	if tableRangeDesc.Replicas[0].NodeID != 1 {
		t.Fatalf(
			"expected replica on node 1, got replicas: %+v", tableRangeDesc.Replicas)
	}

	// Replicate the table's range to all the nodes.
	tableRangeDesc = tc.AddReplicas(
		t, tableRangeDesc,
		ReplicationTarget{
			NodeID:  tc.Servers[1].GetNode().Descriptor.NodeID,
			StoreID: tc.Servers[1].GetFirstStoreID(),
		},
		ReplicationTarget{
			NodeID:  tc.Servers[2].GetNode().Descriptor.NodeID,
			StoreID: tc.Servers[2].GetFirstStoreID(),
		})
	log.Infof("after replication, table range desc: %+v", tableRangeDesc)
	if len(tableRangeDesc.Replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %+v", tableRangeDesc.Replicas)
	}
	for i := 0; i < 3; i++ {
		if _, replica := tableRangeDesc.FindReplica(
			tc.Servers[i].GetFirstStoreID()); replica == nil {
			t.Fatalf("expected replica on store %d, got %+v",
				tc.Servers[i].GetFirstStoreID(), tableRangeDesc.Replicas)
		}
	}
}
