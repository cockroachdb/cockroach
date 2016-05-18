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
// Author: Andrei Matei (andreimatei1@gmail.com)

package distsql_test

import (
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Test that resolving spans uses a node's range cache and lease holder cache.
// The idea is to test that resolving is not random, but predictable given the
// state of caches.
func TestResolveSpansUsesCaches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testcluster.StartTestCluster(t, 4,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "t",
			},
		})
	defer tc.Stopper().Stop()

	if _, err := tc.Conns[0].Exec(`CREATE DATABASE t`); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.Conns[0].Exec(`CREATE TABLE test (k INT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	values := []int{0, 10, 20}
	for _, val := range values {
		// Multiply by 10 to space out the values so we can easily construct keys
		// that fall within the range.
		if _, err := tc.Conns[0].Exec("INSERT INTO test VALUES ($1)", val*10); err != nil {
			t.Fatal(err)
		}
	}

	tableDesc := sqlbase.GetTableDescriptor(tc.Servers[0].DB(), "t", "test")
	// Split every SQL row to its own range.
	rowRanges := make([]roachpb.RangeDescriptor, len(values))
	for i, val := range values {
		var err error
		var l roachpb.RangeDescriptor
		l, rowRanges[i], err = splitRangeAtKey(tc.Servers[0], tableDesc, val)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			rowRanges[i-1] = l
		}
	}
	// Replicate the row ranges on all of the first 3 nodes. Save the 4th node in
	// a pristine state, with empty caches.
	for i := 0; i < 3; i++ {
		var err error
		rowRanges[i], err = tc.AddReplicas(
			rowRanges[i].StartKey.AsRawKey(), tc.Target(1), tc.Target(2))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Scatter the leases around; node i gets range i.
	for i := 0; i < 3; i++ {
		if err := tc.TransferRangeLease(rowRanges[i], tc.Target(i)); err != nil {
			t.Fatal(err)
		}
		// Wait for everybody to apply the new lease, so that we can rely on the
		// lease discovery done later by the SpanResolver to be up to date.
		util.SucceedsSoon(t, func() error {
			for j := 0; j < 3; j++ {
				target := tc.Target(j)
				rt, err := tc.FindRangeLeaseHolder(rowRanges[i], &target)
				if err != nil {
					return err
				}
				if rt != tc.Target(i) {
					return errors.Errorf("node %d hasn't applied the lease yet", j)
				}
			}
			return nil
		})
	}

	// Create a SpanResolver using the 4th node, with empty caches.
	s3 := tc.Servers[3]

	lr := distsql.NewSpanResolver(
		s3.DistSender(), s3.Gossip(), s3.GetNode().Descriptor, s3.Stopper(),
		distsql.BinPackingLHGuessingPolicy)

	var spans []roachpb.Span
	for i := 0; i < 3; i++ {
		spans = append(
			spans,
			roachpb.Span{Key: rowRanges[i].StartKey.AsRawKey(), EndKey: rowRanges[i].EndKey.AsRawKey()})
	}

	// Resolve the spans. Since the LeaseHolderCache is empty, all the ranges
	// should be grouped and "assigned" to replica 0.
	replicas, err := lr.ResolveSpans(context.TODO(), distsql.Ascending, spans...)
	if err != nil {
		t.Fatal(err)
	}
	if len(replicas) != 3 {
		t.Fatalf("expected replies for 3 spans, got %d: %+v", len(replicas), replicas)
	}
	si := tc.Servers[0]
	nodeID := si.GetNode().Descriptor.NodeID
	storeID := si.GetFirstStoreID()
	for i := 0; i < 3; i++ {
		if len(replicas[i]) != 1 {
			t.Fatalf("expected 1 range for span %s, got %d (%+v)",
				spans[i], len(replicas[i]), replicas[i])
		}
		rd := replicas[i][0].ReplicaDescriptor
		if rd.NodeID != nodeID || rd.StoreID != storeID {
			t.Fatalf("expected span %s to be on replica (%d, %d) but was on %s",
				spans[i], nodeID, storeID, rd)
		}
	}

	// Now populate the cached on node 4 and query again. Now we expect to see
	// each range on its own range.
	if err := populateCache(tc.Conns[3], 3 /* expectedNumRows */); err != nil {
		t.Fatal(err)
	}
	replicas, err = lr.ResolveSpans(context.TODO(), distsql.Ascending, spans...)
	if err != nil {
		t.Fatal(err)
	}
	if len(replicas) != 3 {
		t.Fatalf("expected replies for 3 spans, got %d: %+v", len(replicas), replicas)
	}
	for i := 0; i < 3; i++ {
		if len(replicas[i]) != 1 {
			t.Fatalf("expected 1 range for span %s, got %d (%+v)",
				spans[i], len(replicas[i]), replicas[i])
		}
		rd := replicas[i][0].ReplicaDescriptor
		expectedServer := tc.Servers[i]
		if rd.NodeID != expectedServer.GetNode().Descriptor.NodeID ||
			rd.StoreID != expectedServer.GetFirstStoreID() {
			t.Fatalf("expected span %s to be on replica (%d, %d) but was on %s",
				spans[i], nodeID, storeID, rd)
		}
	}
}

// populateCache runs a scan over a whole table to populate the range cache and
// the lease holder cache of the server to which db is connected.
func populateCache(db *gosql.DB, expectedNumRows int) error {
	var numRows int
	err := db.QueryRow(`SELECT COUNT(1) FROM test`).Scan(&numRows)
	if err != nil {
		return err
	}
	if numRows != expectedNumRows {
		return errors.Errorf("expected %d rows, got %d", expectedNumRows, numRows)
	}
	return nil
}

// splitRangeAtKey splits the range for a table with schema
// `CREATE TABLE test (k INT PRIMARY KEY)` at row with value pk (the row will be
// the first on the right of the split).
func splitRangeAtKey(
	ts *server.TestServer, tableDesc *sqlbase.TableDescriptor, pk int,
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error) {
	if len(tableDesc.Columns) != 1 {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{}, errors.Errorf("expected table with one col, got: %+v", tableDesc)
	}
	if tableDesc.Columns[0].Type.Kind != sqlbase.ColumnType_INT {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{}, errors.Errorf("expected table with one INT col, got: %+v", tableDesc)
	}

	if len(tableDesc.Indexes) != 0 {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{}, errors.Errorf("expected table with just a PK, got: %+v", tableDesc)
	}
	if len(tableDesc.PrimaryIndex.ColumnIDs) != 1 ||
		tableDesc.PrimaryIndex.ColumnIDs[0] != tableDesc.Columns[0].ID ||
		tableDesc.PrimaryIndex.ColumnDirections[0] != sqlbase.IndexDescriptor_ASC {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{}, errors.Errorf("table with unexpected PK: %+v", tableDesc)
	}

	pik := primaryIndexKey(tableDesc, parser.NewDInt(parser.DInt(pk)))
	startKey := keys.MakeFamilyKey(pik, uint32(tableDesc.Families[0].ID))
	leftRange, rightRange, err := ts.SplitRange(startKey)
	if err != nil {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{}, errors.Wrapf(err, "failed to split at row: %d", pk)
	}
	return leftRange, rightRange, nil
}

func primaryIndexKey(tableDesc *sqlbase.TableDescriptor, val parser.Datum) roachpb.Key {
	primaryIndexKeyPrefix := sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)
	colIDtoRowIndex := map[sqlbase.ColumnID]int{tableDesc.Columns[0].ID: 0}
	pk, _, err := sqlbase.EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, colIDtoRowIndex, []parser.Datum{val}, primaryIndexKeyPrefix)
	if err != nil {
		panic(err)
	}
	return roachpb.Key(pk)
}

func TestResolveSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, cdb := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "t",
	})
	defer s.Stopper().Stop()

	if _, err := db.Exec(`CREATE DATABASE t`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE test (k INT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	values := []int{0, 10, 20}
	for _, val := range values {
		// Multiply by 10 to space out the values so we can easily construct keys
		// that fall within the range.
		if _, err := db.Exec("INSERT INTO test VALUES ($1)", val*10); err != nil {
			t.Fatal(err)
		}
	}

	lr := distsql.NewSpanResolver(
		s.DistSender(), s.Gossip(),
		s.(*server.TestServer).GetNode().Descriptor, s.Stopper(),
		distsql.BinPackingLHGuessingPolicy)

	tableDesc := sqlbase.GetTableDescriptor(cdb, "t", "test")
	// Split every SQL row to its own range.
	rowRanges := make([]roachpb.RangeDescriptor, len(values))
	for i, val := range values {
		var err error
		var l roachpb.RangeDescriptor
		l, rowRanges[i], err = splitRangeAtKey(s.(*server.TestServer), tableDesc, val)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			rowRanges[i-1] = l
		}
	}

	replicas, err := lr.ResolveSpans(
		context.TODO(), distsql.Ascending, makeSpan(tableDesc, 0, 10000))
	if err != nil {
		t.Fatal(err)
	}
	if err := expectResolved(replicas,
		[]roachpb.ReplicaDescriptor{
			onlyReplica(rowRanges[0]),
			onlyReplica(rowRanges[1]),
			onlyReplica(rowRanges[2])},
	); err != nil {
		t.Fatal(err)
	}

	replicas, err = lr.ResolveSpans(
		context.TODO(), distsql.Ascending,
		makeSpan(tableDesc, 0, 9),
		makeSpan(tableDesc, 11, 19),
		makeSpan(tableDesc, 21, 29))
	if err != nil {
		t.Fatal(err)
	}
	if err := expectResolved(replicas,
		[]roachpb.ReplicaDescriptor{onlyReplica(rowRanges[0])},
		[]roachpb.ReplicaDescriptor{onlyReplica(rowRanges[1])},
		[]roachpb.ReplicaDescriptor{onlyReplica(rowRanges[2])},
	); err != nil {
		t.Fatal(err)
	}
}

func onlyReplica(rng roachpb.RangeDescriptor) roachpb.ReplicaDescriptor {
	if len(rng.Replicas) != 1 {
		panic(fmt.Sprintf("expected one replica in %+v", rng))
	}
	return rng.Replicas[0]
}

func expectResolved(actual [][]kv.ReplicaInfo, expected ...[]roachpb.ReplicaDescriptor) error {
	if len(actual) != len(expected) {
		return errors.Errorf(
			"expected %d ranges, got %d: %+v", len(expected), len(actual), actual)
	}
	for i, exp := range expected {
		act := actual[i]
		if len(exp) != len(act) {
			return errors.Errorf("expected %d ranges, got %d (%+v)",
				len(exp), len(act), act)
		}
		for i, e := range exp {
			a := act[i]
			if e != a.ReplicaDescriptor {
				return errors.Errorf(
					"expected replica: %s but got: %s", e, a.ReplicaDescriptor)
			}
		}
	}
	return nil
}

func makeSpan(tableDesc *sqlbase.TableDescriptor, i, j int) roachpb.Span {
	di := parser.NewDInt(parser.DInt(i))
	dj := parser.NewDInt(parser.DInt(j))
	return roachpb.Span{
		Key:    primaryIndexKey(tableDesc, di),
		EndKey: primaryIndexKey(tableDesc, dj),
	}
}
