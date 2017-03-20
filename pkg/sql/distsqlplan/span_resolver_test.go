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

package distsqlplan_test

import (
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Test that resolving spans uses a node's range cache and lease holder cache.
// The idea is to test that resolving is not random, but predictable given the
// state of caches.
func TestSpanResolverUsesCaches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#13525")
	tc := testcluster.StartTestCluster(t, 4,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "t",
			},
		})
	defer tc.Stopper().Stop()

	rowRanges, _ := setupRanges(
		tc.Conns[0], tc.Servers[0], tc.Servers[0].KVClient().(*client.DB), t)

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
		testutils.SucceedsSoon(t, func() error {
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

	lr := distsqlplan.NewSpanResolver(
		s3.DistSender(), s3.Gossip(), s3.GetNode().Descriptor,
		distsqlplan.BinPackingLeaseHolderChoice)

	var spans []spanWithDir
	for i := 0; i < 3; i++ {
		spans = append(
			spans,
			spanWithDir{
				Span: roachpb.Span{
					Key:    rowRanges[i].StartKey.AsRawKey(),
					EndKey: rowRanges[i].EndKey.AsRawKey(),
				},
				dir: kv.Ascending,
			})
	}

	// Resolve the spans. Since the LeaseHolderCache is empty, all the ranges
	// should be grouped and "assigned" to replica 0.
	replicas, err := resolveSpans(context.TODO(), lr.NewSpanResolverIterator(nil), spans...)
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
				spans[i].Span, len(replicas[i]), replicas[i])
		}
		rd := replicas[i][0].ReplicaDescriptor
		if rd.NodeID != nodeID || rd.StoreID != storeID {
			t.Fatalf("expected span %s to be on replica (%d, %d) but was on %s",
				spans[i].Span, nodeID, storeID, rd)
		}
	}

	// Now populate the cached on node 4 and query again. This time, we expect to see
	// each span on its own range.
	if err := populateCache(tc.Conns[3], 3 /* expectedNumRows */); err != nil {
		t.Fatal(err)
	}
	replicas, err = resolveSpans(context.TODO(), lr.NewSpanResolverIterator(nil), spans...)
	if err != nil {
		t.Fatal(err)
	}

	var expected [][]rngInfo
	for i := 0; i < 3; i++ {
		expected = append(expected, []rngInfo{selectReplica(tc.Servers[i].NodeID(), rowRanges[i])})
	}
	if err = expectResolved(replicas, expected...); err != nil {
		t.Fatal(err)
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

// splitRangeAtVal splits the range for a table with schema
// `CREATE TABLE test (k INT PRIMARY KEY)` at row with value pk (the row will be
// the first on the right of the split).
func splitRangeAtVal(
	ts *server.TestServer, tableDesc *sqlbase.TableDescriptor, pk int,
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error) {
	if len(tableDesc.Indexes) != 0 {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{},
			errors.Errorf("expected table with just a PK, got: %+v", tableDesc)
	}
	pik, err := sqlbase.MakePrimaryIndexKey(tableDesc, pk)
	if err != nil {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{}, err
	}

	startKey := keys.MakeRowSentinelKey(pik)
	leftRange, rightRange, err := ts.SplitRange(startKey)
	if err != nil {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{},
			errors.Wrapf(err, "failed to split at row: %d", pk)
	}
	return leftRange, rightRange, nil
}

func TestSpanResolver(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#13237")
	s, db, cdb := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "t",
	})
	defer s.Stopper().Stop()

	rowRanges, tableDesc := setupRanges(db, s.(*server.TestServer), cdb, t)
	lr := distsqlplan.NewSpanResolver(
		s.DistSender(), s.Gossip(),
		s.(*server.TestServer).GetNode().Descriptor,
		distsqlplan.BinPackingLeaseHolderChoice)

	ctx := context.Background()
	it := lr.NewSpanResolverIterator(nil)

	testCases := []struct {
		spans    []roachpb.Span
		expected [][]rngInfo
	}{
		{
			[]roachpb.Span{makeSpan(tableDesc, 0, 10000)},
			[][]rngInfo{{
				onlyReplica(rowRanges[0]),
				onlyReplica(rowRanges[1]),
				onlyReplica(rowRanges[2])}},
		},
		{
			[]roachpb.Span{
				makeSpan(tableDesc, 0, 9),
				makeSpan(tableDesc, 11, 19),
				makeSpan(tableDesc, 21, 29),
			},
			[][]rngInfo{
				{onlyReplica(rowRanges[0])},
				{onlyReplica(rowRanges[1])},
				{onlyReplica(rowRanges[2])},
			},
		},
		{
			[]roachpb.Span{
				makeSpan(tableDesc, 0, 20),
				makeSpan(tableDesc, 20, 29),
			},
			[][]rngInfo{
				{onlyReplica(rowRanges[0]), onlyReplica(rowRanges[1])},
				{onlyReplica(rowRanges[2])},
			},
		},
		{
			[]roachpb.Span{
				makeSpan(tableDesc, 0, 1),
				makeSpan(tableDesc, 1, 2),
				makeSpan(tableDesc, 2, 3),
				makeSpan(tableDesc, 3, 4),
				makeSpan(tableDesc, 5, 11),
				makeSpan(tableDesc, 20, 29),
			},
			[][]rngInfo{
				{onlyReplica(rowRanges[0])},
				{onlyReplica(rowRanges[0])},
				{onlyReplica(rowRanges[0])},
				{onlyReplica(rowRanges[0])},
				{onlyReplica(rowRanges[0]), onlyReplica(rowRanges[1])},
				{onlyReplica(rowRanges[2])},
			},
		},
	}
	for i, tc := range testCases {
		for _, dir := range []kv.ScanDirection{kv.Ascending, kv.Descending} {
			t.Run(fmt.Sprintf("%d-direction:%d", i, dir), func(t *testing.T) {
				replicas, err := resolveSpans(ctx, it, orient(dir, tc.spans...)...)
				if err != nil {
					t.Fatal(err)
				}
				if dir == kv.Descending {
					// When testing Descending resolving, reverse the expected results.
					for i, j := 0, len(tc.expected)-1; i <= j; i, j = i+1, j-1 {
						reverse(tc.expected[i])
						if i != j {
							reverse(tc.expected[j])
						}
						tc.expected[i], tc.expected[j] = tc.expected[j], tc.expected[i]
					}
				}
				if err = expectResolved(replicas, tc.expected...); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestMixedDirections(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, cdb := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "t",
	})
	defer s.Stopper().Stop()

	rowRanges, tableDesc := setupRanges(db, s.(*server.TestServer), cdb, t)
	lr := distsqlplan.NewSpanResolver(
		s.DistSender(), s.Gossip(),
		s.(*server.TestServer).GetNode().Descriptor,
		distsqlplan.BinPackingLeaseHolderChoice)

	ctx := context.Background()
	it := lr.NewSpanResolverIterator(nil)

	spans := []spanWithDir{
		orient(kv.Ascending, makeSpan(tableDesc, 11, 15))[0],
		orient(kv.Descending, makeSpan(tableDesc, 1, 14))[0],
	}
	replicas, err := resolveSpans(ctx, it, spans...)
	if err != nil {
		t.Fatal(err)
	}
	expected := [][]rngInfo{
		{onlyReplica(rowRanges[1])},
		{onlyReplica(rowRanges[1]), onlyReplica(rowRanges[0])},
	}
	if err = expectResolved(replicas, expected...); err != nil {
		t.Fatal(err)
	}
}

func setupRanges(
	db *gosql.DB, s *server.TestServer, cdb *client.DB, t *testing.T,
) ([]roachpb.RangeDescriptor, *sqlbase.TableDescriptor) {
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

	tableDesc := sqlbase.GetTableDescriptor(cdb, "t", "test")
	// Split every SQL row to its own range.
	rowRanges := make([]roachpb.RangeDescriptor, len(values))
	for i, val := range values {
		var err error
		var l roachpb.RangeDescriptor
		l, rowRanges[i], err = splitRangeAtVal(s, tableDesc, val)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			rowRanges[i-1] = l
		}
	}

	// TODO(andrei): The sleep below serves to remove the noise that the
	// RangeCache might encounter, clobbering descriptors with old versions.
	// Remove once all the causes of such clobbering, listed in #10751, have been
	// fixed.
	time.Sleep(300 * time.Millisecond)
	// Run a select across the whole table to populate the caches with all the
	// ranges.
	if _, err := db.Exec(`SELECT COUNT(1) from test`); err != nil {
		t.Fatal(err)
	}

	return rowRanges, tableDesc
}

type spanWithDir struct {
	roachpb.Span
	dir kv.ScanDirection
}

func orient(dir kv.ScanDirection, spans ...roachpb.Span) []spanWithDir {
	res := make([]spanWithDir, 0, len(spans))
	for _, span := range spans {
		res = append(res, spanWithDir{span, dir})
	}
	if dir == kv.Descending {
		for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
			res[i], res[j] = res[j], res[i]
		}
	}
	return res
}

type rngInfo struct {
	roachpb.ReplicaDescriptor
	rngDesc roachpb.RangeDescriptor
}

func resolveSpans(
	ctx context.Context, it distsqlplan.SpanResolverIterator, spans ...spanWithDir,
) ([][]rngInfo, error) {
	res := make([][]rngInfo, 0)
	for _, span := range spans {
		repls := make([]rngInfo, 0)
		for it.Seek(ctx, span.Span, span.dir); ; it.Next(ctx) {
			if !it.Valid() {
				return nil, it.Error()
			}
			repl, err := it.ReplicaInfo(ctx)
			if err != nil {
				return nil, err
			}
			repls = append(repls, rngInfo{
				ReplicaDescriptor: repl.ReplicaDescriptor,
				rngDesc:           it.Desc(),
			})
			if !it.NeedAnother() {
				break
			}
		}
		res = append(res, repls)
	}
	return res, nil
}

func onlyReplica(rng roachpb.RangeDescriptor) rngInfo {
	if len(rng.Replicas) != 1 {
		panic(fmt.Sprintf("expected one replica in %+v", rng))
	}
	return rngInfo{ReplicaDescriptor: rng.Replicas[0], rngDesc: rng}
}

func selectReplica(nodeID roachpb.NodeID, rng roachpb.RangeDescriptor) rngInfo {
	for _, rep := range rng.Replicas {
		if rep.NodeID == nodeID {
			return rngInfo{ReplicaDescriptor: rep, rngDesc: rng}
		}
	}
	panic(fmt.Sprintf("no replica on node %d in: %s", nodeID, rng))
}

func expectResolved(actual [][]rngInfo, expected ...[]rngInfo) error {
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
			if e.ReplicaDescriptor != a.ReplicaDescriptor || e.rngDesc.RangeID != a.rngDesc.RangeID {
				return errors.Errorf(
					"expected replica: %+v but got: %+v", e, a)
			}
		}
	}
	return nil
}

func makeSpan(tableDesc *sqlbase.TableDescriptor, i, j int) roachpb.Span {
	makeKey := func(val int) roachpb.Key {
		key, err := sqlbase.MakePrimaryIndexKey(tableDesc, val)
		if err != nil {
			panic(err)
		}
		return key
	}
	return roachpb.Span{
		Key:    makeKey(i),
		EndKey: makeKey(j),
	}
}

func reverse(r []rngInfo) {
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
}
