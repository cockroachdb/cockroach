// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package physicalplan_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan/replicaoracle"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Test that resolving spans uses a node's range cache and lease holder cache.
// The idea is to test that resolving is not random, but predictable given the
// state of caches.
func TestSpanResolverUsesCaches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testcluster.StartTestCluster(t, 4,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "t",
			},
		})
	defer tc.Stopper().Stop(context.Background())

	rowRanges, _ := setupRanges(
		tc.Conns[0], tc.Servers[0], tc.Servers[0].DB(), t)

	// Replicate the row ranges on all of the first 3 nodes. Save the 4th node in
	// a pristine state, with empty caches.
	for i := 0; i < 3; i++ {
		rowRanges[i] = tc.AddVotersOrFatal(
			t, rowRanges[i].StartKey.AsRawKey(), tc.Target(1), tc.Target(2))
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

	lr := physicalplan.NewSpanResolver(
		s3.Cfg.Settings,
		s3.DistSenderI().(*kvcoord.DistSender),
		s3.Gossip(),
		s3.GetNode().Descriptor, nil,
		replicaoracle.BinPackingChoice)

	var spans []spanWithDir
	for i := 0; i < 3; i++ {
		spans = append(
			spans,
			spanWithDir{
				Span: roachpb.Span{
					Key:    rowRanges[i].StartKey.AsRawKey(),
					EndKey: rowRanges[i].EndKey.AsRawKey(),
				},
				dir: kvcoord.Ascending,
			})
	}

	// Resolve the spans. Since the range descriptor cache doesn't have any
	// leases, all the ranges should be grouped and "assigned" to replica 0.
	replicas, err := resolveSpans(context.Background(), lr.NewSpanResolverIterator(nil), spans...)
	if err != nil {
		t.Fatal(err)
	}
	if len(replicas) != 3 {
		t.Fatalf("expected replies for 3 spans, got %d", len(replicas))
	}
	si := tc.Servers[0]

	storeID := si.GetFirstStoreID()
	for i := 0; i < 3; i++ {
		if len(replicas[i]) != 1 {
			t.Fatalf("expected 1 range for span %s, got %d",
				spans[i].Span, len(replicas[i]))
		}
		rd := replicas[i][0].ReplicaDescriptor
		if rd.StoreID != storeID {
			t.Fatalf("expected span %s to be on replica (%d) but was on %s",
				spans[i].Span, storeID, rd)
		}
	}

	// Now populate the cached on node 4 and query again. This time, we expect to see
	// each span on its own range.
	if err := populateCache(tc.Conns[3], 3 /* expectedNumRows */); err != nil {
		t.Fatal(err)
	}
	replicas, err = resolveSpans(context.Background(), lr.NewSpanResolverIterator(nil), spans...)
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
	err := db.QueryRow(`SELECT count(1) FROM test`).Scan(&numRows)
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
	ts *server.TestServer, tableDesc catalog.TableDescriptor, pk int,
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error) {
	if len(tableDesc.PublicNonPrimaryIndexes()) != 0 {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{},
			errors.AssertionFailedf("expected table with just a PK, got: %+v", tableDesc)
	}
	pik, err := randgen.TestingMakePrimaryIndexKey(tableDesc, pk)
	if err != nil {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{}, err
	}

	leftRange, rightRange, err := ts.SplitRange(pik)
	if err != nil {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{},
			errors.Wrapf(err, "failed to split at row: %d", pk)
	}
	return leftRange, rightRange, nil
}

func TestSpanResolver(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, cdb := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "t",
	})
	defer s.Stopper().Stop(context.Background())

	rowRanges, tableDesc := setupRanges(db, s.(*server.TestServer), cdb, t)
	lr := physicalplan.NewSpanResolver(
		s.(*server.TestServer).Cfg.Settings,
		s.DistSenderI().(*kvcoord.DistSender),
		s.GossipI().(*gossip.Gossip),
		s.(*server.TestServer).GetNode().Descriptor, nil,
		replicaoracle.BinPackingChoice)

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
		for _, dir := range []kvcoord.ScanDirection{kvcoord.Ascending, kvcoord.Descending} {
			t.Run(fmt.Sprintf("%d-direction:%d", i, dir), func(t *testing.T) {
				replicas, err := resolveSpans(ctx, it, orient(dir, tc.spans...)...)
				if err != nil {
					t.Fatal(err)
				}
				if dir == kvcoord.Descending {
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
	defer log.Scope(t).Close(t)
	s, db, cdb := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "t",
	})
	defer s.Stopper().Stop(context.Background())

	rowRanges, tableDesc := setupRanges(db, s.(*server.TestServer), cdb, t)
	lr := physicalplan.NewSpanResolver(
		s.(*server.TestServer).Cfg.Settings,
		s.DistSenderI().(*kvcoord.DistSender),
		s.GossipI().(*gossip.Gossip),
		s.(*server.TestServer).GetNode().Descriptor,
		nil,
		replicaoracle.BinPackingChoice)

	ctx := context.Background()
	it := lr.NewSpanResolverIterator(nil)

	spans := []spanWithDir{
		orient(kvcoord.Ascending, makeSpan(tableDesc, 11, 15))[0],
		orient(kvcoord.Descending, makeSpan(tableDesc, 1, 14))[0],
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
	db *gosql.DB, s *server.TestServer, cdb *kv.DB, t *testing.T,
) ([]roachpb.RangeDescriptor, catalog.TableDescriptor) {
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

	tableDesc := catalogkv.TestingGetTableDescriptor(cdb, keys.SystemSQLCodec, "t", "test")
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

	// Run a select across the whole table to populate the caches with all the
	// ranges.
	if _, err := db.Exec(`SELECT count(1) from test`); err != nil {
		t.Fatal(err)
	}

	return rowRanges, tableDesc
}

type spanWithDir struct {
	roachpb.Span
	dir kvcoord.ScanDirection
}

func orient(dir kvcoord.ScanDirection, spans ...roachpb.Span) []spanWithDir {
	res := make([]spanWithDir, 0, len(spans))
	for _, span := range spans {
		res = append(res, spanWithDir{span, dir})
	}
	if dir == kvcoord.Descending {
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
	ctx context.Context, it physicalplan.SpanResolverIterator, spans ...spanWithDir,
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
				ReplicaDescriptor: repl,
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
	if len(rng.InternalReplicas) != 1 {
		panic(errors.AssertionFailedf("expected one replica in %+v", rng))
	}
	return rngInfo{ReplicaDescriptor: rng.InternalReplicas[0], rngDesc: rng}
}

func selectReplica(nodeID roachpb.NodeID, rng roachpb.RangeDescriptor) rngInfo {
	for _, rep := range rng.InternalReplicas {
		if rep.NodeID == nodeID {
			return rngInfo{ReplicaDescriptor: rep, rngDesc: rng}
		}
	}
	panic(errors.AssertionFailedf("no replica on node %d in: %s", nodeID, &rng))
}

func expectResolved(actual [][]rngInfo, expected ...[]rngInfo) error {
	if len(actual) != len(expected) {
		return errors.Errorf(
			"expected %d ranges, got %d", len(expected), len(actual))
	}
	for i, exp := range expected {
		act := actual[i]
		if len(exp) != len(act) {
			return errors.Errorf("expected %d ranges, got %d",
				len(exp), len(act))
		}
		for i, e := range exp {
			a := act[i]
			if e.ReplicaDescriptor.StoreID != a.ReplicaDescriptor.StoreID || e.rngDesc.RangeID != a.rngDesc.RangeID {
				return errors.Errorf(
					"expected replica (%d,%d) but got: (%d,%d)",
					e.ReplicaDescriptor.StoreID, e.rngDesc.RangeID,
					a.ReplicaDescriptor.StoreID, a.rngDesc.RangeID)
			}
		}
	}
	return nil
}

func makeSpan(tableDesc catalog.TableDescriptor, i, j int) roachpb.Span {
	makeKey := func(val int) roachpb.Key {
		key, err := randgen.TestingMakePrimaryIndexKey(tableDesc, val)
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
