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
// Author: Radu Berinde (radu@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql

import (
	gosql "database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// SplitTable splits a range in the table, creates a replica for the right
// side of the split on targetNodeIdx, and moves the lease for the right
// side of the split to targetNodeIdx. This forces the querying against
// the table to be distributed. vals is a list of values forming a primary
// key for the table.
//
// TODO(radu): SplitTable or its equivalent should be added to TestCluster.
//
// TODO(radu): we should verify that the queries in tests using SplitTable
// are indeed distributed as intended.
func SplitTable(
	t *testing.T,
	tc serverutils.TestClusterInterface,
	desc *sqlbase.TableDescriptor,
	targetNodeIdx int,
	vals ...interface{},
) {
	pik, err := sqlbase.MakePrimaryIndexKey(desc, vals...)
	if err != nil {
		t.Fatal(err)
	}

	splitKey := keys.MakeRowSentinelKey(pik)
	_, rightRange, err := tc.Server(0).SplitRange(splitKey)
	if err != nil {
		t.Fatal(err)
	}

	rightRangeStartKey := rightRange.StartKey.AsRawKey()
	rightRange, err = tc.AddReplicas(rightRangeStartKey, tc.Target(targetNodeIdx))
	if err != nil {
		t.Fatal(err)
	}

	if err := tc.TransferRangeLease(rightRange, tc.Target(targetNodeIdx)); err != nil {
		t.Fatal(err)
	}
}

// TestPlanningDuringSplits verifies that table reader planning (resolving
// spans) tolerates concurrent splits.
func TestPlanningDuringSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const n = 100
	const numNodes = 1
	tc := serverutils.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{UseDatabase: "test"},
	})

	defer tc.Stopper().Stop(context.TODO())

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "t", "x INT PRIMARY KEY, xsquared INT",
		n,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, func(row int) parser.Datum {
			return parser.NewDInt(parser.DInt(row * row))
		}),
	)

	// Start a worker that continuously performs splits in the background.
	tc.Stopper().RunWorker(context.TODO(), func(ctx context.Context) {
		rng, _ := randutil.NewPseudoRand()
		cdb := tc.Server(0).KVClient().(*client.DB)
		for {
			select {
			case <-tc.Stopper().ShouldStop():
				return
			default:
				// Split the table at a random row.
				desc := sqlbase.GetTableDescriptor(cdb, "test", "t")

				val := rng.Intn(n)
				t.Logf("splitting at %d", val)
				pik, err := sqlbase.MakePrimaryIndexKey(desc, val)
				if err != nil {
					panic(err)
				}

				splitKey := keys.MakeRowSentinelKey(pik)
				if _, _, err := tc.Server(0).SplitRange(splitKey); err != nil {
					panic(err)
				}
			}
		}
	})

	sumX, sumXSquared := 0, 0
	for x := 1; x <= n; x++ {
		sumX += x
		sumXSquared += x * x
	}

	// Run queries continuously in parallel workers. We need more than one worker
	// because some queries result in cache updates, and we want to verify
	// race conditions when planning during cache updates (see #15249).
	const numQueriers = 4

	var wg sync.WaitGroup
	wg.Add(numQueriers)

	for i := 0; i < numQueriers; i++ {
		go func(idx int) {
			defer wg.Done()

			// Create a gosql.DB for this worker.
			pgURL, cleanupGoDB := sqlutils.PGUrl(
				t, tc.Server(0).ServingAddr(), fmt.Sprintf("%d", idx), url.User(security.RootUser),
			)
			defer cleanupGoDB()

			pgURL.Path = "test"
			goDB, err := gosql.Open("postgres", pgURL.String())
			if err != nil {
				t.Error(err)
				return
			}

			defer func() {
				if err := goDB.Close(); err != nil {
					t.Error(err)
				}
			}()

			// Limit to 1 connection because we set a session variable.
			goDB.SetMaxOpenConns(1)
			if _, err := goDB.Exec("SET DISTSQL = ALWAYS"); err != nil {
				t.Error(err)
				return
			}

			for run := 0; run < 20; run++ {
				t.Logf("querier %d run %d", idx, run)
				rows, err := goDB.Query("SELECT SUM(x), SUM(xsquared) FROM t")
				if err != nil {
					t.Error(err)
					return
				}
				if !rows.Next() {
					t.Errorf("no rows")
					return
				}
				var sum, sumSq int
				if err := rows.Scan(&sum, &sumSq); err != nil {
					t.Error(err)
					return
				}
				if sum != sumX || sumXSquared != sumSq {
					t.Errorf("invalid results: expected %d, %d got %d, %d", sumX, sumXSquared, sum, sumSq)
					return
				}
				if rows.Next() {
					t.Errorf("more than one row")
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestDistBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test sets up various queries using these tables:
	//  - a NumToSquare table of size N that maps integers from 1 to n to their
	//    squares
	//  - a NumToStr table of size N^2 that maps integers to their string
	//    representations. This table is split and distributed to all the nodes.
	const n = 100
	const numNodes = 5

	tc := serverutils.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
				Knobs: base.TestingKnobs{
					SQLSchemaChanger: &SchemaChangerTestingKnobs{
						// Aggressively write checkpoints, so that
						// we test checkpointing functionality while
						// a schema change backfill is progressing.
						WriteCheckpointInterval: time.Nanosecond,
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.TODO())
	cdb := tc.Server(0).KVClient().(*client.DB)

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "NumToSquare", "x INT PRIMARY KEY, xsquared INT",
		n,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, func(row int) parser.Datum {
			return parser.NewDInt(parser.DInt(row * row))
		}),
	)

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "NumToStr", "y INT PRIMARY KEY, str STRING",
		n*n,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowEnglishFn),
	)
	// Split the table into multiple ranges.
	descNumToStr := sqlbase.GetTableDescriptor(cdb, "test", "NumToStr")
	// SplitTable moves the right range, so we split things back to front
	// in order to move less data.
	for i := numNodes - 1; i > 0; i-- {
		SplitTable(t, tc, descNumToStr, i, n*n/numNodes*i)
	}

	r := sqlutils.MakeSQLRunner(t, tc.ServerConn(0))
	r.DB.SetMaxOpenConns(1)
	r.Exec("SET DISTSQL = OFF")
	if _, err := tc.ServerConn(0).Exec("CREATE INDEX foo ON NumToStr (str)"); err != nil {
		t.Fatal(err)
	}
	r.Exec("SET DISTSQL = ALWAYS")
	res := r.QueryStr("SELECT str FROM NumToStr@foo")
	if len(res) != n*n {
		t.Errorf("expected %d entries, got %d", n*n, len(res))
	}
	// Check res is sorted.
	curr := ""
	for i, str := range res {
		if curr > str[0] {
			t.Errorf("unexpected unsorted %s > %s at %d", curr, str[0], i)
		}
		curr = str[0]
	}
}

// Test that distSQLReceiver uses inbound metadata to update the
// RangeDescriptorCache and the LeaseHolderCache.
func TestDistSQLReceiverUpdatesCaches(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rangeCache := kv.NewRangeDescriptorCache(nil /* db */, 2<<10 /* size */)
	leaseCache := kv.NewLeaseHolderCache(2 << 10 /* size */)
	r := makeDistSQLReceiver(context.TODO(), nil /* sink */, rangeCache, leaseCache)

	descs := []roachpb.RangeDescriptor{
		{RangeID: 1, StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("c")},
		{RangeID: 2, StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("e")},
		{RangeID: 3, StartKey: roachpb.RKey("g"), EndKey: roachpb.RKey("z")},
	}

	// Push some metadata and check that the caches are updated with it.
	status := r.Push(nil /* row */, distsqlrun.ProducerMetadata{
		Ranges: []roachpb.RangeInfo{
			{
				Desc: descs[0],
				Lease: roachpb.Lease{Replica: roachpb.ReplicaDescriptor{
					NodeID: 1, StoreID: 1, ReplicaID: 1}},
			},
			{
				Desc: descs[1],
				Lease: roachpb.Lease{Replica: roachpb.ReplicaDescriptor{
					NodeID: 2, StoreID: 2, ReplicaID: 2}},
			},
		}})
	if status != distsqlrun.NeedMoreRows {
		t.Fatalf("expected status NeedMoreRows, got: %d", status)
	}
	status = r.Push(nil /* row */, distsqlrun.ProducerMetadata{
		Ranges: []roachpb.RangeInfo{
			{
				Desc: descs[2],
				Lease: roachpb.Lease{Replica: roachpb.ReplicaDescriptor{
					NodeID: 3, StoreID: 3, ReplicaID: 3}},
			},
		}})
	if status != distsqlrun.NeedMoreRows {
		t.Fatalf("expected status NeedMoreRows, got: %d", status)
	}

	for i := range descs {
		desc, err := rangeCache.GetCachedRangeDescriptor(descs[i].StartKey, false /* inclusive */)
		if err != nil {
			t.Fatal(err)
		}
		if desc == nil {
			t.Fatalf("failed to find range for key: %s", descs[i].StartKey)
		}
		if !desc.Equal(descs[i]) {
			t.Fatalf("expected: %+v, got: %+v", descs[i], desc)
		}

		replica, ok := leaseCache.Lookup(context.TODO(), descs[i].RangeID)
		if !ok {
			t.Fatalf("didn't find lease for RangeID: %d", descs[i].RangeID)
		}
		if replica.ReplicaID != roachpb.ReplicaID(i+1) {
			t.Fatalf("expected ReplicaID: %d but found replica: %d", i, replica)
		}
	}
}

// Test that a gateway improves the physical plans that it generates as a result
// of running a badly-planned query and receiving range information in response;
// this range information is used to update caches on the gateway.
func TestDistSQLRangeCachesIntegrationTest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We're going to setup a cluster with 4 nodes. The last one will not be a
	// target of any replication so that its caches stay virgin.

	tc := serverutils.StartTestCluster(t, 4, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
			},
		})
	defer tc.Stopper().Stop(context.TODO())

	db0 := tc.ServerConn(0)
	sqlutils.CreateTable(t, db0, "left",
		"num INT PRIMARY KEY",
		3, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn))
	sqlutils.CreateTable(t, db0, "right",
		"num INT PRIMARY KEY",
		3, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn))

	db3 := tc.ServerConn(3)
	// Do a query on node 4 so that it populates the its cache with an initial
	// descriptor containing all the SQL key space. If we don't do this, the state
	// of the cache is left at the whim of gossiping the first descriptor done
	// during cluster startup - it can happen that the cache remains empty, which
	// is not what this test wants.
	_, err := db3.Exec(`SELECT * FROM "left"`)
	if err != nil {
		t.Fatal(err)
	}

	// We're going to split one of the tables, but node 4 is unaware of this.
	_, err = db0.Exec(fmt.Sprintf(`
	ALTER TABLE "right" SPLIT AT VALUES (1), (2), (3);
	ALTER TABLE "right" TESTING_RELOCATE VALUES (ARRAY[%d], 1), (ARRAY[%d], 2), (ARRAY[%d], 3);
	`,
		tc.Server(1).GetFirstStoreID(),
		tc.Server(0).GetFirstStoreID(),
		tc.Server(2).GetFirstStoreID()))
	if err != nil {
		t.Fatal(err)
	}

	// Run everything in a transaction, so we're bound on a connection on which we
	// force DistSQL.
	txn, err := db3.BeginTx(context.TODO(), nil /* opts */)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("SET DISTSQL = ALWAYS"); err != nil {
		t.Fatal(err)
	}

	// Check that the initial planning is suboptimal: the cache on db3 is unaware
	// of the splits and still holds the state after the first dummy query at the
	// beginning of the test, which had everything on the first node.
	query := `SELECT COUNT(1) FROM "left" INNER JOIN "right" USING (num)`
	row := db3.QueryRow(fmt.Sprintf("SELECT JSON FROM [EXPLAIN (DISTSQL) %v]", query))
	var json string
	if err := row.Scan(&json); err != nil {
		t.Fatal(err)
	}
	exp := `{"nodeNames":["1","4"]`
	if !strings.HasPrefix(json, exp) {
		t.Fatalf("expected prefix %s, but json is: %s", exp, json)
	}

	// Run a non-trivial query to force the "wrong range" metadata to flow through
	// a number of components.
	row = txn.QueryRowContext(context.TODO(), query)
	var cnt int
	if err := row.Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 3 {
		t.Fatalf("expected 3, got: %d", cnt)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Now assert that new plans correctly contain all the nodes. This is expected
	// to be a result of the caches having been updated on the gateway by the
	// previous query.
	row = db3.QueryRow(fmt.Sprintf("SELECT JSON FROM [EXPLAIN (DISTSQL) %v]", query))
	if err := row.Scan(&json); err != nil {
		t.Fatal(err)
	}
	exp = `{"nodeNames":["1","2","3","4"]`
	if !strings.HasPrefix(json, exp) {
		t.Fatalf("expected prefix %q, but json is: %s", exp, json)
	}
}
