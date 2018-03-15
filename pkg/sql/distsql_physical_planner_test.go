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

package sql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
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

	_, rightRange, err := tc.Server(0).SplitRange(pik)
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
		sqlutils.ToRowFn(sqlutils.RowIdxFn, func(row int) tree.Datum {
			return tree.NewDInt(tree.DInt(row * row))
		}),
	)

	// Start a worker that continuously performs splits in the background.
	tc.Stopper().RunWorker(context.TODO(), func(ctx context.Context) {
		rng, _ := randutil.NewPseudoRand()
		cdb := tc.Server(0).DB()
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

				if _, _, err := tc.Server(0).SplitRange(pik); err != nil {
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

// Test that distSQLReceiver uses inbound metadata to update the
// RangeDescriptorCache and the LeaseHolderCache.
func TestDistSQLReceiverUpdatesCaches(t *testing.T) {
	defer leaktest.AfterTest(t)()

	size := func() int64 { return 2 << 10 }
	st := cluster.MakeTestingClusterSettings()
	rangeCache := kv.NewRangeDescriptorCache(st, nil /* db */, size)
	leaseCache := kv.NewLeaseHolderCache(size)
	r := makeDistSQLReceiver(
		context.TODO(), nil /* resultWriter */, tree.Rows,
		rangeCache, leaseCache, nil /* txn */, nil /* updateClock */)

	descs := []roachpb.RangeDescriptor{
		{RangeID: 1, StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("c")},
		{RangeID: 2, StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("e")},
		{RangeID: 3, StartKey: roachpb.RKey("g"), EndKey: roachpb.RKey("z")},
	}

	// Push some metadata and check that the caches are updated with it.
	status := r.Push(nil /* row */, &distsqlrun.ProducerMetadata{
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
	status = r.Push(nil /* row */, &distsqlrun.ProducerMetadata{
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

		_, ok := leaseCache.Lookup(context.TODO(), descs[i].RangeID)
		if !ok {
			t.Fatalf("didn't find lease for RangeID: %d", descs[i].RangeID)
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
	row := db3.QueryRow(fmt.Sprintf(`SELECT "JSON" FROM [EXPLAIN (DISTSQL) %v]`, query))
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
	row = db3.QueryRow(fmt.Sprintf(`SELECT "JSON" FROM [EXPLAIN (DISTSQL) %v]`, query))
	if err := row.Scan(&json); err != nil {
		t.Fatal(err)
	}
	exp = `{"nodeNames":["1","2","3","4"]`
	if !strings.HasPrefix(json, exp) {
		t.Fatalf("expected prefix %q, but json is: %s", exp, json)
	}
}

func TestDistSQLDeadHosts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("test is too slow; we need to tweak timeouts so connections die faster (see #14376)")

	const n = 100
	const numNodes = 5

	tc := serverutils.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      base.TestServerArgs{UseDatabase: "test"},
	})
	defer tc.Stopper().Stop(context.TODO())

	r := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	r.DB.SetMaxOpenConns(1)
	r.Exec(t, "CREATE DATABASE test")

	r.Exec(t, "CREATE TABLE t (x INT PRIMARY KEY, xsquared INT)")

	for i := 0; i < numNodes; i++ {
		r.Exec(t, fmt.Sprintf("ALTER TABLE t SPLIT AT VALUES (%d)", n*i/5))
	}

	for i := 0; i < numNodes; i++ {
		r.Exec(t, fmt.Sprintf(
			"ALTER TABLE t TESTING_RELOCATE VALUES (ARRAY[%d,%d,%d], %d)",
			i+1, (i+1)%5+1, (i+2)%5+1, n*i/5,
		))
	}

	r.Exec(t, fmt.Sprintf("INSERT INTO t SELECT i, i*i FROM GENERATE_SERIES(1, %d) AS g(i)", n))

	r.Exec(t, "SET DISTSQL = ON")

	// Run a query that uses the entire table and is easy to verify.
	runQuery := func() error {
		log.Infof(context.TODO(), "running test query")
		var res int
		if err := r.DB.QueryRow("SELECT SUM(xsquared) FROM t").Scan(&res); err != nil {
			return err
		}
		if exp := (n * (n + 1) * (2*n + 1)) / 6; res != exp {
			t.Fatalf("incorrect result %d, expected %d", res, exp)
		}
		log.Infof(context.TODO(), "test query OK")
		return nil
	}
	if err := runQuery(); err != nil {
		t.Error(err)
	}

	// Verify the plan (should include all 5 nodes).
	r.CheckQueryResults(t,
		"SELECT URL FROM [EXPLAIN (DISTSQL) SELECT SUM(xsquared) FROM t]",
		[][]string{{"https://cockroachdb.github.io/distsqlplan/decode.html?eJy8k09LwzAYxu9-CnlOCu9h7bo5e5rHHXQy9SQ91OalFLamJCkoo99d1iDaIskgo8f8-T2_PG1yRC0FP-UH1kjfEYEQgzAHIQFhgYzQKFmw1lKdtlhgIz6RzghV3bTmNJ0RCqkY6RGmMntGitf8Y887zgUrEASbvNr3kkZVh1x9rQ0I29ak1-sYWUeQrflJ6-h8z0NZKi5zI0eal7fHm3V0e3b0b2JbSyVYsRgEZt2F5dFE38_jCakQT1TB4wmpMJ-ogscTUiGZqILHc6mH-E_0jnUja82jBznMywgsSrZvWctWFfysZNGH2-G2391PCNbGrkZ2sKnt0ulYf-HICccDOBrDsdvsUc-ddOKGk5BzL5zw0m1ehpjvnPDKbV6FmO_d_2rmuSbuSzZ2Z93VdwAAAP__XTV6BQ=="}},
	)

	// Stop node 5.
	tc.StopServer(4)

	testutils.SucceedsSoon(t, runQuery)

	r.CheckQueryResults(t,
		"SELECT URL FROM [EXPLAIN (DISTSQL) SELECT SUM(xsquared) FROM t]",
		[][]string{{"https://cockroachdb.github.io/distsqlplan/decode.html?eJy8k8FK7DAYhff3KS5npZCF6dRx7KouZ6Ejo64ki9j8lEKnKUkKytB3lzaItkg60qHL5M93vpySHlFpRQ_yQBbJKzgYIjCswBBDMNRGZ2StNt3YH96qdyRXDEVVN67bFgyZNoTkCFe4kpDgWb6VtCepyIBBkZNF2QtqUxyk-UgdGHaNS_6nEUTLoBv3lday0z13eW4ol06PNE8v9xcpvzw5-juxqbRRZEgNAkV7Zjlf6PtNeOZUiBaqMOGZU2G1UIUJz7le8S_Re7K1riyNXvMwTzCQysn_CFY3JqNHo7M-3C93_el-Q5F1fsr9Ylv5UXetnzAPwtEA5mM4CsK3YfMqCMdhOJ5z7esgvA6b13PMN0F4EzZv_mQW7b_PAAAA__-DuA-E"}},
	)

	// Stop node 2; note that no range had replicas on both 2 and 5.
	tc.StopServer(1)

	testutils.SucceedsSoon(t, runQuery)

	r.CheckQueryResults(t,
		"SELECT URL FROM [EXPLAIN (DISTSQL) SELECT SUM(xsquared) FROM t]",
		[][]string{{"https://cockroachdb.github.io/distsqlplan/decode.html?eJy8kkFLwzAUx-9-CvmfFHIwXZ3QUz3uoJOpJ8khNo9S6JrykoIy-t2lDaItkk02dkxe_r_fe-Ht0FhDj3pLDtkbJAQWEEihBFq2BTlneSiFhyvzgexGoGrazg_XSqCwTMh28JWvCRle9HtNG9KGGAKGvK7qEd5ytdX8mXsIrDufXeYJVC9gO_9N68XhnvuyZCq1tzPN8-vDVS6vD0b_ELvGsiEmMwGq_sRyeab_2-M5ZoTkTCPs8ZxqBf5Ab8i1tnE0W4UpTwmQKSlskbMdF_TEthjh4bgeX48XhpwPVRkOqyaUhrZ-h2U0nEzCch5OouG7uHkRDafxcHpM27fR8DJuXv7LrPqLrwAAAP__vMyldA=="}},
	)
}

func TestDistSQLDrainingHosts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numNodes = 2
	tc := serverutils.StartTestCluster(
		t,
		numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      base.TestServerArgs{UseDatabase: "test"},
		},
	)
	ctx := context.TODO()
	defer tc.Stopper().Stop(ctx)

	conn := tc.ServerConn(0)
	sqlutils.CreateTable(
		t,
		conn,
		"nums",
		"num INT",
		numNodes, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	r := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	r.DB.SetMaxOpenConns(1)

	r.Exec(t, "SET DISTSQL = ON")
	// Force the query to be distributed.
	r.Exec(
		t,
		fmt.Sprintf(`
			ALTER TABLE nums SPLIT AT VALUES (1);
			ALTER TABLE nums TESTING_RELOCATE VALUES (ARRAY[%d], 1);
		`,
			tc.Server(1).GetFirstStoreID(),
		),
	)

	const query = "SELECT COUNT(*) FROM NUMS"
	expectPlan := func(expectedPlan [][]string) {
		planQuery := fmt.Sprintf(`SELECT "URL" FROM [EXPLAIN (DISTSQL) %s]`, query)
		testutils.SucceedsSoon(t, func() error {
			resultPlan := r.QueryStr(t, planQuery)
			if !reflect.DeepEqual(resultPlan, expectedPlan) {
				return errors.Errorf("\nexpected:%v\ngot:%v", expectedPlan, resultPlan)
			}
			return nil
		})
	}

	// Verify distribution.
	expectPlan([][]string{{"https://cockroachdb.github.io/distsqlplan/decode.html?eJzEkTFr8zAQhvfvV3zclIKGyEkXTSmdMtQujkOHYoJqHcZgS-YkQUvwfy-2htQmdpuhdNRJz_u8nM6gjcJYNmhBvAIHBhHkDFoyBVprqB-HR3v1DmLNoNKtd_04Z1AYQhBncJWrEQRk8q3GFKVCAgYKnazqIbilqpH0sdO-scAg8U78j41GyDsGxrtLpHWyRBC8Yz_XPpQlYSmdmVgfk2OcndLk5bC6mzVFs6aLwGtDCgnVKD_vbuhyOD6d9nG22vH5KptRFf43u_5G-0u7vmJK0bZGW5zs_Hryuv8LVCWGj7PGU4HPZIpBE47JwA0DhdaFWx4Oex2u-oJfYb4IRyOYT-FoEb5fNm8W4e0yvL2pdt79-wwAAP__GRdW0w=="}})

	// Drain the second node and expect the query to be planned on only the
	// first node.
	distServer := tc.Server(1).DistSQLServer().(*distsqlrun.ServerImpl)
	distServer.ServerConfig.TestingKnobs.DrainFast = true
	distServer.Drain(ctx, 0 /* flowDrainWait */)

	expectPlan([][]string{{"https://cockroachdb.github.io/distsqlplan/decode.html?eJyUkEFLxDAQhe_-CnknhRy2e8xJ8bSXVuqKBwkSmyEU2kxJJqAs_e_S5qAuVNzjvMn3vjAnBHZU25ES9CsqGIUpckcpcVyi8uDgPqB3Cn2YsiyxUeg4EvQJ0stA0Dja94Faso4iFByJ7Ye1dIr9aOPnXchjgkKTRV_XHAhmVuAs35VJrCfoalb_1957H8lb4TPrQ_NcH9_a5uXp5nbTtL_E1FKaOCT65dlq3s1GgZyncsTEOXb0GLlbNWVsVm4NHCUp26oMh1BWywd_wtWf8P4MNvPVVwAAAP__856gRQ=="}})

	// Verify correctness.
	var res int
	if err := r.DB.QueryRow(query).Scan(&res); err != nil {
		t.Fatal(err)
	}
	if res != numNodes {
		t.Fatalf("expected %d rows but got %d", numNodes, res)
	}
}

// testSpanResolverRange describes a range in a test. The ranges are specified
// in order, so only the start key is needed.
type testSpanResolverRange struct {
	startKey string
	node     int
}

// testSpanResolver is a SpanResolver that uses a fixed set of ranges.
type testSpanResolver struct {
	nodes []*roachpb.NodeDescriptor

	ranges []testSpanResolverRange
}

// NewSpanResolverIterator is part of the SpanResolver interface.
func (tsr *testSpanResolver) NewSpanResolverIterator(
	_ *client.Txn,
) distsqlplan.SpanResolverIterator {
	return &testSpanResolverIterator{tsr: tsr}
}

type testSpanResolverIterator struct {
	tsr         *testSpanResolver
	curRangeIdx int
	endKey      string
}

var _ distsqlplan.SpanResolverIterator = &testSpanResolverIterator{}

// Seek is part of the SpanResolverIterator interface.
func (it *testSpanResolverIterator) Seek(
	ctx context.Context, span roachpb.Span, scanDir kv.ScanDirection,
) {
	if scanDir != kv.Ascending {
		panic("descending not implemented")
	}
	it.endKey = string(span.EndKey)
	key := string(span.Key)
	i := 0
	for ; i < len(it.tsr.ranges)-1; i++ {
		if key < it.tsr.ranges[i+1].startKey {
			break
		}
	}
	it.curRangeIdx = i
}

// Valid is part of the SpanResolverIterator interface.
func (*testSpanResolverIterator) Valid() bool {
	return true
}

// Error is part of the SpanResolverIterator interface.
func (*testSpanResolverIterator) Error() error {
	return nil
}

// NeedAnother is part of the SpanResolverIterator interface.
func (it *testSpanResolverIterator) NeedAnother() bool {
	return it.curRangeIdx < len(it.tsr.ranges)-1 &&
		it.tsr.ranges[it.curRangeIdx+1].startKey < it.endKey
}

// Next is part of the SpanResolverIterator interface.
func (it *testSpanResolverIterator) Next(_ context.Context) {
	if !it.NeedAnother() {
		panic("Next called with NeedAnother false")
	}
	it.curRangeIdx++
}

// Desc is part of the SpanResolverIterator interface.
func (it *testSpanResolverIterator) Desc() roachpb.RangeDescriptor {
	endKey := roachpb.RKeyMax
	if it.curRangeIdx < len(it.tsr.ranges)-1 {
		endKey = roachpb.RKey(it.tsr.ranges[it.curRangeIdx+1].startKey)
	}
	return roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(it.tsr.ranges[it.curRangeIdx].startKey),
		EndKey:   endKey,
	}
}

// ReplicaInfo is part of the SpanResolverIterator interface.
func (it *testSpanResolverIterator) ReplicaInfo(_ context.Context) (kv.ReplicaInfo, error) {
	n := it.tsr.nodes[it.tsr.ranges[it.curRangeIdx].node-1]
	return kv.ReplicaInfo{
		ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: n.NodeID},
		NodeDesc:          n,
	}, nil
}

func TestPartitionSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		ranges    []testSpanResolverRange
		deadNodes []int

		gatewayNode int

		// spans to be passed to partitionSpans
		spans [][2]string

		// expected result: a map of node to list of spans.
		partitions map[int][][2]string
	}{
		{
			ranges:      []testSpanResolverRange{{"A", 1}, {"B", 2}, {"C", 1}, {"D", 3}},
			gatewayNode: 1,

			spans: [][2]string{{"A1", "C1"}, {"D1", "X"}},

			partitions: map[int][][2]string{
				1: {{"A1", "B"}, {"C", "C1"}},
				2: {{"B", "C"}},
				3: {{"D1", "X"}},
			},
		},

		{
			ranges:      []testSpanResolverRange{{"A", 1}, {"B", 2}, {"C", 1}, {"D", 3}},
			deadNodes:   []int{1}, // The health status of the gateway node shouldn't matter.
			gatewayNode: 1,

			spans: [][2]string{{"A1", "C1"}, {"D1", "X"}},

			partitions: map[int][][2]string{
				1: {{"A1", "B"}, {"C", "C1"}},
				2: {{"B", "C"}},
				3: {{"D1", "X"}},
			},
		},

		{
			ranges:      []testSpanResolverRange{{"A", 1}, {"B", 2}, {"C", 1}, {"D", 3}},
			deadNodes:   []int{2},
			gatewayNode: 1,

			spans: [][2]string{{"A1", "C1"}, {"D1", "X"}},

			partitions: map[int][][2]string{
				1: {{"A1", "C1"}},
				3: {{"D1", "X"}},
			},
		},

		{
			ranges:      []testSpanResolverRange{{"A", 1}, {"B", 2}, {"C", 1}, {"D", 3}},
			deadNodes:   []int{3},
			gatewayNode: 1,

			spans: [][2]string{{"A1", "C1"}, {"D1", "X"}},

			partitions: map[int][][2]string{
				1: {{"A1", "B"}, {"C", "C1"}, {"D1", "X"}},
				2: {{"B", "C"}},
			},
		},

		{
			ranges:      []testSpanResolverRange{{"A", 1}, {"B", 2}, {"C", 1}, {"D", 3}},
			deadNodes:   []int{1},
			gatewayNode: 2,

			spans: [][2]string{{"A1", "C1"}, {"D1", "X"}},

			partitions: map[int][][2]string{
				2: {{"A1", "C1"}},
				3: {{"D1", "X"}},
			},
		},

		{
			ranges:      []testSpanResolverRange{{"A", 1}, {"B", 2}, {"C", 1}, {"D", 3}},
			deadNodes:   []int{1},
			gatewayNode: 3,

			spans: [][2]string{{"A1", "C1"}, {"D1", "X"}},

			partitions: map[int][][2]string{
				2: {{"B", "C"}},
				3: {{"A1", "B"}, {"C", "C1"}, {"D1", "X"}},
			},
		},
	}

	// We need a mock Gossip to contain addresses for the nodes. Otherwise the
	// DistSQLPlanner will not plan flows on them.
	testStopper := stop.NewStopper()
	defer testStopper.Stop(context.TODO())
	mockGossip := gossip.NewTest(roachpb.NodeID(1), nil /* rpcContext */, nil, /* grpcServer */
		testStopper, metric.NewRegistry())
	var nodeDescs []*roachpb.NodeDescriptor
	for i := 1; i <= 10; i++ {
		nodeID := roachpb.NodeID(i)
		desc := &roachpb.NodeDescriptor{
			NodeID:  nodeID,
			Address: util.UnresolvedAddr{AddressField: fmt.Sprintf("addr%d", i)},
		}
		if err := mockGossip.SetNodeDescriptor(desc); err != nil {
			t.Fatal(err)
		}
		if err := mockGossip.AddInfoProto(
			gossip.MakeDistSQLNodeVersionKey(nodeID),
			&distsqlrun.DistSQLVersionGossipInfo{
				MinAcceptedVersion: distsqlrun.MinAcceptedVersion,
				Version:            distsqlrun.Version,
			},
			0, // ttl - no expiration
		); err != nil {
			t.Fatal(err)
		}

		nodeDescs = append(nodeDescs, desc)
	}

	for testIdx, tc := range testCases {
		t.Run(strconv.Itoa(testIdx), func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())

			tsp := &testSpanResolver{
				nodes:  nodeDescs,
				ranges: tc.ranges,
			}

			dsp := DistSQLPlanner{
				planVersion:  distsqlrun.Version,
				st:           cluster.MakeTestingClusterSettings(),
				nodeDesc:     *tsp.nodes[tc.gatewayNode-1],
				stopper:      stopper,
				spanResolver: tsp,
				gossip:       mockGossip,
				testingKnobs: DistSQLPlannerTestingKnobs{
					OverrideHealthCheck: func(node roachpb.NodeID, addr string) error {
						for _, n := range tc.deadNodes {
							if int(node) == n {
								return fmt.Errorf("test node is unhealthy")
							}
						}
						return nil
					},
				},
			}

			planCtx := dsp.newPlanningCtx(context.Background(), nil /* evalCtx */, nil /* txn */)
			var spans []roachpb.Span
			for _, s := range tc.spans {
				spans = append(spans, roachpb.Span{Key: roachpb.Key(s[0]), EndKey: roachpb.Key(s[1])})
			}

			partitions, err := dsp.partitionSpans(&planCtx, spans)
			if err != nil {
				t.Fatal(err)
			}

			resMap := make(map[int][][2]string)
			for _, p := range partitions {
				if _, ok := resMap[int(p.node)]; ok {
					t.Fatalf("node %d shows up in multiple partitions", p)
				}
				var spans [][2]string
				for _, s := range p.spans {
					spans = append(spans, [2]string{string(s.Key), string(s.EndKey)})
				}
				resMap[int(p.node)] = spans
			}

			if !reflect.DeepEqual(resMap, tc.partitions) {
				t.Errorf("expected partitions:\n  %v\ngot:\n  %v", tc.partitions, resMap)
			}
		})
	}
}

// Test that span partitioning takes into account the advertised acceptable
// versions of each node. Spans for which the owner node doesn't support our
// plan's version will be planned on the gateway.
func TestPartitionSpansSkipsIncompatibleNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The spans that we're going to plan for.
	span := roachpb.Span{Key: roachpb.Key("A"), EndKey: roachpb.Key("Z")}
	gatewayNode := roachpb.NodeID(2)
	ranges := []testSpanResolverRange{{"A", 1}, {"B", 2}, {"C", 1}}

	testCases := []struct {
		// the test's name
		name string

		// planVersion is the DistSQL version that this plan is targeting.
		// We'll play with this version and expect nodes to be skipped because of
		// this.
		planVersion distsqlrun.DistSQLVersion

		// The versions accepted by each node.
		nodeVersions map[roachpb.NodeID]distsqlrun.DistSQLVersionGossipInfo

		// nodesNotAdvertisingDistSQLVersion is the set of nodes for which gossip is
		// not going to have information about the supported DistSQL version. This
		// is to simulate CRDB 1.0 nodes which don't advertise this information.
		nodesNotAdvertisingDistSQLVersion map[roachpb.NodeID]struct{}

		// expected result: a map of node to list of spans.
		partitions map[roachpb.NodeID][][2]string
	}{
		{
			// In the first test, all nodes are compatible.
			name:        "current_version",
			planVersion: 2,
			nodeVersions: map[roachpb.NodeID]distsqlrun.DistSQLVersionGossipInfo{
				1: {
					MinAcceptedVersion: 1,
					Version:            2,
				},
				2: {
					MinAcceptedVersion: 1,
					Version:            2,
				},
			},
			partitions: map[roachpb.NodeID][][2]string{
				1: {{"A", "B"}, {"C", "Z"}},
				2: {{"B", "C"}},
			},
		},
		{
			// Plan version is incompatible with node 1. We expect everything to be
			// assigned to the gateway.
			// Remember that the gateway is node 2.
			name:        "next_version",
			planVersion: 3,
			nodeVersions: map[roachpb.NodeID]distsqlrun.DistSQLVersionGossipInfo{
				1: {
					MinAcceptedVersion: 1,
					Version:            2,
				},
				2: {
					MinAcceptedVersion: 3,
					Version:            3,
				},
			},
			partitions: map[roachpb.NodeID][][2]string{
				2: {{"A", "Z"}},
			},
		},
		{
			// Like the above, except node 1 is not gossiping its version (simulating
			// a crdb 1.0 node).
			name:        "crdb_1.0",
			planVersion: 3,
			nodeVersions: map[roachpb.NodeID]distsqlrun.DistSQLVersionGossipInfo{
				2: {
					MinAcceptedVersion: 3,
					Version:            3,
				},
			},
			nodesNotAdvertisingDistSQLVersion: map[roachpb.NodeID]struct{}{
				1: {},
			},
			partitions: map[roachpb.NodeID][][2]string{
				2: {{"A", "Z"}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())

			// We need a mock Gossip to contain addresses for the nodes. Otherwise the
			// DistSQLPlanner will not plan flows on them. This Gossip will also
			// reflect tc.nodesNotAdvertisingDistSQLVersion.
			testStopper := stop.NewStopper()
			defer testStopper.Stop(context.TODO())
			mockGossip := gossip.NewTest(roachpb.NodeID(1), nil /* rpcContext */, nil, /* grpcServer */
				testStopper, metric.NewRegistry())
			var nodeDescs []*roachpb.NodeDescriptor
			for i := 1; i <= 2; i++ {
				nodeID := roachpb.NodeID(i)
				desc := &roachpb.NodeDescriptor{
					NodeID:  nodeID,
					Address: util.UnresolvedAddr{AddressField: fmt.Sprintf("addr%d", i)},
				}
				if err := mockGossip.SetNodeDescriptor(desc); err != nil {
					t.Fatal(err)
				}
				if _, ok := tc.nodesNotAdvertisingDistSQLVersion[nodeID]; !ok {
					verInfo := tc.nodeVersions[nodeID]
					if err := mockGossip.AddInfoProto(
						gossip.MakeDistSQLNodeVersionKey(nodeID),
						&verInfo,
						0, // ttl - no expiration
					); err != nil {
						t.Fatal(err)
					}
				}

				nodeDescs = append(nodeDescs, desc)
			}
			tsp := &testSpanResolver{
				nodes:  nodeDescs,
				ranges: ranges,
			}

			dsp := DistSQLPlanner{
				planVersion:  tc.planVersion,
				st:           cluster.MakeTestingClusterSettings(),
				nodeDesc:     *tsp.nodes[gatewayNode-1],
				stopper:      stopper,
				spanResolver: tsp,
				gossip:       mockGossip,
				testingKnobs: DistSQLPlannerTestingKnobs{
					OverrideHealthCheck: func(node roachpb.NodeID, addr string) error {
						// All the nodes are healthy.
						return nil
					},
				},
			}

			planCtx := dsp.newPlanningCtx(context.Background(), nil /* evalCtx */, nil /* txn */)
			partitions, err := dsp.partitionSpans(&planCtx, roachpb.Spans{span})
			if err != nil {
				t.Fatal(err)
			}

			resMap := make(map[roachpb.NodeID][][2]string)
			for _, p := range partitions {
				if _, ok := resMap[p.node]; ok {
					t.Fatalf("node %d shows up in multiple partitions", p)
				}
				var spans [][2]string
				for _, s := range p.spans {
					spans = append(spans, [2]string{string(s.Key), string(s.EndKey)})
				}
				resMap[p.node] = spans
			}

			if !reflect.DeepEqual(resMap, tc.partitions) {
				t.Errorf("expected partitions:\n  %v\ngot:\n  %v", tc.partitions, resMap)
			}
		})
	}
}

// Test that a node whose descriptor info is not accessible through gossip is
// not used. This is to simulate nodes that have been decomisioned and also
// nodes that have been "replaced" by another node at the same address (which, I
// guess, is also a type of decomissioning).
func TestPartitionSpansSkipsNodesNotInGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The spans that we're going to plan for.
	span := roachpb.Span{Key: roachpb.Key("A"), EndKey: roachpb.Key("Z")}
	gatewayNode := roachpb.NodeID(2)
	ranges := []testSpanResolverRange{{"A", 1}, {"B", 2}, {"C", 1}}

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	mockGossip := gossip.NewTest(roachpb.NodeID(1), nil /* rpcContext */, nil, /* grpcServer */
		stopper, metric.NewRegistry())
	var nodeDescs []*roachpb.NodeDescriptor
	for i := 1; i <= 2; i++ {
		nodeID := roachpb.NodeID(i)
		desc := &roachpb.NodeDescriptor{
			NodeID:  nodeID,
			Address: util.UnresolvedAddr{AddressField: fmt.Sprintf("addr%d", i)},
		}
		if i == 2 {
			if err := mockGossip.SetNodeDescriptor(desc); err != nil {
				t.Fatal(err)
			}
		}
		// All the nodes advertise their DistSQL versions. This is to simulate the
		// "node overridden by another node at the same address" case mentioned in
		// the test comment - for such a node, the descriptor would be taken out of
		// the gossip data, but other datums it advertised are left in place.
		if err := mockGossip.AddInfoProto(
			gossip.MakeDistSQLNodeVersionKey(nodeID),
			&distsqlrun.DistSQLVersionGossipInfo{
				MinAcceptedVersion: distsqlrun.MinAcceptedVersion,
				Version:            distsqlrun.Version,
			},
			0, // ttl - no expiration
		); err != nil {
			t.Fatal(err)
		}

		nodeDescs = append(nodeDescs, desc)
	}
	tsp := &testSpanResolver{
		nodes:  nodeDescs,
		ranges: ranges,
	}

	dsp := DistSQLPlanner{
		planVersion:  distsqlrun.Version,
		st:           cluster.MakeTestingClusterSettings(),
		nodeDesc:     *tsp.nodes[gatewayNode-1],
		stopper:      stopper,
		spanResolver: tsp,
		gossip:       mockGossip,
		testingKnobs: DistSQLPlannerTestingKnobs{
			OverrideHealthCheck: func(node roachpb.NodeID, addr string) error {
				// All the nodes are healthy.
				return nil
			},
		},
	}

	planCtx := dsp.newPlanningCtx(context.Background(), nil /* evalCtx */, nil /* txn */)
	partitions, err := dsp.partitionSpans(&planCtx, roachpb.Spans{span})
	if err != nil {
		t.Fatal(err)
	}

	resMap := make(map[roachpb.NodeID][][2]string)
	for _, p := range partitions {
		if _, ok := resMap[p.node]; ok {
			t.Fatalf("node %d shows up in multiple partitions", p)
		}
		var spans [][2]string
		for _, s := range p.spans {
			spans = append(spans, [2]string{string(s.Key), string(s.EndKey)})
		}
		resMap[p.node] = spans
	}

	expectedPartitions :=
		map[roachpb.NodeID][][2]string{
			2: {{"A", "Z"}},
		}
	if !reflect.DeepEqual(resMap, expectedPartitions) {
		t.Errorf("expected partitions:\n  %v\ngot:\n  %v", expectedPartitions, resMap)
	}
}

func TestCheckNodeHealth(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	const nodeID = roachpb.NodeID(5)

	mockGossip := gossip.NewTest(nodeID, nil /* rpcContext */, nil, /* grpcServer */
		stopper, metric.NewRegistry())

	desc := &roachpb.NodeDescriptor{
		NodeID:  nodeID,
		Address: util.UnresolvedAddr{NetworkField: "tcp", AddressField: "testaddr"},
	}
	if err := mockGossip.SetNodeDescriptor(desc); err != nil {
		t.Fatal(err)
	}
	if err := mockGossip.AddInfoProto(
		gossip.MakeDistSQLNodeVersionKey(nodeID),
		&distsqlrun.DistSQLVersionGossipInfo{
			MinAcceptedVersion: distsqlrun.MinAcceptedVersion,
			Version:            distsqlrun.Version,
		},
		0, // ttl - no expiration
	); err != nil {
		t.Fatal(err)
	}

	errLive := func(roachpb.NodeID) (bool, error) {
		return false, errors.New("injected liveness error")
	}
	notLive := func(roachpb.NodeID) (bool, error) {
		return false, nil
	}
	live := func(roachpb.NodeID) (bool, error) {
		return true, nil
	}

	connHealthy := func(string) error {
		return nil
	}
	connUnhealthy := func(string) error {
		return errors.New("injected conn health error")
	}
	_ = connUnhealthy

	livenessTests := []struct {
		isLive func(roachpb.NodeID) (bool, error)
		exp    string
	}{
		{live, ""},
		{errLive, "not using n5 due to liveness: injected liveness error"},
		{notLive, "not using n5 due to liveness: node is not live"},
	}

	for _, test := range livenessTests {
		t.Run("liveness", func(t *testing.T) {
			if err := checkNodeHealth(
				context.Background(), nodeID, desc.Address.AddressField,
				DistSQLPlannerTestingKnobs{}, /* knobs */
				mockGossip, connHealthy, test.isLive,
			); !testutils.IsError(err, test.exp) {
				t.Fatalf("expected %v, got %v", test.exp, err)
			}
		})
	}

	connHealthTests := []struct {
		connHealth func(string) error
		exp        string
	}{
		{connHealthy, ""},
		{connUnhealthy, "injected conn health error"},
	}

	for _, test := range connHealthTests {
		t.Run("connHealth", func(t *testing.T) {
			if err := checkNodeHealth(
				context.Background(), nodeID, desc.Address.AddressField,
				DistSQLPlannerTestingKnobs{}, /* knobs */
				mockGossip, test.connHealth, live,
			); !testutils.IsError(err, test.exp) {
				t.Fatalf("expected %v, got %v", test.exp, err)
			}
		})
	}

}
