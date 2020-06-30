// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// SplitTable splits a range in the table, creates a replica for the right
// side of the split on TargetNodeIdx, and moves the lease for the right
// side of the split to TargetNodeIdx for each SplitPoint. This forces the
// querying against the table to be distributed.
//
// TODO(radu): SplitTable or its equivalent should be added to TestCluster.
//
// TODO(radu): we should verify that the queries in tests using SplitTable
// are indeed distributed as intended.
func SplitTable(
	t *testing.T,
	tc serverutils.TestClusterInterface,
	desc *sqlbase.TableDescriptor,
	sps []SplitPoint,
) {
	if tc.ReplicationMode() != base.ReplicationManual {
		t.Fatal("SplitTable called on a test cluster that was not in manual replication mode")
	}

	rkts := make(map[roachpb.RangeID]rangeAndKT)
	for _, sp := range sps {
		pik, err := sqlbase.TestingMakePrimaryIndexKey(desc, sp.Vals...)
		if err != nil {
			t.Fatal(err)
		}

		_, rightRange, err := tc.Server(0).SplitRange(pik)
		if err != nil {
			t.Fatal(err)
		}

		rightRangeStartKey := rightRange.StartKey.AsRawKey()
		target := tc.Target(sp.TargetNodeIdx)

		rkts[rightRange.RangeID] = rangeAndKT{
			rightRange,
			serverutils.KeyAndTargets{StartKey: rightRangeStartKey, Targets: []roachpb.ReplicationTarget{target}}}
	}

	var kts []serverutils.KeyAndTargets
	for _, rkt := range rkts {
		kts = append(kts, rkt.KT)
	}
	descs, errs := tc.AddReplicasMulti(kts...)
	for _, err := range errs {
		if err != nil && !testutils.IsError(err, "is already present") {
			t.Fatal(err)
		}
	}

	for _, desc := range descs {
		rkt, ok := rkts[desc.RangeID]
		if !ok {
			continue
		}

		for _, target := range rkt.KT.Targets {
			if err := tc.TransferRangeLease(desc, target); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// SplitPoint describes a split point that is passed to SplitTable.
type SplitPoint struct {
	// TargetNodeIdx is the node that will have the lease for the new range.
	TargetNodeIdx int
	// Vals is list of values forming a primary key for the table.
	Vals []interface{}
}

type rangeAndKT struct {
	Range roachpb.RangeDescriptor
	KT    serverutils.KeyAndTargets
}

// TestPlanningDuringSplits verifies that table reader planning (resolving
// spans) tolerates concurrent splits and merges.
func TestPlanningDuringSplitsAndMerges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const n = 100
	const numNodes = 1
	tc := serverutils.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{UseDatabase: "test"},
	})

	defer tc.Stopper().Stop(context.Background())

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "t", "x INT PRIMARY KEY, xsquared INT",
		n,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, func(row int) tree.Datum {
			return tree.NewDInt(tree.DInt(row * row))
		}),
	)

	// Start a worker that continuously performs splits in the background.
	tc.Stopper().RunWorker(context.Background(), func(ctx context.Context) {
		rng, _ := randutil.NewPseudoRand()
		cdb := tc.Server(0).DB()
		for {
			select {
			case <-tc.Stopper().ShouldStop():
				return
			default:
				// Split the table at a random row.
				desc := sqlbase.TestingGetTableDescriptor(cdb, keys.SystemSQLCodec, "test", "t")

				val := rng.Intn(n)
				t.Logf("splitting at %d", val)
				pik, err := sqlbase.TestingMakePrimaryIndexKey(desc, val)
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
				t, tc.Server(0).ServingSQLAddr(), fmt.Sprintf("%d", idx), url.User(security.RootUser),
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
				rows, err := goDB.Query("SELECT sum(x), sum(xsquared) FROM t")
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

// Test that DistSQLReceiver uses inbound metadata to update the
// RangeDescriptorCache.
func TestDistSQLReceiverUpdatesCaches(t *testing.T) {
	defer leaktest.AfterTest(t)()

	size := func() int64 { return 2 << 10 }
	st := cluster.MakeTestingClusterSettings()
	rangeCache := kvcoord.NewRangeDescriptorCache(st, nil /* db */, size, stop.NewStopper())
	r := MakeDistSQLReceiver(
		context.Background(), nil /* resultWriter */, tree.Rows,
		rangeCache, nil /* txn */, nil /* updateClock */, &SessionTracing{})

	replicas := []roachpb.ReplicaDescriptor{{ReplicaID: 1}, {ReplicaID: 2}, {ReplicaID: 3}}

	descs := []roachpb.RangeDescriptor{
		{RangeID: 1, StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("c"), InternalReplicas: replicas},
		{RangeID: 2, StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("e"), InternalReplicas: replicas},
		{RangeID: 3, StartKey: roachpb.RKey("g"), EndKey: roachpb.RKey("z"), InternalReplicas: replicas},
	}

	// Push some metadata and check that the caches are updated with it.
	status := r.Push(nil /* row */, &execinfrapb.ProducerMetadata{
		Ranges: []roachpb.RangeInfo{
			{
				Desc: descs[0],
				Lease: roachpb.Lease{
					Replica: roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1},
					Start:   hlc.MinTimestamp,
				},
			},
			{
				Desc: descs[1],
				Lease: roachpb.Lease{
					Replica: roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2},
					Start:   hlc.MinTimestamp,
				},
			},
		}})
	if status != execinfra.NeedMoreRows {
		t.Fatalf("expected status NeedMoreRows, got: %d", status)
	}
	status = r.Push(nil /* row */, &execinfrapb.ProducerMetadata{
		Ranges: []roachpb.RangeInfo{
			{
				Desc: descs[2],
				Lease: roachpb.Lease{
					Replica: roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3, ReplicaID: 3},
					Start:   hlc.MinTimestamp,
				},
			},
		}})
	if status != execinfra.NeedMoreRows {
		t.Fatalf("expected status NeedMoreRows, got: %d", status)
	}

	for i := range descs {
		ri := rangeCache.GetCached(descs[i].StartKey, false /* inclusive */)
		require.NotNilf(t, ri, "failed to find range for key: %s", descs[i].StartKey)
		require.Equal(t, descs[i], ri.Desc)
		require.False(t, ri.Lease.Empty())
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
	defer tc.Stopper().Stop(context.Background())

	db0 := tc.ServerConn(0)
	sqlutils.CreateTable(t, db0, "left",
		"num INT PRIMARY KEY",
		3, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn))
	sqlutils.CreateTable(t, db0, "right",
		"num INT PRIMARY KEY",
		3, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn))

	// Disable eviction of the first range from the range cache on node 4 because
	// the unpredictable nature of those updates interferes with the expectations
	// of this test below.
	//
	// TODO(andrei): This is super hacky. What this test really wants to do is to
	// precisely control the contents of the range cache on node 4.
	tc.Server(3).DistSenderI().(*kvcoord.DistSender).DisableFirstRangeUpdates()
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
	ALTER TABLE "right" EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 1), (ARRAY[%d], 2), (ARRAY[%d], 3);
	`,
		tc.Server(1).GetFirstStoreID(),
		tc.Server(0).GetFirstStoreID(),
		tc.Server(2).GetFirstStoreID()))
	if err != nil {
		t.Fatal(err)
	}

	// Ensure that the range cache is populated (see #31235).
	_, err = db0.Exec(`SHOW RANGES FROM TABLE "right"`)
	if err != nil {
		t.Fatal(err)
	}

	// Run everything in a transaction, so we're bound on a connection on which we
	// force DistSQL.
	txn, err := db3.BeginTx(context.Background(), nil /* opts */)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("SET DISTSQL = ALWAYS"); err != nil {
		t.Fatal(err)
	}

	// Check that the initial planning is suboptimal: the cache on db3 is unaware
	// of the splits and still holds the state after the first dummy query at the
	// beginning of the test, which had everything on the first node.
	query := `SELECT count(1) FROM "left" INNER JOIN "right" USING (num)`
	row := db3.QueryRow(fmt.Sprintf(`SELECT json FROM [EXPLAIN (DISTSQL) %v]`, query))
	var json string
	if err := row.Scan(&json); err != nil {
		t.Fatal(err)
	}
	exp := `"nodeNames":["1","4"]`
	if !strings.Contains(json, exp) {
		t.Fatalf("expected json to contain %s, but json is: %s", exp, json)
	}

	// Run a non-trivial query to force the "wrong range" metadata to flow through
	// a number of components.
	row = txn.QueryRowContext(context.Background(), query)
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
	row = db3.QueryRow(fmt.Sprintf(`SELECT json FROM [EXPLAIN (DISTSQL) %v]`, query))
	if err := row.Scan(&json); err != nil {
		t.Fatal(err)
	}
	exp = `"nodeNames":["1","2","3","4"]`
	if !strings.Contains(json, exp) {
		t.Fatalf("expected json to contain %s, but json is: %s", exp, json)
	}
}

func TestDistSQLDeadHosts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("#49843. test is too slow; we need to tweak timeouts so connections die faster (see #14376)")

	const n = 100
	const numNodes = 5

	tc := serverutils.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      base.TestServerArgs{UseDatabase: "test"},
	})
	defer tc.Stopper().Stop(context.Background())

	db := tc.ServerConn(0)
	db.SetMaxOpenConns(1)
	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, "CREATE DATABASE test")

	r.Exec(t, "CREATE TABLE t (x INT PRIMARY KEY, xsquared INT)")

	for i := 0; i < numNodes; i++ {
		r.Exec(t, fmt.Sprintf("ALTER TABLE t SPLIT AT VALUES (%d)", n*i/5))
	}

	for i := 0; i < numNodes; i++ {
		r.Exec(t, fmt.Sprintf(
			"ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d,%d,%d], %d)",
			i+1, (i+1)%5+1, (i+2)%5+1, n*i/5,
		))
	}

	r.Exec(t, "SHOW RANGES FROM TABLE t")

	r.Exec(t, fmt.Sprintf("INSERT INTO t SELECT i, i*i FROM generate_series(1, %d) AS g(i)", n))

	r.Exec(t, "SET DISTSQL = ON")

	// Run a query that uses the entire table and is easy to verify.
	runQuery := func() error {
		log.Infof(context.Background(), "running test query")
		var res int
		if err := db.QueryRow("SELECT sum(xsquared) FROM t").Scan(&res); err != nil {
			return err
		}
		if exp := (n * (n + 1) * (2*n + 1)) / 6; res != exp {
			t.Fatalf("incorrect result %d, expected %d", res, exp)
		}
		log.Infof(context.Background(), "test query OK")
		return nil
	}
	if err := runQuery(); err != nil {
		t.Error(err)
	}

	// Verify the plan (should include all 5 nodes).
	r.CheckQueryResults(t,
		"SELECT url FROM [EXPLAIN (DISTSQL) SELECT sum(xsquared) FROM t]",
		[][]string{{"https://cockroachdb.github.io/distsqlplan/decode.html#eJy8k09LwzAYxu9-CnlOCu9h7bo5e5rHHXQy9SQ91OalFLamJCkoo99d1iDaIskgo8f8-T2_PG1yRC0FP-UH1kjfEYEQgzAHIQFhgYzQKFmw1lKdtlhgIz6RzghV3bTmNJ0RCqkY6RGmMntGitf8Y887zgUrEASbvNr3kkZVh1x9rQ0I29ak1-sYWUeQrflJ6-h8z0NZKi5zI0eal7fHm3V0e3b0b2JbSyVYsRgEZt2F5dFE38_jCakQT1TB4wmpMJ-ogscTUiGZqILHc6mH-E_0jnUja82jBznMywgsSrZvWctWFfysZNGH2-G2391PCNbGrkZ2sKnt0ulYf-HICccDOBrDsdvsUc-ddOKGk5BzL5zw0m1ehpjvnPDKbV6FmO_d_2rmuSbuSzZ2Z93VdwAAAP__XTV6BQ=="}},
	)

	// Stop node 5.
	tc.StopServer(4)

	testutils.SucceedsSoon(t, runQuery)

	r.CheckQueryResults(t,
		"SELECT url FROM [EXPLAIN (DISTSQL) SELECT sum(xsquared) FROM t]",
		[][]string{{"https://cockroachdb.github.io/distsqlplan/decode.html#eJy8k8FK7DAYhff3KS5npZCF6dRx7KouZ6Ejo64ki9j8lEKnKUkKytB3lzaItkg60qHL5M93vpySHlFpRQ_yQBbJKzgYIjCswBBDMNRGZ2StNt3YH96qdyRXDEVVN67bFgyZNoTkCFe4kpDgWb6VtCepyIBBkZNF2QtqUxyk-UgdGHaNS_6nEUTLoBv3lday0z13eW4ol06PNE8v9xcpvzw5-juxqbRRZEgNAkV7Zjlf6PtNeOZUiBaqMOGZU2G1UIUJz7le8S_Re7K1riyNXvMwTzCQysn_CFY3JqNHo7M-3C93_el-Q5F1fsr9Ylv5UXetnzAPwtEA5mM4CsK3YfMqCMdhOJ5z7esgvA6b13PMN0F4EzZv_mQW7b_PAAAA__-DuA-E"}},
	)

	// Stop node 2; note that no range had replicas on both 2 and 5.
	tc.StopServer(1)

	testutils.SucceedsSoon(t, runQuery)

	r.CheckQueryResults(t,
		"SELECT url FROM [EXPLAIN (DISTSQL) SELECT sum(xsquared) FROM t]",
		[][]string{{"https://cockroachdb.github.io/distsqlplan/decode.html#eJy8kkFLwzAUx-9-CvmfFHIwXZ3QUz3uoJOpJ8khNo9S6JrykoIy-t2lDaItkk02dkxe_r_fe-Ht0FhDj3pLDtkbJAQWEEihBFq2BTlneSiFhyvzgexGoGrazg_XSqCwTMh28JWvCRle9HtNG9KGGAKGvK7qEd5ytdX8mXsIrDufXeYJVC9gO_9N68XhnvuyZCq1tzPN8-vDVS6vD0b_ELvGsiEmMwGq_sRyeab_2-M5ZoTkTCPs8ZxqBf5Ab8i1tnE0W4UpTwmQKSlskbMdF_TEthjh4bgeX48XhpwPVRkOqyaUhrZ-h2U0nEzCch5OouG7uHkRDafxcHpM27fR8DJuXv7LrPqLrwAAAP__vMyldA=="}},
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
			ServerArgs:      base.TestServerArgs{Knobs: base.TestingKnobs{DistSQL: &execinfra.TestingKnobs{DrainFast: true}}, UseDatabase: "test"},
		},
	)
	ctx := context.Background()
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

	db := tc.ServerConn(0)
	db.SetMaxOpenConns(1)
	r := sqlutils.MakeSQLRunner(db)

	// Force the query to be distributed.
	r.Exec(t, "SET DISTSQL = ON")

	// Shortly after starting a cluster, the first server's StorePool may not be
	// fully initialized and ready to do rebalancing yet, so wrap this in a
	// SucceedsSoon.
	testutils.SucceedsSoon(t, func() error {
		_, err := db.Exec(
			fmt.Sprintf(`ALTER TABLE nums SPLIT AT VALUES (1);
									 ALTER TABLE nums EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 1);`,
				tc.Server(1).GetFirstStoreID(),
			),
		)
		return err
	})

	// Ensure that the range cache is populated (see #31235).
	r.Exec(t, "SHOW RANGES FROM TABLE nums")

	const query = "SELECT count(*) FROM NUMS"
	expectPlan := func(expectedPlan [][]string) {
		planQuery := fmt.Sprintf(`SELECT url FROM [EXPLAIN (DISTSQL) %s]`, query)
		testutils.SucceedsSoon(t, func() error {
			resultPlan := r.QueryStr(t, planQuery)
			if !reflect.DeepEqual(resultPlan, expectedPlan) {
				return errors.Errorf("\nexpected:%v\ngot:%v", expectedPlan, resultPlan)
			}
			return nil
		})
	}

	// Verify distribution.
	expectPlan([][]string{{"https://cockroachdb.github.io/distsqlplan/decode.html#eJyskd-Lm0AQx9_7V8g8mbKHWZO-7NMd1xSEnF7Vo4UgYesOIphduz-gJfi_F7WQGBKblj46O9_5fJw5gvneAINss908557TjfcpTV683ebr6_Ypij3_Y5Tl2eftwvvdUionrf9-MfZJdzAFEJBKYMwPaIDtgAKBEAoCrVYlGqN0Xz4OTZH4AWxJoJats325IFAqjcCOYGvbIDDI-bcGU-QCdbAEAgItr5thdKvrA9c_H3suEMhaLg3zHoIemTjLvFhJhKIjoJw9EYzlFQKjHbnf4qmqNFbcKh2EU4nn5C3O92nyJfMXN1nhTdYJ4aTSAjWKyfyim7dZT22yt5d9FOf-I70ts5rI0PvXT-9Zf0Af_mH9f7A4--HVf13_FVaKplXS4MUZrk9e9udBUeF4S6OcLvFVq3LAjJ_JkBsKAo0dX-n4EcnxqRc8D9PZcDgJ08twOBv-ME9ezYbX8-H1X2kX3btfAQAA__9aiHOO"}})

	// Drain the second node and expect the query to be planned on only the
	// first node.
	distServer := tc.Server(1).DistSQLServer().(*distsql.ServerImpl)
	distServer.Drain(ctx, 0 /* flowDrainWait */, nil /* reporter */)

	expectPlan([][]string{{"https://cockroachdb.github.io/distsqlplan/decode.html#eJyUkM9Kw0AYxO8-xTKnVlba9LgnS60QqElNIgolyJp8hEC6G_cPKCHvLkkErVDR4843M79hO9jXBgLpdrfdZMybht0m8R07bJ_2u3UYsdlNmGbp_W7OPi2F9srNLueTT_mjzcGhdEmRPJKFOCBAztEaXZC12gxSNxrC8g1iyVGr1rtBzjkKbQiig6tdQxDI5EtDCcmSzGIJjpKcrJuxtjX1UZr364EJjrSVygp2BY7YO8EirQh5z6G9--q3TlYEEfT87xvWVWWokk6bRXA6YRM_RNlzEj-ms_lZ1uo_rIRsq5WlE8655mWfc1BZ0fSnVntT0N7oYsRMz3jMjUJJ1k3XYHqEajoNA7-Hg1_Dqx_hvL_4CAAA__-ln7ge"}})

	// Verify correctness.
	var res int
	if err := db.QueryRow(query).Scan(&res); err != nil {
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
func (tsr *testSpanResolver) NewSpanResolverIterator(_ *kv.Txn) physicalplan.SpanResolverIterator {
	return &testSpanResolverIterator{tsr: tsr}
}

type testSpanResolverIterator struct {
	tsr         *testSpanResolver
	curRangeIdx int
	endKey      string
}

var _ physicalplan.SpanResolverIterator = &testSpanResolverIterator{}

// Seek is part of the SpanResolverIterator interface.
func (it *testSpanResolverIterator) Seek(
	ctx context.Context, span roachpb.Span, scanDir kvcoord.ScanDirection,
) {
	if scanDir != kvcoord.Ascending {
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
func (it *testSpanResolverIterator) ReplicaInfo(
	_ context.Context,
) (roachpb.ReplicaDescriptor, error) {
	n := it.tsr.nodes[it.tsr.ranges[it.curRangeIdx].node-1]
	return roachpb.ReplicaDescriptor{NodeID: n.NodeID}, nil
}

func TestPartitionSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		ranges    []testSpanResolverRange
		deadNodes []int

		gatewayNode int

		// spans to be passed to PartitionSpans
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
	defer testStopper.Stop(context.Background())
	mockGossip := gossip.NewTest(roachpb.NodeID(1), nil /* rpcContext */, nil, /* grpcServer */
		testStopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
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
			&execinfrapb.DistSQLVersionGossipInfo{
				MinAcceptedVersion: execinfra.MinAcceptedVersion,
				Version:            execinfra.Version,
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
			defer stopper.Stop(context.Background())

			tsp := &testSpanResolver{
				nodes:  nodeDescs,
				ranges: tc.ranges,
			}

			gw := gossip.MakeExposedGossip(mockGossip)
			dsp := DistSQLPlanner{
				planVersion:   execinfra.Version,
				st:            cluster.MakeTestingClusterSettings(),
				gatewayNodeID: tsp.nodes[tc.gatewayNode-1].NodeID,
				stopper:       stopper,
				spanResolver:  tsp,
				gossip:        gw,
				nodeHealth: distSQLNodeHealth{
					gossip: gw,
					connHealth: func(node roachpb.NodeID, _ rpc.ConnectionClass) error {
						for _, n := range tc.deadNodes {
							if int(node) == n {
								return fmt.Errorf("test node is unhealthy")
							}
						}
						return nil
					},
					isLive: func(nodeID roachpb.NodeID) (bool, error) {
						return true, nil
					},
				},
			}

			planCtx := dsp.NewPlanningCtx(context.Background(), &extendedEvalContext{
				EvalContext: tree.EvalContext{Codec: keys.SystemSQLCodec},
			}, nil /* txn */, true /* distribute */)
			var spans []roachpb.Span
			for _, s := range tc.spans {
				spans = append(spans, roachpb.Span{Key: roachpb.Key(s[0]), EndKey: roachpb.Key(s[1])})
			}

			partitions, err := dsp.PartitionSpans(planCtx, spans)
			if err != nil {
				t.Fatal(err)
			}

			resMap := make(map[int][][2]string)
			for _, p := range partitions {
				if _, ok := resMap[int(p.Node)]; ok {
					t.Fatalf("node %d shows up in multiple partitions", p)
				}
				var spans [][2]string
				for _, s := range p.Spans {
					spans = append(spans, [2]string{string(s.Key), string(s.EndKey)})
				}
				resMap[int(p.Node)] = spans
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
		planVersion execinfrapb.DistSQLVersion

		// The versions accepted by each node.
		nodeVersions map[roachpb.NodeID]execinfrapb.DistSQLVersionGossipInfo

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
			nodeVersions: map[roachpb.NodeID]execinfrapb.DistSQLVersionGossipInfo{
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
			nodeVersions: map[roachpb.NodeID]execinfrapb.DistSQLVersionGossipInfo{
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
			nodeVersions: map[roachpb.NodeID]execinfrapb.DistSQLVersionGossipInfo{
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
			defer stopper.Stop(context.Background())

			// We need a mock Gossip to contain addresses for the nodes. Otherwise the
			// DistSQLPlanner will not plan flows on them. This Gossip will also
			// reflect tc.nodesNotAdvertisingDistSQLVersion.
			testStopper := stop.NewStopper()
			defer testStopper.Stop(context.Background())
			mockGossip := gossip.NewTest(roachpb.NodeID(1), nil /* rpcContext */, nil, /* grpcServer */
				testStopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
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

			gw := gossip.MakeExposedGossip(mockGossip)
			dsp := DistSQLPlanner{
				planVersion:   tc.planVersion,
				st:            cluster.MakeTestingClusterSettings(),
				gatewayNodeID: tsp.nodes[gatewayNode-1].NodeID,
				stopper:       stopper,
				spanResolver:  tsp,
				gossip:        gw,
				nodeHealth: distSQLNodeHealth{
					gossip: gw,
					connHealth: func(roachpb.NodeID, rpc.ConnectionClass) error {
						// All the nodes are healthy.
						return nil
					},
					isLive: func(roachpb.NodeID) (bool, error) {
						return true, nil
					},
				},
			}

			planCtx := dsp.NewPlanningCtx(context.Background(), &extendedEvalContext{
				EvalContext: tree.EvalContext{Codec: keys.SystemSQLCodec},
			}, nil /* txn */, true /* distribute */)
			partitions, err := dsp.PartitionSpans(planCtx, roachpb.Spans{span})
			if err != nil {
				t.Fatal(err)
			}

			resMap := make(map[roachpb.NodeID][][2]string)
			for _, p := range partitions {
				if _, ok := resMap[p.Node]; ok {
					t.Fatalf("node %d shows up in multiple partitions", p)
				}
				var spans [][2]string
				for _, s := range p.Spans {
					spans = append(spans, [2]string{string(s.Key), string(s.EndKey)})
				}
				resMap[p.Node] = spans
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
	defer stopper.Stop(context.Background())

	mockGossip := gossip.NewTest(roachpb.NodeID(1), nil /* rpcContext */, nil, /* grpcServer */
		stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
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
			&execinfrapb.DistSQLVersionGossipInfo{
				MinAcceptedVersion: execinfra.MinAcceptedVersion,
				Version:            execinfra.Version,
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

	gw := gossip.MakeExposedGossip(mockGossip)
	dsp := DistSQLPlanner{
		planVersion:   execinfra.Version,
		st:            cluster.MakeTestingClusterSettings(),
		gatewayNodeID: tsp.nodes[gatewayNode-1].NodeID,
		stopper:       stopper,
		spanResolver:  tsp,
		gossip:        gw,
		nodeHealth: distSQLNodeHealth{
			gossip: gw,
			connHealth: func(node roachpb.NodeID, _ rpc.ConnectionClass) error {
				_, err := mockGossip.GetNodeIDAddress(node)
				return err
			},
			isLive: func(roachpb.NodeID) (bool, error) {
				return true, nil
			},
		},
	}

	planCtx := dsp.NewPlanningCtx(context.Background(), &extendedEvalContext{
		EvalContext: tree.EvalContext{Codec: keys.SystemSQLCodec},
	}, nil /* txn */, true /* distribute */)
	partitions, err := dsp.PartitionSpans(planCtx, roachpb.Spans{span})
	if err != nil {
		t.Fatal(err)
	}

	resMap := make(map[roachpb.NodeID][][2]string)
	for _, p := range partitions {
		if _, ok := resMap[p.Node]; ok {
			t.Fatalf("node %d shows up in multiple partitions", p)
		}
		var spans [][2]string
		for _, s := range p.Spans {
			spans = append(spans, [2]string{string(s.Key), string(s.EndKey)})
		}
		resMap[p.Node] = spans
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
	defer stopper.Stop(context.Background())

	const nodeID = roachpb.NodeID(5)

	mockGossip := gossip.NewTest(nodeID, nil /* rpcContext */, nil, /* grpcServer */
		stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())

	desc := &roachpb.NodeDescriptor{
		NodeID:  nodeID,
		Address: util.UnresolvedAddr{NetworkField: "tcp", AddressField: "testaddr"},
	}
	if err := mockGossip.SetNodeDescriptor(desc); err != nil {
		t.Fatal(err)
	}
	if err := mockGossip.AddInfoProto(
		gossip.MakeDistSQLNodeVersionKey(nodeID),
		&execinfrapb.DistSQLVersionGossipInfo{
			MinAcceptedVersion: execinfra.MinAcceptedVersion,
			Version:            execinfra.Version,
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

	connHealthy := func(roachpb.NodeID, rpc.ConnectionClass) error {
		return nil
	}
	connUnhealthy := func(roachpb.NodeID, rpc.ConnectionClass) error {
		return errors.New("injected conn health error")
	}
	_ = connUnhealthy

	livenessTests := []struct {
		isLive func(roachpb.NodeID) (bool, error)
		exp    string
	}{
		{live, ""},
		{errLive, "not using n5 due to liveness: injected liveness error"},
		{notLive, "not using n5 due to liveness: node n5 is not live"},
	}

	gw := gossip.MakeExposedGossip(mockGossip)
	for _, test := range livenessTests {
		t.Run("liveness", func(t *testing.T) {
			h := distSQLNodeHealth{
				gossip:     gw,
				connHealth: connHealthy,
				isLive:     test.isLive,
			}
			if err := h.check(context.Background(), nodeID); !testutils.IsError(err, test.exp) {
				t.Fatalf("expected %v, got %v", test.exp, err)
			}
		})
	}

	connHealthTests := []struct {
		connHealth func(roachpb.NodeID, rpc.ConnectionClass) error
		exp        string
	}{
		{connHealthy, ""},
		{connUnhealthy, "injected conn health error"},
	}

	for _, test := range connHealthTests {
		t.Run("connHealth", func(t *testing.T) {
			h := distSQLNodeHealth{
				gossip:     gw,
				connHealth: test.connHealth,
				isLive:     live,
			}
			if err := h.check(context.Background(), nodeID); !testutils.IsError(err, test.exp) {
				t.Fatalf("expected %v, got %v", test.exp, err)
			}
		})
	}
}
