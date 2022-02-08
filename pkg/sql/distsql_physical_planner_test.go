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
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	t *testing.T, tc serverutils.TestClusterInterface, desc catalog.TableDescriptor, sps []SplitPoint,
) {
	if tc.ReplicationMode() != base.ReplicationManual {
		t.Fatal("SplitTable called on a test cluster that was not in manual replication mode")
	}

	rkts := make(map[roachpb.RangeID]rangeAndKT)
	for _, sp := range sps {
		pik, err := randgen.TestingMakePrimaryIndexKey(desc, sp.Vals...)
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
	descs, errs := tc.AddVotersMulti(kts...)
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
	defer log.Scope(t).Close(t)

	const n = 100
	const numNodes = 1
	tc := serverutils.StartNewTestCluster(t, numNodes, base.TestClusterArgs{
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

	ctx := tc.Server(0).AnnotateCtx(context.Background())

	// Start a worker that continuously performs splits in the background.
	_ = tc.Stopper().RunAsyncTask(ctx, "splitter", func(ctx context.Context) {
		rng, _ := randutil.NewTestRand()
		cdb := tc.Server(0).DB()
		for {
			select {
			case <-tc.Stopper().ShouldQuiesce():
				return
			default:
				// Split the table at a random row.
				tableDesc := desctestutils.TestingGetTableDescriptor(
					cdb, keys.SystemSQLCodec, "test", "public", "t",
				)

				val := rng.Intn(n)
				t.Logf("splitting at %d", val)
				pik, err := randgen.TestingMakePrimaryIndexKey(tableDesc, val)
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	size := func() int64 { return 2 << 10 }
	st := cluster.MakeTestingClusterSettings()
	tr := tracing.NewTracer()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	rangeCache := rangecache.NewRangeCache(st, nil /* db */, size, stopper, tr)
	r := MakeDistSQLReceiver(
		ctx,
		&errOnlyResultWriter{}, /* resultWriter */
		tree.Rows,
		rangeCache,
		nil, /* txn */
		nil, /* clockUpdater */
		&SessionTracing{},
		nil, /* contentionRegistry */
		nil, /* testingPushCallback */
	)

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
					Replica:  roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1},
					Start:    hlc.MinClockTimestamp,
					Sequence: 1,
				},
			},
			{
				Desc: descs[1],
				Lease: roachpb.Lease{
					Replica:  roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2},
					Start:    hlc.MinClockTimestamp,
					Sequence: 1,
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
					Replica:  roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3, ReplicaID: 3},
					Start:    hlc.MinClockTimestamp,
					Sequence: 1,
				},
			},
		}})
	if status != execinfra.NeedMoreRows {
		t.Fatalf("expected status NeedMoreRows, got: %d", status)
	}

	for i := range descs {
		ri := rangeCache.GetCached(ctx, descs[i].StartKey, false /* inclusive */)
		require.NotNilf(t, ri, "failed to find range for key: %s", descs[i].StartKey)
		require.Equal(t, &descs[i], ri.Desc())
		require.NotNil(t, ri.Lease())
	}
}

// Test that a gateway improves the physical plans that it generates as a result
// of running a badly-planned query and receiving range information in response;
// this range information is used to update caches on the gateway.
func TestDistSQLRangeCachesIntegrationTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to setup a cluster with 4 nodes. The last one will not be a
	// target of any replication so that its caches stay virgin.

	tc := serverutils.StartNewTestCluster(t, 4, /* numNodes */
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
	// Force the DistSQL on this connection.
	_, err := db3.Exec(`SET CLUSTER SETTING sql.defaults.distsql = always; SET distsql = always`)
	if err != nil {
		t.Fatal(err)
	}
	// Do a query on node 4 so that it populates the its cache with an initial
	// descriptor containing all the SQL key space. If we don't do this, the state
	// of the cache is left at the whim of gossiping the first descriptor done
	// during cluster startup - it can happen that the cache remains empty, which
	// is not what this test wants.
	_, err = db3.Exec(`SELECT * FROM "left"`)
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

	// Check that the initial planning is suboptimal: the cache on db3 is unaware
	// of the splits and still holds the state after the first dummy query at the
	// beginning of the test, which had everything on the first node.
	query := `SELECT count(1) FROM "left" INNER JOIN "right" USING (num)`
	row := db3.QueryRow(fmt.Sprintf(`EXPLAIN (DISTSQL, JSON) %v`, query))
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
	row = db3.QueryRow(fmt.Sprintf(`EXPLAIN (DISTSQL, JSON) %v`, query))
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
	defer log.Scope(t).Close(t)

	skip.UnderShort(t, "takes 20s")

	const n = 100
	const numNodes = 5

	tc := serverutils.StartNewTestCluster(t, numNodes, base.TestClusterArgs{
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

	// Evenly spread the ranges between the first 4 nodes. Only the last range
	// has a replica on the fifth node.
	for i := 0; i < numNodes; i++ {
		r.Exec(t, fmt.Sprintf(
			"ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d,%d,%d], %d)",
			i+1, (i+1)%4+1, (i+2)%4+1, n*i/5,
		))
	}
	r.CheckQueryResults(t,
		"SELECT start_key, end_key, lease_holder, replicas FROM [SHOW RANGES FROM TABLE t]",
		[][]string{
			{"NULL", "/0", "1", "{1}"},
			{"/0", "/20", "1", "{1,2,3}"},
			{"/20", "/40", "2", "{2,3,4}"},
			{"/40", "/60", "3", "{1,3,4}"},
			{"/60", "/80", "4", "{1,2,4}"},
			{"/80", "NULL", "5", "{2,3,5}"},
		},
	)

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
		"SELECT info FROM [EXPLAIN (DISTSQL) SELECT sum(xsquared) FROM t] WHERE info LIKE 'Diagram%'",
		[][]string{{"Diagram: https://cockroachdb.github.io/distsqlplan/decode.html#eJyslG-Lm0AQxt_3U8hAuQQ2uBqT83x1x52l0tyfxpQWDinbOBWpcb3dFa6EfPeitsSEu9XGe-nuPPM8v3GZLcinDDwI_YV_vTLS_Cc3Pizvb41H_9vD4iq4M0Y3QbgKPy_Gxt8aWW5Gz_KpZALjcVOsIuPrR3_pN_pF8Mk3zm5Slgi2eX8GBHIe4x3boATvESwgYAOBKRBwgMAMIgKF4GuUkouqZFsLgvgZPEogzYtSVccRgTUXCN4WVKoyBA9W7EeGS2QxCpMCgRgVS7PaRl2q78Uv_A0ErnlWbnLpGf9iA4GwYNXJxLQpRDsCvFR7G6lYguBZO9I_ylWSCEyY4sKcHSYJv9yOLq3xqzb2qzb77mXORYxN9H3raKcPYtH_SzI9SGL1n711yuxNm05Mp-_4O9K0qOdDxm_3h7ZPgnboxJz3he5I04I-HwI97Q89PQl6Tiem2xe6I00L2h0C7fSHdk6CdumkJ3FHlBbxxVutlhdsligLnks8WjEvd6bV6sE4wWZPSV6KNT4Ivq5tms_7WlcfxChVc2s1H0HeXFUB22JLK7YPxNax2NY7d1hPtWpHL3aG5J5pxXO983yI87lW7Oqd3SHOF_p_RTueif6RHXtHu3d_AgAA___Fl-el"}},
	)

	// Stop node 5.
	tc.StopServer(4)

	testutils.SucceedsSoon(t, runQuery)

	// The leaseholder for the last range should have moved to either node 2 or 3.
	query := "SELECT info FROM [EXPLAIN (DISTSQL) SELECT sum(xsquared) FROM t] WHERE info LIKE 'Diagram%'"
	exp2 := [][]string{{"Diagram: https://cockroachdb.github.io/distsqlplan/decode.html#eJysk29r2zAQxt_vU4iD0QQUbDmum_lVS-sxs_TP4owNihladPPMHMuVZOgI-e7D9kaS0ipuspeW7rnn95x8K9APBYSQRNPock7y8ock72e31-Q--no3vYhvyOAqTubJp-mQ_K3R9XLwqB9qrlAMu2KTki8folnU6afxx4icXOU8U3z59gQolFLgDV-ihvAeGFDwgMIYKPiQUqiUXKDWUjXXq7Y4Fo8QuhTysqpNc5xSWEiFEK7A5KZACGHOvxc4Qy5QOS5QEGh4XrQW5tx8q37hb6BwKYt6WeqQ_EMGCknFm5OR47mQrinI2mxstOEZQsjWtD_KRZYpzLiRyvF3SZLP14NzNnzRxnvRZtO9LqUS2KFvWqdrO8jkdSDjHRDWf_TskNE7njtyfJfwUhBGpPmJqudL7CHbGsDpMS_h9R-Ad9AAfHfkBH1_vz00W6GDY0KP-4ceHxQ6cEfOpG_oPTRboc_-1849YzNDXclS45Pde76z2-wkigy7BdayVgu8U3LR2nSft62uPRCoTXfLuo-47K4awG0xs4q9HTF7Kvas4nd257FV7NvF_jHYp1ZxYHcOjnE-s4ondufJq5zT9Zs_AQAA__9QI3J0"}}
	exp3 := [][]string{{"Diagram: https://cockroachdb.github.io/distsqlplan/decode.html#eJysk29r2zAQxt_vU4iD0QQUbDmum_lVS-sxs_TP4owNihladPPMHMuVZOgI-e7D9kaS0ipuspfW3XPP7x5zK9APBYSQRNPock7y8ock72e31-Q--no3vYhvyOAqTubJp-mQ_O3R9XLwqB9qrlAMu2aTki8folnU6afxx4icXOU8U3z59gQolFLgDV-ihvAeGFDwgMIYKPiQUqiUXKDWUjXlVdsci0cIXQp5WdWmeU4pLKRCCFdgclMghDDn3wucIReoHBcoCDQ8L1oLc26-Vb_wN1C4lEW9LHVI_iEDhaTizcvI8VxI1xRkbTY22vAMIWRr2h_lIssUZtxI5fi7JMnn68E5G75o471os5lel1IJ7NA3o9O1HWTyOpDxDgjrHz07JHrHc0eO3zf9PTRbS58ek77Xf2nvoKV9d-QELuGlIIxI8xNVzwD2kG0FEBwTwLh_AOODAgjckTPp-9f30Gwtffa_bu4ZmxnqSpYan9ze85Pd5iZRZNgdsJa1WuCdkovWpvu8bXXtg0BtuirrPuKyKzWA22JmFXs7YvZU7FnF7-zOY6vYt4v9Y7BPreLA7hwc43xmFU_szpNXOafrN38CAAD__0dtcnQ="}}

	res := r.QueryStr(t, query)
	if !reflect.DeepEqual(res, exp2) && !reflect.DeepEqual(res, exp3) {
		t.Errorf("query '%s': expected:\neither\n%vor\n%v\ngot:\n%v\n",
			query, sqlutils.MatrixToStr(exp2), sqlutils.MatrixToStr(exp3), sqlutils.MatrixToStr(res),
		)
	}

	// Stop node 4; note that no range had replicas on both 4 and 5.
	tc.StopServer(3)

	testutils.SucceedsSoon(t, runQuery)
}

func TestDistSQLDrainingHosts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numNodes = 2
	tc := serverutils.StartNewTestCluster(
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
		t.Helper()
		planQuery := fmt.Sprintf(`SELECT info FROM [EXPLAIN (DISTSQL) %s] WHERE info LIKE 'Diagram%%'`, query)
		testutils.SucceedsSoon(t, func() error {
			resultPlan := r.QueryStr(t, planQuery)
			if !reflect.DeepEqual(resultPlan, expectedPlan) {
				return errors.Errorf("\nexpected:%v\ngot:%v", expectedPlan, resultPlan)
			}
			return nil
		})
	}

	// Verify distribution.
	expectPlan([][]string{{"Diagram: https://cockroachdb.github.io/distsqlplan/decode.html#eJyskVFr2zAQx9_3KcTBaDJUEjnZi55aUo-ZpXFnu3RQTNCsmzGzJU-SYSP4uw_bg9al8RLoi0HS_e7_890B7K8SOMT-1t8kpFA_NPkUhbfk0f92t70OdmR2E8RJ_HU7J_9qMt0oN_swH-pUU9mUPHz2I3-gt8EXn1zcFCI3onp_ARSUlrgTFVrgj8CAggcphdroDK3Vprs-9EWB_A18SaFQdeO665RCpg0CP4ArXInAIRHfS4xQSDSLJVCQ6ERR9q07lavus69_4h-gsNFlUynLgUJcC2U5uVwwSFsKunFPEdaJHIGzlp6ucZ3nBnPhtFl4Y4tNeL9L9lH4EM_mR7O8o1lPEY3SRqJBOeqfttM267FNfH-7D3bJ7Iodl1mNZNjp82dnz3_BLk-c_380nv3x6k3n_0pWhLbWyuKLPbzeedntB2WOwzKtbkyGd0ZnfcxwDHuuv5Bo3fDKhkOghqdO8DnMJmFvBLOXsDcJf5xOXk3C62l4fZZ22r77GwAA__-hBX17"}})

	// Drain the second node and expect the query to be planned on only the
	// first node.
	distServer := tc.Server(1).DistSQLServer().(*distsql.ServerImpl)
	distServer.Drain(ctx, 0 /* flowDrainWait */, nil /* reporter */)

	expectPlan([][]string{{"Diagram: https://cockroachdb.github.io/distsqlplan/decode.html#eJyUkF9LwzAUxd_9FOGCbJPI1j3mybFVLNZ1tpUJo4zYXkuxTWr-gDL63aWNoBMm-hK4555zf4ccQL_WwCDxQ3-Zkko8S3IdR3dk5z9uwkWwJuNVkKTJfTghn55cWmHGFxPnE7bRGdne-LHv0mFw65PRquKl4s35CCgIWeCaN6iB7cCDjEKrZI5aS9VLh8EQFG_AZhQq0VrTyxmFXCoEdgBTmRqBQcqfaoyRF6imM6BQoOFVPZzta1z1z759wXegsJS1bYRmQCFpudCMXELWUZDWfAG04SUC8zr69xKLslRYciPV1DvusIwe1uk-jrbJeHKSNf8PK0bdSqHxiHPq8qzLKGBRovtULa3KcaNkPmDcGA25QShQG7f13BAIt-oLfg97v4bnP8JZd_YRAAD___rywTs="}})

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
	defer log.Scope(t).Close(t)

	testCases := []struct {
		ranges    []testSpanResolverRange
		deadNodes []int

		gatewayNode int

		// spans to be passed to PartitionSpans. If the second string is empty,
		// the span is actually a point lookup.
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

		// Test point lookups in isolation.
		{
			ranges:      []testSpanResolverRange{{"A", 1}, {"B", 2}},
			gatewayNode: 1,

			spans: [][2]string{{"A2", ""}, {"A1", ""}, {"B1", ""}},

			partitions: map[int][][2]string{
				1: {{"A2", ""}, {"A1", ""}},
				2: {{"B1", ""}},
			},
		},

		// Test point lookups intertwined with span scans.
		{
			ranges:      []testSpanResolverRange{{"A", 1}, {"B", 1}, {"C", 2}},
			gatewayNode: 1,

			spans: [][2]string{{"A1", ""}, {"A1", "A2"}, {"A2", ""}, {"A2", "C2"}, {"B1", ""}, {"A3", "B3"}, {"B2", ""}},

			partitions: map[int][][2]string{
				1: {{"A1", ""}, {"A1", "A2"}, {"A2", ""}, {"A2", "C"}, {"B1", ""}, {"A3", "B3"}, {"B2", ""}},
				2: {{"C", "C2"}},
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
		sqlInstanceID := base.SQLInstanceID(i)
		desc := &roachpb.NodeDescriptor{
			NodeID:  roachpb.NodeID(sqlInstanceID),
			Address: util.UnresolvedAddr{AddressField: fmt.Sprintf("addr%d", i)},
		}
		if err := mockGossip.SetNodeDescriptor(desc); err != nil {
			t.Fatal(err)
		}
		if err := mockGossip.AddInfoProto(
			gossip.MakeDistSQLNodeVersionKey(sqlInstanceID),
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

			gw := gossip.MakeOptionalGossip(mockGossip)
			dsp := DistSQLPlanner{
				planVersion:          execinfra.Version,
				st:                   cluster.MakeTestingClusterSettings(),
				gatewaySQLInstanceID: base.SQLInstanceID(tsp.nodes[tc.gatewayNode-1].NodeID),
				stopper:              stopper,
				spanResolver:         tsp,
				gossip:               gw,
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
					isAvailable: func(base.SQLInstanceID) bool {
						return true
					},
				},
				codec: keys.SystemSQLCodec,
			}

			planCtx := dsp.NewPlanningCtx(context.Background(), &extendedEvalContext{
				EvalContext: tree.EvalContext{Codec: keys.SystemSQLCodec},
			}, nil, nil, DistributionTypeSystemTenantOnly)
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
				if _, ok := resMap[int(p.SQLInstanceID)]; ok {
					t.Fatalf("node %d shows up in multiple partitions", p)
				}
				var spans [][2]string
				for _, s := range p.Spans {
					spans = append(spans, [2]string{string(s.Key), string(s.EndKey)})
				}
				resMap[int(p.SQLInstanceID)] = spans
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
	defer log.Scope(t).Close(t)

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
		nodeVersions map[base.SQLInstanceID]execinfrapb.DistSQLVersionGossipInfo

		// nodesNotAdvertisingDistSQLVersion is the set of nodes for which gossip is
		// not going to have information about the supported DistSQL version. This
		// is to simulate CRDB 1.0 nodes which don't advertise this information.
		nodesNotAdvertisingDistSQLVersion map[base.SQLInstanceID]struct{}

		// expected result: a map of node to list of spans.
		partitions map[base.SQLInstanceID][][2]string
	}{
		{
			// In the first test, all nodes are compatible.
			name:        "current_version",
			planVersion: 2,
			nodeVersions: map[base.SQLInstanceID]execinfrapb.DistSQLVersionGossipInfo{
				1: {
					MinAcceptedVersion: 1,
					Version:            2,
				},
				2: {
					MinAcceptedVersion: 1,
					Version:            2,
				},
			},
			partitions: map[base.SQLInstanceID][][2]string{
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
			nodeVersions: map[base.SQLInstanceID]execinfrapb.DistSQLVersionGossipInfo{
				1: {
					MinAcceptedVersion: 1,
					Version:            2,
				},
				2: {
					MinAcceptedVersion: 3,
					Version:            3,
				},
			},
			partitions: map[base.SQLInstanceID][][2]string{
				2: {{"A", "Z"}},
			},
		},
		{
			// Like the above, except node 1 is not gossiping its version (simulating
			// a crdb 1.0 node).
			name:        "crdb_1.0",
			planVersion: 3,
			nodeVersions: map[base.SQLInstanceID]execinfrapb.DistSQLVersionGossipInfo{
				2: {
					MinAcceptedVersion: 3,
					Version:            3,
				},
			},
			nodesNotAdvertisingDistSQLVersion: map[base.SQLInstanceID]struct{}{
				1: {},
			},
			partitions: map[base.SQLInstanceID][][2]string{
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
				sqlInstanceID := base.SQLInstanceID(i)
				desc := &roachpb.NodeDescriptor{
					NodeID:  roachpb.NodeID(sqlInstanceID),
					Address: util.UnresolvedAddr{AddressField: fmt.Sprintf("addr%d", i)},
				}
				if err := mockGossip.SetNodeDescriptor(desc); err != nil {
					t.Fatal(err)
				}
				if _, ok := tc.nodesNotAdvertisingDistSQLVersion[sqlInstanceID]; !ok {
					verInfo := tc.nodeVersions[sqlInstanceID]
					if err := mockGossip.AddInfoProto(
						gossip.MakeDistSQLNodeVersionKey(sqlInstanceID),
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

			gw := gossip.MakeOptionalGossip(mockGossip)
			dsp := DistSQLPlanner{
				planVersion:          tc.planVersion,
				st:                   cluster.MakeTestingClusterSettings(),
				gatewaySQLInstanceID: base.SQLInstanceID(tsp.nodes[gatewayNode-1].NodeID),
				stopper:              stopper,
				spanResolver:         tsp,
				gossip:               gw,
				nodeHealth: distSQLNodeHealth{
					gossip: gw,
					connHealth: func(roachpb.NodeID, rpc.ConnectionClass) error {
						// All the nodes are healthy.
						return nil
					},
					isAvailable: func(base.SQLInstanceID) bool {
						return true
					},
				},
				codec: keys.SystemSQLCodec,
			}

			planCtx := dsp.NewPlanningCtx(context.Background(), &extendedEvalContext{
				EvalContext: tree.EvalContext{Codec: keys.SystemSQLCodec},
			}, nil, nil, DistributionTypeSystemTenantOnly)
			partitions, err := dsp.PartitionSpans(planCtx, roachpb.Spans{span})
			if err != nil {
				t.Fatal(err)
			}

			resMap := make(map[base.SQLInstanceID][][2]string)
			for _, p := range partitions {
				if _, ok := resMap[p.SQLInstanceID]; ok {
					t.Fatalf("node %d shows up in multiple partitions", p)
				}
				var spans [][2]string
				for _, s := range p.Spans {
					spans = append(spans, [2]string{string(s.Key), string(s.EndKey)})
				}
				resMap[p.SQLInstanceID] = spans
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
	defer log.Scope(t).Close(t)

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
		sqlInstanceID := base.SQLInstanceID(i)
		desc := &roachpb.NodeDescriptor{
			NodeID:  roachpb.NodeID(sqlInstanceID),
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
			gossip.MakeDistSQLNodeVersionKey(sqlInstanceID),
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

	gw := gossip.MakeOptionalGossip(mockGossip)
	dsp := DistSQLPlanner{
		planVersion:          execinfra.Version,
		st:                   cluster.MakeTestingClusterSettings(),
		gatewaySQLInstanceID: base.SQLInstanceID(tsp.nodes[gatewayNode-1].NodeID),
		stopper:              stopper,
		spanResolver:         tsp,
		gossip:               gw,
		nodeHealth: distSQLNodeHealth{
			gossip: gw,
			connHealth: func(node roachpb.NodeID, _ rpc.ConnectionClass) error {
				_, err := mockGossip.GetNodeIDAddress(node)
				return err
			},
			isAvailable: func(base.SQLInstanceID) bool {
				return true
			},
		},
		codec: keys.SystemSQLCodec,
	}

	planCtx := dsp.NewPlanningCtx(context.Background(), &extendedEvalContext{
		EvalContext: tree.EvalContext{Codec: keys.SystemSQLCodec},
	}, nil, nil, DistributionTypeSystemTenantOnly)
	partitions, err := dsp.PartitionSpans(planCtx, roachpb.Spans{span})
	if err != nil {
		t.Fatal(err)
	}

	resMap := make(map[base.SQLInstanceID][][2]string)
	for _, p := range partitions {
		if _, ok := resMap[p.SQLInstanceID]; ok {
			t.Fatalf("node %d shows up in multiple partitions", p)
		}
		var spans [][2]string
		for _, s := range p.Spans {
			spans = append(spans, [2]string{string(s.Key), string(s.EndKey)})
		}
		resMap[p.SQLInstanceID] = spans
	}

	expectedPartitions :=
		map[base.SQLInstanceID][][2]string{
			2: {{"A", "Z"}},
		}
	if !reflect.DeepEqual(resMap, expectedPartitions) {
		t.Errorf("expected partitions:\n  %v\ngot:\n  %v", expectedPartitions, resMap)
	}
}

func TestCheckNodeHealth(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	const sqlInstanceID = base.SQLInstanceID(5)

	mockGossip := gossip.NewTest(roachpb.NodeID(sqlInstanceID), nil /* rpcContext */, nil, /* grpcServer */
		stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())

	desc := &roachpb.NodeDescriptor{
		NodeID:  roachpb.NodeID(sqlInstanceID),
		Address: util.UnresolvedAddr{NetworkField: "tcp", AddressField: "testaddr"},
	}
	if err := mockGossip.SetNodeDescriptor(desc); err != nil {
		t.Fatal(err)
	}
	if err := mockGossip.AddInfoProto(
		gossip.MakeDistSQLNodeVersionKey(sqlInstanceID),
		&execinfrapb.DistSQLVersionGossipInfo{
			MinAcceptedVersion: execinfra.MinAcceptedVersion,
			Version:            execinfra.Version,
		},
		0, // ttl - no expiration
	); err != nil {
		t.Fatal(err)
	}

	notAvailable := func(base.SQLInstanceID) bool {
		return false
	}
	available := func(base.SQLInstanceID) bool {
		return true
	}

	connHealthy := func(roachpb.NodeID, rpc.ConnectionClass) error {
		return nil
	}
	connUnhealthy := func(roachpb.NodeID, rpc.ConnectionClass) error {
		return errors.New("injected conn health error")
	}
	_ = connUnhealthy

	livenessTests := []struct {
		isAvailable func(id base.SQLInstanceID) bool
		exp         string
	}{
		{available, ""},
		{notAvailable, "not using n5 since it is not available"},
	}

	gw := gossip.MakeOptionalGossip(mockGossip)
	for _, test := range livenessTests {
		t.Run("liveness", func(t *testing.T) {
			h := distSQLNodeHealth{
				gossip:      gw,
				connHealth:  connHealthy,
				isAvailable: test.isAvailable,
			}
			if err := h.check(context.Background(), sqlInstanceID); !testutils.IsError(err, test.exp) {
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
				gossip:      gw,
				connHealth:  test.connHealth,
				isAvailable: available,
			}
			if err := h.check(context.Background(), sqlInstanceID); !testutils.IsError(err, test.exp) {
				t.Fatalf("expected %v, got %v", test.exp, err)
			}
		})
	}
}

func TestCheckScanParallelizationIfLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	makeTableDesc := func() catalog.TableDescriptor {
		tableDesc := descpb.TableDescriptor{
			PrimaryIndex: descpb.IndexDescriptor{},
		}
		b := tabledesc.NewBuilder(&tableDesc)
		b.RunPostDeserializationChanges()
		return b.BuildImmutableTable()
	}

	scanToParallelize := &scanNode{parallelize: true}
	for _, tc := range []struct {
		plan                     planComponents
		prohibitParallelization  bool
		hasScanNodeToParallelize bool
	}{
		{
			plan: planComponents{main: planMaybePhysical{planNode: &scanNode{}}},
			// scanNode.parallelize is not set.
			hasScanNodeToParallelize: false,
		},
		{
			plan: planComponents{main: planMaybePhysical{planNode: &scanNode{parallelize: true, reqOrdering: ReqOrdering{{}}}}},
			// scanNode.reqOrdering is not empty.
			hasScanNodeToParallelize: false,
		},
		{
			plan:                     planComponents{main: planMaybePhysical{planNode: scanToParallelize}},
			hasScanNodeToParallelize: true,
		},
		{
			plan:                     planComponents{main: planMaybePhysical{planNode: &distinctNode{plan: scanToParallelize}}},
			hasScanNodeToParallelize: true,
		},
		{
			plan: planComponents{main: planMaybePhysical{planNode: &filterNode{source: planDataSource{plan: scanToParallelize}}}},
			// filterNode might be handled via wrapping a row-execution
			// processor, so we safely prohibit the parallelization.
			prohibitParallelization: true,
		},
		{
			plan: planComponents{main: planMaybePhysical{planNode: &groupNode{
				plan:  scanToParallelize,
				funcs: []*aggregateFuncHolder{{filterRenderIdx: tree.NoColumnIdx}}},
			}},
			// Non-filtering aggregation is supported.
			hasScanNodeToParallelize: true,
		},
		{
			plan: planComponents{main: planMaybePhysical{planNode: &groupNode{
				plan:  scanToParallelize,
				funcs: []*aggregateFuncHolder{{filterRenderIdx: 0}}},
			}},
			// Filtering aggregation is not natively supported.
			prohibitParallelization: true,
		},
		{
			plan: planComponents{main: planMaybePhysical{planNode: &indexJoinNode{
				input: scanToParallelize,
				table: &scanNode{desc: makeTableDesc()},
			}}},
			hasScanNodeToParallelize: true,
		},
		{
			plan:                     planComponents{main: planMaybePhysical{planNode: &limitNode{plan: scanToParallelize}}},
			hasScanNodeToParallelize: true,
		},
		{
			plan:                     planComponents{main: planMaybePhysical{planNode: &ordinalityNode{source: scanToParallelize}}},
			hasScanNodeToParallelize: true,
		},
		{
			plan: planComponents{main: planMaybePhysical{planNode: &renderNode{
				source: planDataSource{plan: scanToParallelize},
				render: []tree.TypedExpr{&tree.IndexedVar{Idx: 0}},
			}}},
			hasScanNodeToParallelize: true,
		},
		{
			plan: planComponents{main: planMaybePhysical{planNode: &renderNode{
				source: planDataSource{plan: scanToParallelize},
				render: []tree.TypedExpr{&tree.IsNullExpr{}},
			}}},
			// Not a simple projection (some expressions might be handled by
			// wrapping a row-execution processor, so we choose to be safe and
			// prohibit the parallelization for all non-IndexedVar expressions).
			prohibitParallelization: true,
		},
		{
			plan:                     planComponents{main: planMaybePhysical{planNode: &sortNode{plan: scanToParallelize}}},
			hasScanNodeToParallelize: true,
		},
		{
			plan:                     planComponents{main: planMaybePhysical{planNode: &unionNode{left: scanToParallelize, right: &scanNode{}}}},
			hasScanNodeToParallelize: true,
		},
		{
			plan:                     planComponents{main: planMaybePhysical{planNode: &unionNode{right: scanToParallelize, left: &scanNode{}}}},
			hasScanNodeToParallelize: true,
		},
		{
			plan:                     planComponents{main: planMaybePhysical{planNode: &valuesNode{}}},
			hasScanNodeToParallelize: false,
		},
		{
			plan: planComponents{main: planMaybePhysical{planNode: &windowNode{plan: scanToParallelize}}},
			// windowNode is not fully supported by the vectorized.
			prohibitParallelization: true,
		},

		// Unsupported edge cases.
		{
			plan:                    planComponents{main: planMaybePhysical{physPlan: &physicalPlanTop{}}},
			prohibitParallelization: true,
		},
		{
			plan:                    planComponents{cascades: []cascadeMetadata{{}}},
			prohibitParallelization: true,
		},
		{
			plan:                    planComponents{checkPlans: []checkPlan{{}}},
			prohibitParallelization: true,
		},
	} {
		prohibitParallelization, hasScanNodeToParallize := checkScanParallelizationIfLocal(context.Background(), &tc.plan)
		require.Equal(t, tc.prohibitParallelization, prohibitParallelization)
		require.Equal(t, tc.hasScanNodeToParallelize, hasScanNodeToParallize)
	}
}
