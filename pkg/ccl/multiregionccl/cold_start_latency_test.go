// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl

import (
	"context"
	gosql "database/sql"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils/regionlatency"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestColdStartLatency attempts to capture the cold start latency for
// sql pods given different cluster topologies.
func TestColdStartLatency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "too slow")
	skip.UnderStress(t, "too slow")

	// We'll need to make some per-node args to assign the different
	// KV nodes to different regions and AZs. We'll want to do it to
	// look somewhat like the real cluster topologies we have in mind.
	//
	// Initially we'll want 9 nodes, 3 per region in 3 regions with
	// 2 per AZ. We can tune the various latencies between these regions.
	regionLatencies := regionlatency.RoundTripPairs{
		{A: "us-east1", B: "us-west1"}:     66 * time.Millisecond,
		{A: "us-east1", B: "europe-west1"}: 64 * time.Millisecond,
		{A: "us-west1", B: "europe-west1"}: 146 * time.Millisecond,
	}.ToLatencyMap()
	const (
		numNodes        = 9
		numAZsPerRegion = 3
	)
	localities := makeLocalities(regionLatencies, numNodes, numAZsPerRegion)
	regions := make([]string, len(localities))
	for i, l := range localities {
		regions[i], _ = l.Find("region")
	}
	pauseAfter := make(chan struct{})
	signalAfter := make([]chan struct{}, numNodes)
	var latencyEnabled syncutil.AtomicBool
	var addrsToNodeIDs sync.Map

	// Set up the host cluster.
	perServerArgs := make(map[int]base.TestServerArgs, numNodes)
	for i := 0; i < numNodes; i++ {
		i := i
		args := base.TestServerArgs{
			DefaultTestTenant: base.TODOTestTenantDisabled,
			Locality:          localities[i],
		}
		signalAfter[i] = make(chan struct{})
		serverKnobs := &server.TestingKnobs{
			PauseAfterGettingRPCAddress:  pauseAfter,
			SignalAfterGettingRPCAddress: signalAfter[i],
			ContextTestingKnobs: rpc.ContextTestingKnobs{
				InjectedLatencyOracle:  regionlatency.MakeAddrMap(),
				InjectedLatencyEnabled: latencyEnabled.Get,
				UnaryClientInterceptor: func(
					target string, class rpc.ConnectionClass,
				) grpc.UnaryClientInterceptor {
					return func(
						ctx context.Context, method string, req, reply interface{},
						cc *grpc.ClientConn, invoker grpc.UnaryInvoker,
						opts ...grpc.CallOption,
					) error {
						if !log.ExpensiveLogEnabled(ctx, 2) {
							return invoker(ctx, method, req, reply, cc, opts...)
						}
						nodeIDi, _ := addrsToNodeIDs.Load(target)
						nodeID, _ := nodeIDi.(int)
						start := timeutil.Now()
						defer func() {
							log.VEventf(ctx, 2, "%d->%d (%v->%v) %s %v %v took %v",
								i, nodeID, localities[i], localities[nodeID],
								method, req, reply, timeutil.Since(start),
							)
						}()
						return invoker(ctx, method, req, reply, cc, opts...)
					}
				},
			},
		}
		args.Knobs.Server = serverKnobs
		perServerArgs[i] = args
	}
	tc := testcluster.NewTestCluster(t, numNodes, base.TestClusterArgs{
		ParallelStart:     true,
		ServerArgsPerNode: perServerArgs,
	})
	go func() {
		for _, c := range signalAfter {
			<-c
		}
		assert.NoError(t, regionLatencies.Apply(tc))
		close(pauseAfter)
	}()
	tc.Start(t)
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	enableLatency := func() {
		latencyEnabled.Set(true)
		for i := 0; i < numNodes; i++ {
			tc.Server(i).RPCContext().RemoteClocks.TestingResetLatencyInfos()
		}
	}

	for i := 0; i < numNodes; i++ {
		addrsToNodeIDs.Store(tc.Server(i).RPCAddr(), i)
	}
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(1))

	// Shorten the closed timestamp target duration so that span configs
	// propagate more rapidly.
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '200ms'`)
	tdb.Exec(t, "SET CLUSTER SETTING kv.allocator.load_based_rebalancing = off")
	tdb.Exec(t, "SET CLUSTER SETTING kv.allocator.min_lease_transfer_interval = '10ms'")
	// Lengthen the lead time for the global tables to prevent overload from
	// resulting in delays in propagating closed timestamps and, ultimately
	// forcing requests from being redirected to the leaseholder. Without this
	// change, the test sometimes is flakey because the latency budget allocated
	// to closed timestamp propagation proves to be insufficient. This value is
	// very cautious, and makes this already slow test even slower.
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50 ms'")
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.lead_for_global_reads_override = '1500ms'`)
	tdb.Exec(t, `ALTER TENANT ALL SET CLUSTER SETTING spanconfig.reconciliation_job.checkpoint_interval = '500ms'`)

	configureSystem := func(t *testing.T, db *gosql.DB, isTenant bool) {
		var stmts []string
		if !isTenant {
			stmts = []string{
				"ALTER TENANT ALL SET CLUSTER SETTING sql.zone_configs.allow_for_secondary_tenant.enabled = true",
				"ALTER TENANT ALL SET CLUSTER SETTING sql.multi_region.allow_abstractions_for_secondary_tenants.enabled = true",
				`alter range meta configure zone using constraints = '{"+region=us-east1": 1, "+region=us-west1": 1, "+region=europe-west1": 1}';`,
			}
		} else {
			stmts = []string{`
BEGIN;
ALTER DATABASE system PRIMARY REGION "us-east1";
ALTER DATABASE system ADD REGION "us-west1";
ALTER DATABASE system ADD REGION "europe-west1";
COMMIT;`}
		}
		tdb := sqlutils.MakeSQLRunner(db)
		for i, stmt := range stmts {
			t.Log(i, stmt)
			tdb.Exec(t, stmt)
		}
	}
	configureSystem(t, tc.ServerConn(0), false)

	var blockCrossRegionTenantAccess atomic.Bool
	maybeWait := func(ctx context.Context, a, b int) {
		if regions[a] != regions[b] && blockCrossRegionTenantAccess.Load() {
			<-ctx.Done()
		}
	}
	tenantServerKnobs := func(i int) *server.TestingKnobs {
		return &server.TestingKnobs{
			ContextTestingKnobs: rpc.ContextTestingKnobs{
				InjectedLatencyOracle: tc.Server(i).TestingKnobs().
					Server.(*server.TestingKnobs).ContextTestingKnobs.
					InjectedLatencyOracle,
				InjectedLatencyEnabled: latencyEnabled.Get,
				StreamClientInterceptor: func(
					target string, class rpc.ConnectionClass,
				) grpc.StreamClientInterceptor {
					return func(
						ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
						method string, streamer grpc.Streamer, opts ...grpc.CallOption,
					) (grpc.ClientStream, error) {
						nodeIDi, _ := addrsToNodeIDs.Load(target)
						nodeID, _ := nodeIDi.(int)
						start := timeutil.Now()
						maybeWait(ctx, i, nodeID)
						defer func() {
							if !log.ExpensiveLogEnabled(ctx, 2) {
								return
							}
							log.VEventf(
								ctx, 2, "tenant%d->%d opening stream %v to %v (%v->%v)",
								i, nodeID, method, target, localities[i], localities[nodeID],
							)
						}()
						c, err := streamer(ctx, desc, cc, method, opts...)
						if err != nil {
							return nil, err
						}
						return wrappedStream{
							start:        start,
							ClientStream: c,
						}, nil
					}
				},
				UnaryClientInterceptor: func(target string, class rpc.ConnectionClass) grpc.UnaryClientInterceptor {
					nodeIDi, _ := addrsToNodeIDs.Load(target)
					nodeID, _ := nodeIDi.(int)
					return func(
						ctx context.Context, method string, req, reply interface{},
						cc *grpc.ClientConn, invoker grpc.UnaryInvoker,
						opts ...grpc.CallOption,
					) error {
						maybeWait(ctx, i, nodeID)
						start := timeutil.Now()
						defer func() {
							log.VEventf(
								ctx, 2, "tenant%d->%d %v->%v %s %v %v took %v",
								i, nodeID, localities[i], localities[nodeID],
								method, req, reply, timeutil.Since(start),
							)
						}()
						return invoker(ctx, method, req, reply, cc, opts...)
					}
				},
			},
		}
	}
	const password = "asdf"
	{
		tenant, tenantDB := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
			TenantID: serverutils.TestTenantID(),
			TestingKnobs: base.TestingKnobs{
				Server: tenantServerKnobs(0),
			},
			Locality: localities[0],
		})
		configureSystem(t, tenantDB, true)
		tdb := sqlutils.MakeSQLRunner(tenantDB)
		tdb.Exec(t, "CREATE USER foo PASSWORD $1 LOGIN", password)
		tdb.Exec(t, "GRANT admin TO foo")

		// Wait for the span configs to propagate. After we know they have
		// propagated, we'll shut down the tenant and wait for them to get
		// applied.
		tdb.Exec(t, "CREATE TABLE after AS SELECT now() AS after")
		tdb.CheckQueryResultsRetry(t, `
  WITH progress AS (
                    SELECT crdb_internal.pb_to_json(
                            'progress',
                            progress
                           )->'AutoSpanConfigReconciliation' AS p
                      FROM crdb_internal.system_jobs
                     WHERE status = 'running'
                ),
       checkpoint AS (
                    SELECT (p->'checkpoint'->>'wallTime')::FLOAT8 / 1e9 AS checkpoint
                      FROM progress
                     WHERE p IS NOT NULL
                  )
SELECT checkpoint > extract(epoch from after)
  FROM checkpoint, after`,
			[][]string{{"true"}})
		tenant.Stopper().Stop(ctx)
	}

	// Wait for the configs to be applied.
	testutils.SucceedsWithin(t, func() error {
		for _, server := range tc.Servers {
			reporter := server.Server.SpanConfigReporter()
			report, err := reporter.SpanConfigConformance(ctx, []roachpb.Span{
				{Key: keys.TableDataMin, EndKey: keys.TenantTableDataMax},
			})
			if err != nil {
				return err
			}
			if !report.IsEmpty() {
				return errors.Errorf("expected empty report, got: {over: %d, under: %d, violating: %d, unavailable: %d}",
					len(report.OverReplicated),
					len(report.UnderReplicated),
					len(report.ViolatingConstraints),
					len(report.Unavailable))
			}
		}
		return nil
	}, 5*time.Minute)

	require.NoError(t, tc.WaitForFullReplication())

	doTest := func(wg *sync.WaitGroup, qp *quotapool.IntPool, i int, duration *time.Duration) {
		defer wg.Done()
		r, _ := qp.Acquire(ctx, 1)
		defer r.Release()
		start := timeutil.Now()
		sn := tenantServerKnobs(i)
		tenant, err := tc.Server(i).StartTenant(ctx, base.TestTenantArgs{
			TenantID:            serverutils.TestTenantID(),
			DisableCreateTenant: true,
			SkipTenantCheck:     true,
			TestingKnobs: base.TestingKnobs{
				Server: sn,
			},
			Locality: localities[i],
		})
		require.NoError(t, err)
		defer tenant.Stopper().Stop(ctx)
		pgURL, cleanup, err := sqlutils.PGUrlWithOptionalClientCertsE(
			tenant.SQLAddr(), "tenantdata", url.UserPassword("foo", password),
			false, // withClientCerts
		)
		if !assert.NoError(t, err) {
			return
		}
		defer cleanup()
		pgURL.Path = "defaultdb"
		conn, err := pgx.Connect(ctx, pgURL.String())
		if !assert.NoError(t, err) {
			return
		}
		var one int
		assert.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&one))
		*duration = timeutil.Since(start)
		t.Log("done", i, localities[i], *duration)
	}
	// This controls how many servers to start up at the same time. The
	// observation is that starting more concurrently does have a major
	// latency impact.
	const concurrency = 1
	runAllTests := func() []time.Duration {
		qp := quotapool.NewIntPool("startup-concurrency", concurrency)
		latencyResults := make([]time.Duration, len(localities))
		var wg sync.WaitGroup
		for i := range localities {
			wg.Add(1)
			go doTest(&wg, qp, i, &latencyResults[i])
		}
		wg.Wait()
		return latencyResults
	}

	enableLatency()
	t.Log("pre running test to allocate instance IDs")
	t.Log("result", localities, runAllTests())
	t.Log("running test with no connectivity from sql pods to remote regions")
	blockCrossRegionTenantAccess.Store(true)
	t.Log("result", localities, runAllTests())
	blockCrossRegionTenantAccess.Store(false)
}

func makeLocalities(
	lm regionlatency.LatencyMap, numNodes, azsPerRegion int,
) (ret []roachpb.Locality) {
	regions := lm.GetRegions()
	for regionIdx, nodesInRegion := range distribute(numNodes, len(regions)) {
		for azIdx, nodesInAZ := range distribute(nodesInRegion, azsPerRegion) {
			for i := 0; i < nodesInAZ; i++ {
				ret = append(ret, roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "region", Value: regions[regionIdx]},
						{Key: "az", Value: string(rune('a' + azIdx))},
					},
				})
			}
		}
	}
	return ret
}

func distribute(total, num int) []int {
	res := make([]int, num)
	for i := range res {
		// Use the average number of remaining connections.
		div := len(res) - i
		res[i] = (total + div/2) / div
		total -= res[i]
	}
	return res
}

type wrappedStream struct {
	start time.Time
	grpc.ClientStream
}

func (w wrappedStream) RecvMsg(m interface{}) error {
	if err := w.ClientStream.RecvMsg(m); err != nil {
		return err
	}
	log.VEventf(w.ClientStream.Context(), 2, "stream received %T %v %v", m, timeutil.Since(w.start), m)
	return nil
}
