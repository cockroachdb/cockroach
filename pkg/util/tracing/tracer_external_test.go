// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracing_test

import (
	"context"
	"runtime"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl" // For tenant functionality.
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/grpcinterceptor"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

// TestSpanPooling checks that Spans are reused from the Tracer's sync.Pool,
// instead of being allocated every time.
func TestSpanPooling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t, "sync.Pool seems to be emptied very frequently under race, making the test unreliable")
	skip.UnderDeadlock(t, "span reuse triggers false-positives in the deadlock detector")
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tr := tracing.NewTracerWithOpt(ctx,
		// Ask the tracer to always create spans.
		tracing.WithTracingMode(tracing.TracingModeActiveSpansRegistry),
		// Ask the tracer to always reuse spans, overriding the testing's
		// metamorphic default and mimicking production.
		tracing.WithSpanReusePercent(100),
		tracing.WithTestingKnobs(tracing.TracerTestingKnobs{
			// Maintain the stats the test will consume.
			MaintainAllocationCounters: true,
		}))
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "test",
		Tracer:      tr,
	})
	defer s.Stopper().Stop(ctx)
	sqlRunner := sqlutils.MakeSQLRunner(db)
	sqlRunner.Exec(t, `create database test; CREATE TABLE test.t(k INT)`)

	// Prime the Tracer's span pool by doing some random work on all CPUs.
	g, _ := errgroup.WithContext(ctx)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		g.Go(func() error {
			for i := 0; i < 10; i++ {
				_, err := db.Exec("select * from t")
				if err != nil {
					return err
				}
				_, err = db.Exec("insert into t values (1)")
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())

	// Run a bunch of queries and count how many spans have been dynamically
	// allocated after each one. We expect the median number to be zero, as we
	// expect all the spans to come from the Tracer's pool.
	//
	// This test is pretty loose, just testing the median. This is to avoid
	// trouble from GC clearing the spans pool, or bursts of the background work
	// temporarily consuming the pool.
	const numQueries = 100
	spansAllocatedPerQuery := make([]int, numQueries)
	tr.TestingGetStatsAndReset() // Reset the stats.
	for i := range spansAllocatedPerQuery {
		sqlRunner.Exec(t, "select * from t")
		sqlRunner.Exec(t, "insert into t values (1)")
		_ /* created */, spansAllocatedPerQuery[i] = tr.TestingGetStatsAndReset()
	}
	sort.Ints(spansAllocatedPerQuery)
	require.Zero(t, spansAllocatedPerQuery[len(spansAllocatedPerQuery)/2], "spans allocated per query: %v", spansAllocatedPerQuery)
}

// Test that a shared-process tenant talking to a local KV server gets KV
// server-side traces.
func TestTraceForTenantWithLocalKVServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	server.RedactServerTracesForSecondaryTenants.Override(ctx, &s.SystemLayer().ClusterSettings().SV, false)

	// Create our own test tenant with a known name.
	const tenantName = "test-tenant"

	testStmt := "SELECT 1 FROM t WHERE id=1"
	var testStmtTrace tracingpb.Recording

	_, tenantDB, err := s.TenantController().StartSharedProcessTenant(ctx,
		base.TestSharedProcessTenantArgs{
			TenantName:  tenantName,
			UseDatabase: "test",
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					WithStatementTrace: func(trace tracingpb.Recording, stmt string) {
						if stmt == testStmt {
							testStmtTrace = trace
						}
					},
				},
			},
		})
	require.NoError(t, err)

	_, err = tenantDB.Exec(`CREATE DATABASE test; CREATE TABLE t(id INT PRIMARY KEY)`)
	require.NoError(t, err)

	// Execute a dummy statement that leases the `t` table descriptor, simplifying
	// the trace for the next query.
	_, err = tenantDB.Exec("SELECT 1 FROM t")
	require.NoError(t, err)

	_, err = tenantDB.Exec(testStmt)
	require.NoError(t, err)
	require.NotNil(t, testStmtTrace)

	// Check that the trace contains a server-side Batch span.
	var found bool
	for _, sp := range testStmtTrace {
		if sp.Operation != grpcinterceptor.BatchMethodName {
			continue
		}
		tag, ok := sp.FindTagGroup(tracingpb.AnonymousTagGroupName).FindTag(tracing.SpanKindTagKey)
		if ok && tag == oteltrace.SpanKindServer.String() {
			found = true
			break
		}
	}
	require.True(t, found)

	// Check that the RPC used the internalClientAdapter.
	_, ok := testStmtTrace.FindLogMessage(kvbase.RoutingRequestLocallyMsg)
	require.True(t, ok)
}
