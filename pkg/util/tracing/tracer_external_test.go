// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing_test

import (
	"context"
	"runtime"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestSpanPooling checks that Spans are reused from the Tracer's sync.Pool,
// instead of being allocated every time.
func TestSpanPooling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t, "sync.Pool seems to be emptied very frequently under race, making the test unreliable")
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
