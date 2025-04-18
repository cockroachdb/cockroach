// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex_test

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

// TestVecIndexConcurrency builds an index on multiple goroutines, with
// background splits and merges enabled.
func TestVecIndexConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	runner := sqlutils.MakeSQLRunner(sqlDB)
	defer srv.Stopper().Stop(ctx)
	mgr := srv.ExecutorConfig().(sql.ExecutorConfig).VecIndexManager

	// Enable vector indexes.
	runner.Exec(t, `SET CLUSTER SETTING feature.vector_index.enabled = true`)

	// Construct the table.
	runner.Exec(t, "CREATE TABLE t (id INT PRIMARY KEY, v VECTOR(512), VECTOR INDEX (v))")

	// Load features.
	vectors := testutils.LoadFeatures(t, 1000)

	for i := 0; i < 1; i++ {
		buildIndex(ctx, t, runner, mgr, vectors)
	}
}

// TestVecIndexStandbyReader builds an index on a source tenant and verifies
// that a PCR standby reader can read the index, but doesn't attempt to initiate
// fixups.
func TestVecIndexStandbyReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "slow test")

	rnd, seed := randutil.NewTestRand()
	t.Logf("random seed: %v", seed)

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 3, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
			},
		})
	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)
	mgr := ts.ExecutorConfig().(sql.ExecutorConfig).VecIndexManager

	_, srcDB, err := ts.TenantController().StartSharedProcessTenant(ctx,
		base.TestSharedProcessTenantArgs{
			TenantID:    serverutils.TestTenantID(),
			TenantName:  "src",
			UseDatabase: "defaultdb",
		},
	)
	require.NoError(t, err)
	dstTenant, dstDB, err := ts.TenantController().StartSharedProcessTenant(ctx,
		base.TestSharedProcessTenantArgs{
			TenantID:    serverutils.TestTenantID2(),
			TenantName:  "dst",
			UseDatabase: "defaultdb",
		},
	)
	require.NoError(t, err)

	srcRunner := sqlutils.MakeSQLRunner(srcDB)
	dstRunner := sqlutils.MakeSQLRunner(dstDB)

	// Enable vector indexes.
	srcRunner.Exec(t, `SET CLUSTER SETTING feature.vector_index.enabled = true`)

	// Construct the table.
	srcRunner.Exec(t, "CREATE TABLE t (id INT PRIMARY KEY, v VECTOR(512), VECTOR INDEX foo (v))")

	// Load features and build the index.
	vectors := testutils.LoadFeatures(t, 1000)
	buildIndex(ctx, t, srcRunner, mgr, vectors)

	// Wait for the standby reader to catch up.
	asOf := serverutils.WaitForStandbyTenantReplication(t, ctx, ts, dstTenant)

	const queryTemplate = `SELECT * FROM t@foo %s ORDER BY v <-> '%s' LIMIT 3`
	asOfClause := fmt.Sprintf("AS OF SYSTEM TIME %s", asOf.AsOfSystemTime())
	for range 10 {
		// Select a random vector from the set and run an ANN query against both
		// tenants. The query results should be identical.
		vec := vectors.At(rnd.Intn(vectors.Count)).String()
		expected := srcRunner.QueryStr(t, fmt.Sprintf(queryTemplate, asOfClause, vec))
		dstRunner.CheckQueryResults(t, fmt.Sprintf(queryTemplate, "", vec), expected)
	}
}

func buildIndex(
	ctx context.Context,
	t *testing.T,
	runner *sqlutils.SQLRunner,
	mgr *vecindex.Manager,
	vectors vector.Set,
) {
	var insertCount atomic.Uint64

	// Insert block of vectors within the scope of a transaction.
	insertBlock := func(start, end int) {
		var valuesClause strings.Builder
		args := make([]interface{}, (end-start)*2)
		for i := start; i < end; i++ {
			argOffset := i - start
			if argOffset > 0 {
				valuesClause.WriteString(", ")
			}
			valuesClause.WriteString(fmt.Sprintf("($%d, $%d)", argOffset*2+1, argOffset*2+2))
			args[argOffset*2] = i
			args[argOffset*2+1] = vectors.At(i).String()
		}

		// Execute the batch insert.
		query := fmt.Sprintf("INSERT INTO t (id, v) VALUES %s", valuesClause.String())
		runner.Exec(t, query, args...)

		insertCount.Add(uint64(end - start))
	}

	// Insert vectors into the store on multiple goroutines.
	var wait sync.WaitGroup
	procs := runtime.GOMAXPROCS(-1)
	countPerProc := (vectors.Count + procs) / procs
	const blockSize = 1
	for i := 0; i < vectors.Count; i += countPerProc {
		end := min(i+countPerProc, vectors.Count)
		wait.Add(1)
		go func(start, end int) {
			defer wait.Done()

			// Break vector group into individual transactions that each insert a
			// block of vectors.
			for j := start; j < end; j += blockSize {
				insertBlock(j, min(j+blockSize, end))
			}
		}(i, end)
	}

	metrics := mgr.Metrics().(*vecindex.Metrics)
	for int(insertCount.Load()) < vectors.Count {
		time.Sleep(time.Second)
		log.Infof(ctx, "%d vectors inserted", insertCount.Load())
		log.Infof(ctx, "%d successful splits", metrics.SuccessfulSplits.Count())
		log.Infof(ctx, "%d pending splits/merges", metrics.PendingSplitsMerges.Value())

		// Fail on foreground goroutine if any background goroutines failed.
		if t.Failed() {
			t.FailNow()
		}
	}

	wait.Wait()
}
