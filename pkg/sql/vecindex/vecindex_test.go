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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecencoding"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
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

	// Load 512d image embedding dataset.
	dataset := testutils.LoadDataset(t, testutils.ImagesDataset)

	// Trim dataset count from 10K to 2K and dimensions from 512 to 64, in order
	// to make the test run faster and hit more interesting concurrency
	// combinations.
	const vectorCount = 2000
	const dims = 64
	vectors := vector.MakeSet(dims)
	for i := range vectorCount {
		vectors.Add(dataset.At(i)[:dims])
	}

	// Construct the table. Use small partition size so that the tree has more
	// levels and more splits to get there.
	const minPartitionSize = 2
	const maxPartitionSize = minPartitionSize * 4
	stmt := `
	CREATE TABLE t (
		id INT PRIMARY KEY,
		v VECTOR(%d),
		VECTOR INDEX (v) WITH (min_partition_size=%d, max_partition_size=%d, build_beam_size=4)
	)
	`
	runner.Exec(t, fmt.Sprintf(stmt, dims, minPartitionSize, maxPartitionSize))

	// Insert vectors into and remove vectors from the store on multiple
	// goroutines.
	var insertCount atomic.Uint64
	var wait sync.WaitGroup
	procs := runtime.GOMAXPROCS(-1)
	countPerProc := (vectors.Count + procs) / procs
	const blockSize = 3
	for i := 0; i < vectors.Count; i += countPerProc {
		end := min(i+countPerProc, vectors.Count)
		wait.Add(1)
		go func(start, end int) {
			defer wait.Done()

			// Break vector group into individual transactions that each insert a
			// block of vectors and then remove the first in the block.
			for j := start; j < end; j += blockSize {
				count := min(blockSize, end-j)
				insertVectors(t, runner, j, vectors.Slice(j, count))
				insertCount.Add(uint64(count))

				// Remove the first vector in the block.
				runner.Exec(t, "DELETE FROM t WHERE id = $1", j)
			}
		}(i, end)
	}

	info := log.Every(time.Second)
	metrics := mgr.Metrics().(*vecindex.Metrics)
	logProgress := func() {
		log.Infof(ctx, "%d vectors inserted", insertCount.Load())
		log.Infof(ctx, "%d successful splits", metrics.SuccessfulSplits.Count())
		log.Infof(ctx, "%d pending splits/merges", metrics.PendingSplitsMerges.Value())
	}

	vecOffset := 0
	for {
		time.Sleep(10 * time.Millisecond)

		if info.ShouldLog() {
			logProgress()
		}

		// Keep looping until we've inserted all vectors and until enough splits
		// have occurred.
		if int(insertCount.Load()) >= vectors.Count {
			if int(metrics.SuccessfulSplits.Count()) >= vectors.Count/maxPartitionSize {
				break
			}
		}

		// Query for a vector while inserts happen in the background.
		if insertCount.Load() > 0 {
			var id int
			vec := vectors.At(vecOffset % vectors.Count)
			row := runner.QueryRow(t, `SELECT id FROM t ORDER BY v <-> $1 LIMIT 1`, vec.String())
			row.Scan(&id)
			vecOffset++
		}

		// Fail on foreground goroutine if any background goroutines failed.
		if t.Failed() {
			t.FailNow()
		}
	}
	wait.Wait()

	logProgress()
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

	// Load dataset and build the index.
	const batchSize = 10
	const numBatches = 100
	vectors := testutils.LoadDataset(t, testutils.ImagesDataset)
	vectors = vectors.Slice(0, batchSize*numBatches)
	for i := 0; i < numBatches; i++ {
		insertVectors(t, srcRunner, i*batchSize, vectors.Slice(i*batchSize, batchSize))
	}

	// Wait for the standby reader to catch up.
	asOf := testcluster.WaitForStandbyTenantReplication(t, ctx, ts.Clock(), dstTenant)

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

// Insert block of vectors within the scope of a transaction.
func insertVectors(t *testing.T, runner *sqlutils.SQLRunner, startId int, vectors vector.Set) {
	var valuesClause strings.Builder
	args := make([]any, vectors.Count*2)
	for i := range vectors.Count {
		if i > 0 {
			valuesClause.WriteString(", ")
		}
		j := i * 2
		valuesClause.WriteString(fmt.Sprintf("($%d, $%d)", j+1, j+2))
		args[j] = startId + i
		args[j+1] = vectors.At(i).String()
	}

	// Execute the batch insert.
	query := fmt.Sprintf("INSERT INTO t (id, v) VALUES %s", valuesClause.String())
	runner.Exec(t, query, args...)
}

// TestVecIndexDeletion tests that rows can be properly deleted from a vector index.
func TestVecIndexDeletion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	runner := sqlutils.MakeSQLRunner(sqlDB)
	defer srv.Stopper().Stop(ctx)

	// Enable vector indexes.
	runner.Exec(t, `SET CLUSTER SETTING feature.vector_index.enabled = true`)

	// Construct the table.
	runner.Exec(t, "CREATE TABLE t (id INT PRIMARY KEY, v VECTOR(512), VECTOR INDEX (v))")

	// Load a small set of vectors for testing.
	vectors := testutils.LoadDataset(t, testutils.ImagesDataset)
	vectors = vectors.Slice(0, 10)

	// Insert the vectors.
	for i := 0; i < vectors.Count; i++ {
		runner.Exec(t, "INSERT INTO t (id, v) VALUES ($1, $2)", i, vectors.At(i).String())
	}

	// Verify the vectors were inserted.
	var count int
	runner.QueryRow(t, "SELECT count(*) FROM t").Scan(&count)
	if count != vectors.Count {
		t.Errorf("expected %d rows, got %d", vectors.Count, count)
	}

	// Get the table descriptor to find the vector index ID.
	var tableID uint32
	runner.QueryRow(t, "SELECT id FROM system.namespace WHERE name = 't'").Scan(&tableID)

	// Get the index ID from crdb_internal.table_indexes
	var indexID uint32
	runner.QueryRow(t, "SELECT index_id FROM crdb_internal.table_indexes WHERE descriptor_id = $1 AND index_name = 't_v_idx'", tableID).Scan(&indexID)
	if indexID == 0 {
		t.Fatal("vector index not found")
	}

	// Start a KV transaction to manually delete the vector index keys.
	db := srv.DB()
	txn := db.NewTxn(ctx, "delete-vector-index-keys")

	// Delete all vector index keys for the table.
	codec := srv.ApplicationLayer().Codec()
	prefix := rowenc.MakeIndexKeyPrefix(codec, descpb.ID(tableID), descpb.IndexID(indexID))
	prefix = vecencoding.EncodePartitionKey(prefix, cspann.RootKey)
	prefix = vecencoding.EncodePartitionLevel(prefix, cspann.LeafLevel)
	key := roachpb.Key(prefix)
	if _, err := txn.DelRange(ctx, key.Next(), key.PrefixEnd(), false); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Delete rows one at a time and verify the count after each deletion.
	for i := 0; i < vectors.Count; i++ {
		runner.Exec(t, "DELETE FROM t WHERE id = $1", i)

		// Verify the count after each deletion
		runner.QueryRow(t, "SELECT count(*) FROM t").Scan(&count)
		expectedCount := vectors.Count - (i + 1)
		if count != expectedCount {
			t.Errorf("after deleting id %d: expected %d rows, got %d", i, expectedCount, count)
		}
	}

	// Final verification that table is empty
	runner.QueryRow(t, "SELECT count(*) FROM t").Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 rows after all deletions, got %d", count)
	}
}
