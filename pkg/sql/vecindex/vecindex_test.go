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
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// TestVecindexConcurrency builds an index on multiple goroutines, with
// background splits and merges enabled.
func TestVecindexConcurrency(t *testing.T) {
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
	// TODO(andyk): replace with runtime.GOMAXPROCS(-1) once contention is solved.
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

// TestVecindexDeletion tests that rows can be properly deleted from a vector index.
func TestVecindexDeletion(t *testing.T) {
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
	vectors := testutils.LoadFeatures(t, 10)

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
