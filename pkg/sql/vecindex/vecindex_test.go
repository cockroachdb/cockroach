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

	// Load features.
	const featureCount = 2000
	features := testutils.LoadFeatures(t, featureCount)

	// Trim feature dimensions from 512 to 64, in order to make the test run
	// faster and hit more interesting concurrency combinations.
	const dims = 64
	vectors := vector.MakeSet(dims)
	for i := range features.Count {
		vectors.Add(features.At(i)[:dims])
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
				// TODO(matt.white): Re-enable once deletion does not assert when
				// it cannot find a vector to delete.
				// runner.Exec(t, "DELETE FROM t WHERE id = $1", j)
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
			if int(metrics.SuccessfulSplits.Count()) >= featureCount/maxPartitionSize {
				break
			}
		}

		// Query for a vector while inserts happen in the background.
		if insertCount.Load() > 0 {
			var id int
			row := runner.QueryRow(t,
				`SELECT id FROM t ORDER BY v <-> $1 LIMIT 1`, vectors.At(vecOffset).String())
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

// Insert block of vectors within the scope of a transaction.
func insertVectors(t *testing.T, runner *sqlutils.SQLRunner, startId int, vectors vector.Set) {
	var valuesClause strings.Builder
	args := make([]any, vectors.Count*2)
	for i := range vectors.Count {
		if i > 0 {
			valuesClause.WriteString(", ")
		}
		valuesClause.WriteString(fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
		args[i*2] = startId + i
		args[i*2+1] = vectors.At(i).String()
	}

	// Execute the batch insert.
	query := fmt.Sprintf("INSERT INTO t (id, v) VALUES %s", valuesClause.String())
	runner.Exec(t, query, args...)
}
