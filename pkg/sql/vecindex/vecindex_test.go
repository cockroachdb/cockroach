// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
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

	// Construct the table.
	runner.Exec(t, "CREATE TABLE t (id INT PRIMARY KEY, v VECTOR(512), VECTOR INDEX (v))")

	// Load features.
	vectors := testutils.LoadFeatures(t, 100)

	for i := 0; i < 1; i++ {
		buildIndex(ctx, t, runner, vectors)
	}
}

func buildIndex(ctx context.Context, t *testing.T, runner *sqlutils.SQLRunner, vectors vector.Set) {
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
	procs := 1
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

	for int(insertCount.Load()) < vectors.Count {
		time.Sleep(time.Second)
		log.Infof(ctx, "inserted %d vectors", insertCount.Load())
	}

	wait.Wait()

	// Fail on foreground goroutine if any background goroutines failed.
	if t.Failed() {
		t.FailNow()
	}
}
