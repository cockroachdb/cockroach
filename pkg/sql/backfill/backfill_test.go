// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfill_test

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func TestVectorColumnAndIndexBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Track whether we've injected an error
	var errorState struct {
		mu         syncutil.Mutex
		hasErrored bool
	}

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				// Inject a retriable error on the first call to the vector index backfiller.
				RunDuringReencodeVectorIndexEntry: func(txn *kv.Txn) error {
					errorState.mu.Lock()
					defer errorState.mu.Unlock()
					if !errorState.hasErrored {
						errorState.hasErrored = true
						return txn.GenerateForcedRetryableErr(ctx, "forcing a retry error")
					}
					return nil
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Enable vector indexes.
	sqlDB.Exec(t, `SET CLUSTER SETTING feature.vector_index.enabled = true`)

	// Create a table with a vector column
	sqlDB.Exec(t, `
		CREATE TABLE vectors (
			id INT PRIMARY KEY,
			vec VECTOR(3),
			data INT
		)
	`)

	// Insert 200 rows with random vector data
	sqlDB.Exec(t, `
		INSERT INTO vectors (id, vec, data)
		SELECT 
			generate_series(1, 200) as id,
			ARRAY[random(), random(), random()]::vector(3) as vec,
			generate_series(1, 200) as data
	`)

	// Create a vector index on the vector column
	sqlDB.Exec(t, `
		CREATE VECTOR INDEX vec_idx ON vectors (vec)
	`)

	// Test vector similarity search and see that the backfiller got at
	// least some of the vectors in there.
	var matchCount int
	sqlDB.QueryRow(t, `
		SELECT count(*)
		FROM (
			SELECT id 
			FROM vectors@vec_idx
			ORDER BY vec <-> ARRAY[0.5, 0.5, 0.5]::vector(3)
			LIMIT 200 
		)
	`).Scan(&matchCount)
	// There's some non-determinism here where we may not find all 200 vectors.
	// I chose 190 as a low water mark to prevent test flakes, but it should really
	// be 200 in most cases.
	require.Greater(t, matchCount, 190, "Expected to find at least 190 similar vectors")
}

func TestConcurrentOperationsDuringVectorIndexCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Channel to block the backfill process
	blockBackfill := make(chan struct{})
	backfillBlocked := make(chan struct{})

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				// Block the backfill process after the first batch
				RunAfterBackfillChunk: func() {
					backfillBlocked <- struct{}{}
					<-blockBackfill
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Enable vector indexes.
	sqlDB.Exec(t, `SET CLUSTER SETTING feature.vector_index.enabled = true`)

	// Create a table with a vector column
	sqlDB.Exec(t, `
		CREATE TABLE vectors (
			id INT PRIMARY KEY,
			vec VECTOR(3),
			data INT
		)
	`)

	// Insert some initial data
	sqlDB.Exec(t, `
		INSERT INTO vectors (id, vec, data) VALUES
		(1, '[1, 2, 3]', 100),
		(2, '[4, 5, 6]', 200),
		(3, '[7, 8, 9]', 300)
	`)

	wg := sync.WaitGroup{}
	wg.Add(1)

	// Start creating the vector index in a goroutine
	var createIndexErr error
	go func() {
		defer wg.Done()
		_, createIndexErr = db.ExecContext(ctx, `CREATE VECTOR INDEX vec_idx ON vectors (vec)`)
	}()

	// Wait for the backfill to be blocked
	<-backfillBlocked

	// Attempt concurrent operations while the index is being created
	// These should fail with the appropriate error
	_, err := db.ExecContext(ctx, `UPDATE vectors SET vec = '[10, 11, 12]', data = 150 WHERE id = 1`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot write to a vector index while it is being built")

	_, err = db.ExecContext(ctx, `INSERT INTO vectors (id, vec, data) VALUES (4, '[13, 14, 15]', 400)`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot write to a vector index while it is being built")

	_, err = db.ExecContext(ctx, `DELETE FROM vectors WHERE id = 2`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot write to a vector index while it is being built")

	// This update should succeed since it only modifies the data column
	_, err = db.ExecContext(ctx, `UPDATE vectors SET data = 250 WHERE id = 2`)
	require.NoError(t, err, "Updating only the data column should succeed during index creation")

	// Unblock the backfill process
	close(blockBackfill)

	wg.Wait()
	// Wait for the index creation to complete
	require.NoError(t, createIndexErr)

	// Verify the index was created successfully
	var id int
	sqlDB.QueryRow(t, `SELECT id FROM vectors@vec_idx ORDER BY vec <-> '[1, 2, 3]' LIMIT 1`).Scan(&id)
	require.Equal(t, 1, id)
}

// Regression for issue #145261: vector index backfill with a prefix column
// crashes the node.
func TestVectorIndexWithPrefixBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Enable vector indexes.
	sqlDB.Exec(t, `SET CLUSTER SETTING feature.vector_index.enabled = true`)

	// Create a table with a vector column + a prefix column.
	sqlDB.Exec(t, `
		CREATE TABLE items (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			customer_id INT NOT NULL,
			name TEXT,
			embedding VECTOR(3)
		);
	`)

	// Generate 10 customer id's, each with 100 vectors.
	sqlDB.Exec(t, `
		INSERT INTO items (customer_id, name, embedding)
		SELECT
			(i % 10) + 1 AS customer_id,
			'Item ' || i,
			ARRAY[random(), random(), random()]::vector
		FROM generate_series(1, 1000) AS s(i);
	`)

	// Create the vector index with a small partition size so that the trees
	// have more levels and splits to get there.
	sqlDB.Exec(t, `
		CREATE VECTOR INDEX ON items (customer_id, embedding)
		WITH (min_partition_size=2, max_partition_size=8, build_beam_size=2);
	`)

	// Ensure that each customer has 100 vectors.
	for i := range 10 {
		func() {
			rows := sqlDB.Query(t, `
				SELECT id FROM items
				WHERE customer_id = $1
				ORDER BY embedding <-> $2
				LIMIT 200`, i+1, "[0, 0, 0]")
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			require.Equal(t, 100, count)
		}()
	}
}
