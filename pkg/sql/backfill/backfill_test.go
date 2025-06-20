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
		if createIndexErr != nil {
			backfillBlocked <- struct{}{}
		}
	}()

	// Wait for the backfill to be blocked
	<-backfillBlocked

	var err error
	// Attempt concurrent operations while the index is being created
	// These should fail with the appropriate error
	_, err = db.ExecContext(ctx, `UPDATE vectors SET vec = '[10, 11, 12]', data = 150 WHERE id = 1`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO vectors (id, vec, data) VALUES (4, '[13, 14, 15]', 400)`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `DELETE FROM vectors WHERE id = 2`)
	require.NoError(t, err)

	// This update should succeed since it only modifies the data column
	_, err = db.ExecContext(ctx, `UPDATE vectors SET data = 250 WHERE id = 3`)
	require.NoError(t, err)

	// Unblock the backfill process
	close(blockBackfill)

	wg.Wait()
	// Wait for the index creation to complete
	require.NoError(t, createIndexErr)

	// Verify the index was created successfully
	var id int
	sqlDB.QueryRow(t, `SELECT id FROM vectors@vec_idx ORDER BY vec <-> '[13, 14, 15]' LIMIT 1`).Scan(&id)
	require.Equal(t, 4, id)

	// This doesn't really tell us anything because we always join on the PK, which will have properly
	// deleted the row.
	sqlDB.QueryRow(t, `SELECT id FROM vectors@vec_idx ORDER BY vec <-> '[4, 5, 6]' LIMIT 1`).Scan(&id)
	require.NotEqual(t, 2, id)

	var data int
	sqlDB.QueryRow(t, `SELECT id, data FROM vectors@vec_idx ORDER BY vec <-> '[7, 8, 9]' LIMIT 1`).Scan(&id, &data)
	require.Equal(t, 3, id)
	require.Equal(t, 250, data)
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

func TestVectorIndexMergingDuringBackfill(t *testing.T) {
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

	// Enable deterministic vector index fixups for consistent testing.
	sqlDB.Exec(t, `SET CLUSTER SETTING sql.vecindex.deterministic_fixups.enabled = true`)

	// Create a table with an integer PK column and a dimension 1 vector column
	sqlDB.Exec(t, `
		CREATE TABLE test_vectors (
			pk INT PRIMARY KEY,
			vec VECTOR(1)
		)
	`)

	// Insert 1000 rows where PK value and vector element are the same
	sqlDB.Exec(t, `
		INSERT INTO test_vectors (pk, vec)
		SELECT
			i as pk,
			ARRAY[i::float]::vector(1) as vec
		FROM generate_series(1, 1000) as i
	`)

	wg := sync.WaitGroup{}
	wg.Add(1)

	// Start creating the vector index in a goroutine
	var createIndexErr error
	go func() {
		defer wg.Done()
		_, createIndexErr = db.ExecContext(ctx, `CREATE VECTOR INDEX vec_idx ON test_vectors (vec)`)
		if createIndexErr != nil {
			backfillBlocked <- struct{}{}
		}
	}()

	// Wait for the backfill to be blocked
	<-backfillBlocked

	// Insert 100 more vectors into the table (values 1001-1100)
	sqlDB.Exec(t, `
		INSERT INTO test_vectors (pk, vec)
		SELECT
			i as pk,
			ARRAY[i::float]::vector(1) as vec
		FROM generate_series(1001, 1100) as i
	`)

	// Unblock the backfill process
	close(blockBackfill)

	wg.Wait()
	// Wait for the index creation to complete
	require.NoError(t, createIndexErr)

	// Search individually for vectors from the second set of inserts (1001-1100)
	// We should find at least 90% of those vectors
	foundCount := 0
	for i := 1001; i <= 1100; i++ {
		var pk int
		err := sqlDB.DB.QueryRowContext(ctx, `
			SELECT pk FROM test_vectors@vec_idx
			ORDER BY vec <-> ARRAY[$1::float]::vector(1)
			LIMIT 1
		`, i).Scan(&pk)
		if err == nil && pk == i {
			foundCount++
		}
	}

	expectedMinFound := int(0.9 * 100) // 90% of 100 vectors
	require.GreaterOrEqual(t, foundCount, expectedMinFound,
		"Expected to find at least 90%% of the vectors from the second insert set, found %d out of 100", foundCount)
}

// TestVectorIndexMergingDuringBackfillWithPrefix tests vector index creation
// with concurrent inserts during backfill, using a prefix column and multiple
// column families. This validates both the merging functionality and the
// span.Splitter optimization in MergeVector that only fetches needed column families.
func TestVectorIndexMergingDuringBackfillWithPrefix(t *testing.T) {
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

	// Enable deterministic vector index fixups for consistent testing.
	sqlDB.Exec(t, `SET CLUSTER SETTING sql.vecindex.deterministic_fixups.enabled = true`)

	// Create a table with an integer PK column, prefix column, and a dimension 1 vector column
	// Use multiple column families to test the span.Splitter optimization in MergeVector
	sqlDB.Exec(t, `
		CREATE TABLE test_vectors_with_prefix (
			pk INT PRIMARY KEY,
			prefix_col INT,
			vec VECTOR(1),
			FAMILY key_family (pk, prefix_col),
			FAMILY vector_family (vec)
		)
	`)

	// Insert 5000 rows with random prefix distribution:
	// 50% to prefix 1, 20% to prefix 2, 10% each to prefixes 3, 4, 5
	sqlDB.Exec(t, `
		INSERT INTO test_vectors_with_prefix (pk, prefix_col, vec)
		SELECT
			i as pk,
			CASE
				WHEN random() < 0.5 THEN 1
				WHEN random() < 0.7 THEN 2  -- 20% of remaining 50%
				WHEN random() < 0.8 THEN 3  -- 10% of remaining 30%
				WHEN random() < 0.9 THEN 4  -- 10% of remaining 20%
				ELSE 5                      -- 10% of remaining 10%
			END as prefix_col,
			ARRAY[i::float]::vector(1) as vec
		FROM generate_series(1, 5000) as i
	`)

	wg := sync.WaitGroup{}
	wg.Add(1)

	// Start creating the vector index with prefix column in a goroutine
	var createIndexErr error
	go func() {
		defer wg.Done()
		_, createIndexErr = db.ExecContext(ctx, `CREATE VECTOR INDEX vec_prefix_idx ON test_vectors_with_prefix (prefix_col, vec)`)
		if createIndexErr != nil {
			backfillBlocked <- struct{}{}
		}
	}()

	// Wait for the backfill to be blocked
	<-backfillBlocked

	// Insert 100 more vectors into the table (values 5001-5100) with same distribution
	sqlDB.Exec(t, `
		INSERT INTO test_vectors_with_prefix (pk, prefix_col, vec)
		SELECT
			i as pk,
			CASE
				WHEN random() < 0.5 THEN 1
				WHEN random() < 0.7 THEN 2
				WHEN random() < 0.8 THEN 3
				WHEN random() < 0.9 THEN 4
				ELSE 5
			END as prefix_col,
			ARRAY[i::float]::vector(1) as vec
		FROM generate_series(5001, 5100) as i
	`)

	// Unblock the backfill process
	close(blockBackfill)

	wg.Wait()
	// Wait for the index creation to complete
	require.NoError(t, createIndexErr)

	// Search individually for vectors from the second set of inserts (5001-5100)
	// We need to get the actual prefix values for each row since they were randomly assigned
	type rowData struct {
		pk     int
		prefix int
	}

	var secondSetRows []rowData
	rows := sqlDB.Query(t, `
		SELECT pk, prefix_col
		FROM test_vectors_with_prefix
		WHERE pk BETWEEN 5001 AND 5100
		ORDER BY pk
	`)
	defer rows.Close()

	for rows.Next() {
		var rd rowData
		require.NoError(t, rows.Scan(&rd.pk, &rd.prefix))
		secondSetRows = append(secondSetRows, rd)
	}

	// Search for each vector using the prefix-aware index
	foundCount := 0
	for _, row := range secondSetRows {
		var pk int
		// Use the pk value as the vector element since vec was created as ARRAY[i::float]
		err := sqlDB.DB.QueryRowContext(ctx, `
			SELECT pk FROM test_vectors_with_prefix@vec_prefix_idx
			WHERE prefix_col = $1
			ORDER BY vec <-> ARRAY[$2::float]::vector(1)
			LIMIT 1
		`, row.prefix, row.pk).Scan(&pk)
		if err == nil && pk == row.pk {
			foundCount++
		}
	}

	expectedMinFound := int(0.9 * 100) // 90% of 100 vectors
	require.GreaterOrEqual(t, foundCount, expectedMinFound,
		"Expected to find at least 90%% of the vectors from the second insert set, found %d out of 100", foundCount)
}
