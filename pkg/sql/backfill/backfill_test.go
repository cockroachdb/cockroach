// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfill_test

import (
	"context"
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

	// Create a table with a vector column
	sqlDB.Exec(t, `
		CREATE TABLE vectors (
			id INT PRIMARY KEY,
			vec VECTOR(3)
		)
	`)

	// Insert 200 rows with random vector data
	sqlDB.Exec(t, `
		INSERT INTO vectors (id, vec)
		SELECT 
			generate_series(1, 200) as id,
			ARRAY[random(), random(), random()]::vector(3) as vec
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
