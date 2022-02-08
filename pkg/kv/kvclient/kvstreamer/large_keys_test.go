// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstreamer_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

// TestLargeKeys verifies that the Streamer successfully completes the queries
// when the keys to lookup (i.e. the enqueued requests themselves) as well as
// the looked up rows are large. It additionally ensures that the Streamer
// issues a reasonable number of the KV request.
func TestLargeKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name  string
		query string
	}{
		{
			name:  "no ordering",
			query: "SELECT * FROM foo@foo_attribute_idx WHERE attribute=1",
		},
	}

	rng, _ := randutil.NewTestRand()
	recCh := make(chan tracing.Recording, 1)
	// We want to capture the trace of the query so that we can count how many
	// KV requests the Streamer issued.
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				WithStatementTrace: func(trace tracing.Recording, stmt string) {
					for _, tc := range testCases {
						if tc.query == stmt {
							recCh <- trace
						}
					}
				},
			},
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Lower the distsql_workmem limit so that we can operate with smaller
	// blobs. Note that the joinReader in the row-by-row engine will override
	// the limit if it is lower than 100KiB, so we cannot go lower than that
	// here.
	_, err := db.Exec("SET distsql_workmem='100KiB'")
	require.NoError(t, err)
	// To improve the test coverage, occasionally lower the maximum number of
	// concurrent requests.
	if rng.Float64() < 0.25 {
		_, err = db.Exec("SET CLUSTER SETTING kv.streamer.concurrency_limit = $1", rng.Intn(10)+1)
		require.NoError(t, err)
	}
	// In both engines, the index joiner will buffer input rows up to a quarter
	// of workmem limit, so we have several interesting options for the blob
	// size:
	// - around 5000 is interesting because multiple requests are enqueued at
	// same time and there might be parallelism going on within the Streamer.
	// - 20000 is interesting because it doesn't exceed the buffer size, yet two
	// rows with such blobs do exceed it. The index joiners are expected to to
	// process each row on its own.
	// - 40000 is interesting because a single row already exceeds the buffer
	// size.
	for _, pkBlobSize := range []int{3000 + rng.Intn(4000), 20000, 40000} {
		// useScans indicates whether we want Scan requests to be used by the
		// Streamer (if we do, then we need to have multiple column families).
		for _, useScans := range []bool{false, true} {
			// onlyLarge determines whether only large blobs are inserted or a
			// mix of large and small blobs.
			for _, onlyLarge := range []bool{false, true} {
				_, err = db.Exec("DROP TABLE IF EXISTS foo")
				require.NoError(t, err)
				// We set up such a table that contains two large columns, one
				// of them being the primary key. The idea is that the test
				// query will first read from the secondary index which would
				// include only the PK blob, and that will be used to construct
				// index join lookups (i.e. the PK blobs will be the enqueued
				// requests for the Streamer) whereas the other blob will be
				// part of the response.
				var familiesSuffix string
				// In order to use Scan requests we need to have multiple column
				// families.
				if useScans {
					familiesSuffix = ", FAMILY (pk_blob, attribute, extra), FAMILY (blob)"
				}
				_, err = db.Exec(fmt.Sprintf(
					`CREATE TABLE foo (
						pk_blob STRING PRIMARY KEY, attribute INT8, extra INT8, blob STRING,
						INDEX (attribute)%s
					);`, familiesSuffix))
				require.NoError(t, err)

				// Insert some number of rows.
				numRows := rng.Intn(10) + 10
				for i := 0; i < numRows; i++ {
					letter := string(byte('a') + byte(i))
					valueSize := pkBlobSize
					if !onlyLarge && rng.Float64() < 0.5 {
						// If we're using a mix of large and small values, with
						// 50% use a small value now.
						valueSize = rng.Intn(10) + 1
					}
					// We randomize the value size for 'blob' column to improve
					// the test coverage.
					blobSize := int(float64(valueSize)*2.0*rng.Float64()) + 1
					_, err = db.Exec("INSERT INTO foo SELECT repeat($1, $2), 1, 1, repeat($1, $3);", letter, valueSize, blobSize)
					require.NoError(t, err)
				}

				// Try two scenarios: one with a single range (so no parallelism
				// within the Streamer) and another with a random number of
				// ranges (which might add parallelism within the Streamer).
				//
				// Note that a single range scenario needs to be exercised first
				// in order to reuse the same table without dropping it (we
				// don't want to deal with merging ranges).
				for _, newRangeProbability := range []float64{0, rng.Float64()} {
					for i := 1; i < numRows; i++ {
						if rng.Float64() < newRangeProbability {
							// Create a new range.
							letter := string(byte('a') + byte(i))
							_, err = db.Exec("ALTER TABLE foo SPLIT AT VALUES ($1);", letter)
							require.NoError(t, err)
						}
					}
					// Populate the range cache.
					_, err = db.Exec("SELECT count(*) FROM foo")
					require.NoError(t, err)

					for _, vectorizeMode := range []string{"on", "off"} {
						_, err = db.Exec("SET vectorize = " + vectorizeMode)
						require.NoError(t, err)
						for _, tc := range testCases {
							t.Run(fmt.Sprintf(
								"%s/size=%s/scans=%t/onlyLarge=%t/numRows=%d/newRangeProb=%.2f/vec=%s",
								tc.name, humanize.Bytes(uint64(pkBlobSize)), useScans,
								onlyLarge, numRows, newRangeProbability, vectorizeMode,
							),
								func(t *testing.T) {
									_, err = db.Exec(tc.query)
									require.NoError(t, err)
									// Now examine the trace and count the async
									// requests issued by the Streamer.
									tr := <-recCh
									var numStreamerRequests int
									for _, rs := range tr {
										if rs.Operation == kvstreamer.AsyncRequestOp {
											numStreamerRequests++
										}
									}
									// Assert that the number of requests is
									// reasonable using the number of rows as
									// the proxy for how many requests need to
									// be issued. We expect some requests to
									// come back empty because of a low initial
									// TargetBytes limit, some requests might
									// get an empty result multiple times while
									// we're figuring out the correct limit, so
									// we use a 4x multiple on the number of
									// rows.
									require.Greater(t, 4*numRows, numStreamerRequests)
								})
						}
					}
				}
			}
		}
	}
}
