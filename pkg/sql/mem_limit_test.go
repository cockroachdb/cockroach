// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestMemoryLimit verifies that we're hitting memory budget errors when reading
// large blobs.
func TestMemoryLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We choose values for --max-sql-memory and the size of a single large blob
	// to be such that a single value could be read without hitting the memory
	// limit, but multiple blobs in a single BatchResponse or across multiple
	// BatchResponses do hit the memory limit.
	//
	// When the Streamer API is not used, then SQL creates a single BatchRequest
	// with TargetBytes of 10MiB (controlled by
	// rowinfra.defaultBatchBytesLimitProductionValue), so the storage layer is
	// happy to get two blobs only to hit the memory limit. A concurrency of 1
	// is sufficient.
	//
	// When the Streamer API is used, TargetBytes are set, so each blob gets a
	// separate BatchResponse. Thus, we need concurrency of 2 so that the
	// aggregate memory usage exceeds the memory limit. It's also likely that
	// the error is encountered in the SQL layer when performing accounting for
	// the read datums.
	//
	// The size here is constructed in such a manner that both rows are returned
	// in a single ScanResponse, meaning that both rows together (including
	// non-blob columns) don't exceed 10MiB.
	const blobSize = 5<<20 - 1<<10 /* 5MiB - 1KiB */
	serverArgs := base.TestServerArgs{
		SQLMemoryPoolSize: 2*blobSize - 1,
	}
	serverArgs.Knobs.SQLEvalContext = &eval.TestingKnobs{
		// This test expects the default value of
		// rowinfra.defaultBatchBytesLimit.
		ForceProductionValues: true,
	}
	s, db, _ := serverutils.StartServer(t, serverArgs)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec("CREATE TABLE foo (id INT PRIMARY KEY, attribute INT, blob TEXT, INDEX(attribute))")
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO foo SELECT 1, 10, repeat('a', %d)", blobSize))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO foo SELECT 2, 10, repeat('a', %d)", blobSize))
	require.NoError(t, err)

	for _, tc := range []struct {
		query       string
		concurrency int
	}{
		// Simple read without using the Streamer API - no need for concurrency.
		{
			query:       "SELECT * FROM foo WHERE blob LIKE 'blah%'",
			concurrency: 1,
		},
		// Perform an index join to read large blobs. It is done via the
		// Streamer API, so we need the concurrency to hit the memory error.
		{
			query:       "SELECT * FROM foo@foo_attribute_idx WHERE attribute=10 AND blob LIKE 'blah%'",
			concurrency: 2,
		},
	} {
		testutils.RunTrueAndFalse(t, "vectorize", func(t *testing.T, vectorize bool) {
			vectorizeMode := "off"
			if vectorize {
				vectorizeMode = "on"
			}
			_, err = db.Exec("SET vectorize = " + vectorizeMode)
			require.NoError(t, err)

			testutils.SucceedsSoon(t, func() error {
				var wg sync.WaitGroup
				wg.Add(tc.concurrency)
				errCh := make(chan error, tc.concurrency)
				for i := 0; i < tc.concurrency; i++ {
					go func(query string) {
						defer wg.Done()
						_, err := db.Exec(query)
						if err != nil {
							errCh <- err
						}
					}(tc.query)
				}
				wg.Wait()
				close(errCh)

				memoryErrorFound := false
				for err := range errCh {
					if strings.Contains(err.Error(), "memory budget exceeded") {
						switch tc.concurrency {
						case 1:
							// Currently in this case we're not using the
							// Streamer API, so we expect to hit the error when
							// performing the memory accounting when evaluating
							// MVCC requests.
							if strings.Contains(err.Error(), "scan with start key") {
								memoryErrorFound = true
							}
						case 2:
							// Here we are using the Streamer API and are ok
							// with the error being encountered at any point.
							memoryErrorFound = true
						default:
							return errors.Newf("unexpected concurrency %d", tc.concurrency)
						}

					} else {
						return err
					}
				}
				if !memoryErrorFound {
					return errors.New("memory budget exceeded error wasn't hit")
				}
				return nil
			})
		})
	}
}

// TestStreamerTightBudget verifies that the Streamer utilizes its available
// budget as tightly as possible, without incurring unnecessary debt. It gives
// the Streamer such a budget that a single result puts it in debt, so there
// should be no more than a single request "in progress" (i.e. one request in
// flight or one unreleased result).
func TestStreamerTightBudget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start a cluster with large --max-sql-memory parameter so that the
	// Streamer isn't hitting the root budget exceeded error.
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: 1 << 30, /* 1GiB */
		Insecure:          true,
	})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	const blobSize = 1 << 20
	const numRows = 5

	sqlDB.Exec(t, "CREATE TABLE t (pk INT PRIMARY KEY, k INT, blob STRING, INDEX (k))")
	for i := 0; i < numRows; i++ {
		if i > 0 {
			// Create a new range for this row.
			sqlDB.Exec(t, fmt.Sprintf("ALTER TABLE t SPLIT AT VALUES(%d)", i))
		}
		sqlDB.Exec(t, fmt.Sprintf("INSERT INTO t SELECT %d, 1, repeat('a', %d)", i, blobSize))
	}

	// Populate the range cache.
	sqlDB.Exec(t, "SELECT count(*) from t")

	// Set the workmem limit to a low value such that it will allow the Streamer
	// to have at most one request to be "in progress".
	sqlDB.Exec(t, fmt.Sprintf("SET distsql_workmem = '%dB'", blobSize))

	// Perform an index join to read the blobs.
	query := "SELECT sum(length(blob)) FROM t@t_k_idx WHERE k = 1"
	maximumMemoryUsageRegex := regexp.MustCompile(`maximum memory usage: (\d+\.\d+) MiB`)
	rows := sqlDB.QueryStr(t, "EXPLAIN ANALYZE (VERBOSE) "+query)
	// Build pretty output for an error message in case we need it.
	var sb strings.Builder
	for _, row := range rows {
		sb.WriteString(row[0] + "\n")
	}
	for _, row := range rows {
		if matches := maximumMemoryUsageRegex.FindStringSubmatch(row[0]); len(matches) > 0 {
			usage, err := strconv.ParseFloat(matches[1], 64)
			require.NoError(t, err)
			// We expect that the maximum memory usage is about 2MiB (1MiB is
			// accounted for by the Streamer, and another 1MiB is accounted for
			// by the ColIndexJoin when the blob is copied into the columnar
			// batch). We allow for 0.1MiB for other memory usage in the query.
			maxAllowed := 2.1
			if maxAllowed >= usage {
				return
			}
			// Get a stmt bundle for this query that might help in debugging the
			// failure.
			rows = sqlDB.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) "+query)
			url := getBundleDownloadURL(t, fmt.Sprint(rows))
			// First check whether the bundle experienced the elevated memory
			// usage too.
			unzip := downloadAndUnzipBundle(t, url)
			for _, f := range unzip.File {
				switch f.Name {
				case "plan.txt":
					contents := readUnzippedFile(t, f)
					sb.WriteString("\n\n\nbundle plan.txt:\n\n\n")
					sb.WriteString(contents)
					// Check that the memory usage is still elevated.
					if matches := maximumMemoryUsageRegex.FindStringSubmatch(contents); len(matches) == 0 {
						t.Fatalf("unexpectedly didn't find a match for maximum memory usage\n%s", sb.String())
					} else {
						bundleUsage, err := strconv.ParseFloat(matches[1], 64)
						require.NoError(t, err)
						if maxAllowed >= bundleUsage {
							// Memory usage is as expected in the bundle - do
							// not fail the test since we don't have any
							// information to help in understanding the original
							// high memory usage.
							return
						}
					}
				}
			}
			// NB: we're not using t.TempDir() because we want these to survive
			// on failure.
			f, err := os.Create(filepath.Join(datapathutils.DebuggableTempDir(), "bundle.zip"))
			require.NoError(t, err)
			downloadBundle(t, url, f)
			t.Fatalf("unexpectedly high memory usage\n----\n%s", sb.String())
		}
	}
	t.Fatalf("unexpectedly didn't find a match for maximum memory usage\n%s", sb.String())
}
