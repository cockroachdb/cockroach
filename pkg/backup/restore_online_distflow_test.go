// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestOnlineRestoreDistFlowSplitScatter verifies that online restore correctly
// pre-splits spans so linked external files are distributed across ranges.
//
// The test strategy:
// 1. Block all reads of backup data SSTs at the storage layer
// 2. Inflate backup file stats by 100,000x (making ~3KB files appear as ~300MB)
// 3. Force-enqueue ranges into the split queue after each LinkExternalSSTable
// 4. Create a deep backup (9 incremental layers) with files across multiple ranges
//
// If restore fails to pre-split properly:
// - All files accumulate on one range
// - Inflated stats trigger write backpressure (blocking further links)
// - The range must split, but split-key finding requires reading blocked SSTs
// - Result: deadlock/timeout
//
// If restore correctly pre-splits:
// - Files distribute across ranges
// - No backpressure triggers
// - Blocked reads are never hit
func TestOnlineRestoreDistFlowSplitScatter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Block reads of backup data SSTs to simulate high S3 latency.
	blockCh := make(chan struct{})
	defer nodelocal.ReplaceNodeLocalForTestingWithInterceptor(
		t.TempDir(),
		func(ctx context.Context, basename string) {
			if strings.HasPrefix(basename, "data/") && strings.HasSuffix(basename, ".sst") {
				select {
				case <-ctx.Done():
				case <-blockCh:
				}
			}
		},
	)()

	ctx := context.Background()
	const statsMultiplier = 100000 // Inflate file sizes to trigger backpressure

	var storePtr atomic.Pointer[kvserver.Store]

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				// Disable queues that read external SSTs unrelated to split finding.
				DisableMergeQueue:       true,
				DisableConsistencyQueue: true,
				// After each link, enqueue the range into the split queue.
				// This triggers backpressure checks on subsequent links.
				TestingResponseFilter: func(
					ctx context.Context, ba *kvpb.BatchRequest, _ *kvpb.BatchResponse,
				) *kvpb.Error {
					st := storePtr.Load()
					if st == nil {
						return nil
					}
					for _, req := range ba.Requests {
						if _, ok := req.GetInner().(*kvpb.LinkExternalSSTableRequest); ok {
							if repl, err := st.GetReplica(ba.Header.RangeID); err == nil {
								st.Enqueue(ctx, "split", repl, true /* skipShouldQueue */, true /* async */)
							}
							return nil
						}
					}
					return nil
				},
			},
			// Inflate file stats in both execution paths.
			BackupRestore: &sql.BackupRestoreTestingKnobs{
				BackupProcessFileOverride: func(f backuppb.BackupManifest_File) backuppb.BackupManifest_File {
					f.EntryCounts.DataSize *= statsMultiplier
					f.ApproximatePhysicalSize *= statsMultiplier
					return f
				},
			},
			DistSQL: &execinfra.TestingKnobs{
				BackupRestoreTestingKnobs: &sql.BackupRestoreTestingKnobs{
					BackupProcessFileOverride: func(f backuppb.BackupManifest_File) backuppb.BackupManifest_File {
						f.EntryCounts.DataSize *= statsMultiplier
						f.ApproximatePhysicalSize *= statsMultiplier
						return f
					},
				},
			},
		},
	})

	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	if err != nil {
		t.Fatal(err)
	}
	storePtr.Store(store)

	// Close blockCh before server shutdown to avoid deadlock with Pebble's
	// table stats collector, which reads external SSTs asynchronously.
	defer s.Stopper().Stop(ctx)
	defer close(blockCh)

	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Increase backpressure tolerance so inflated stats reliably trigger it.
	runner.Exec(t, "SET CLUSTER SETTING kv.range.backpressure_byte_tolerance = '10 GiB'")

	// Create test data across multiple ranges.
	runner.Exec(t, "CREATE DATABASE data")
	runner.Exec(t, "CREATE TABLE data.bank (id INT PRIMARY KEY, balance INT, payload STRING)")
	for i := 0; i < 100; i++ {
		runner.Exec(t, "INSERT INTO data.bank VALUES ($1, $2, $3)",
			i, i*100, strings.Repeat("x", 100))
	}
	runner.Exec(t, "ALTER TABLE data.bank SPLIT AT VALUES (20), (40), (60), (80)")

	// Create backup with 10 total layers (1 full + 9 incremental).
	const backupURI = "nodelocal://1/backup"
	runner.Exec(t, fmt.Sprintf("BACKUP TABLE data.bank INTO '%s'", backupURI))

	// Each incremental updates a sliding window of rows for varied SST boundaries.
	for i := 0; i < 9; i++ {
		start := (i * 13) % 100
		end := start + 35
		if end > 100 {
			end = 100
		}
		runner.Exec(t, "UPDATE data.bank SET balance = balance + 1 WHERE id >= $1 AND id < $2",
			start, end)
		runner.Exec(t, fmt.Sprintf("BACKUP TABLE data.bank INTO LATEST IN '%s'", backupURI))
	}

	// Pause at download phase - if link phase completes, test passes.
	runner.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.before_download'")

	// Helper to run restore and verify link phase completes without deadlock.
	restoreAndCancel := func(t *testing.T) {
		runner.Exec(t, "DROP TABLE IF EXISTS data.bank")
		runner.Exec(t, fmt.Sprintf(
			"RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY",
			backupURI))

		// Cancel the paused download job for cleanup.
		var downloadJobID jobspb.JobID
		runner.QueryRow(t, latestDownloadJobIDQuery).Scan(&downloadJobID)
		runner.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		runner.Exec(t, fmt.Sprintf("CANCEL JOB %d", downloadJobID))
		runner.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.before_download'")
	}

	t.Run("goroutine-path", func(t *testing.T) {
		runner.Exec(t, "SET CLUSTER SETTING backup.restore.online_use_dist_flow.enabled = false")
		restoreAndCancel(t)
	})

	t.Run("distflow-path", func(t *testing.T) {
		runner.Exec(t, "SET CLUSTER SETTING backup.restore.online_use_dist_flow.enabled = true")
		restoreAndCancel(t)
	})
}
