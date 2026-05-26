// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkutil_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCleanupOrphanedFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tempDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: tempDir,
	})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Build a storage factory backed by real nodelocal storage.
	clientFactory := blobs.TestBlobServiceClient(tempDir)
	testSettings := cluster.MakeTestingClusterSettings()
	factory := func(
		ctx context.Context,
		uri string,
		user username.SQLUsername,
		opts ...cloud.ExternalStorageOption,
	) (cloud.ExternalStorage, error) {
		conf, err := cloud.ExternalStorageConfFromURI(uri, user)
		if err != nil {
			return nil, err
		}
		return cloud.MakeExternalStorage(
			ctx, conf, base.ExternalIODirConfig{}, testSettings, clientFactory,
			nil, nil, cloud.NilMetrics,
		)
	}

	db := s.InternalDB().(isql.DB)

	// writeJobFile creates a file at <tempDir>/job/<jobID>/<filename>.
	writeJobFile := func(t *testing.T, jobID int64, filename string) {
		t.Helper()
		dir := filepath.Join(
			tempDir, "job", strconv.FormatInt(jobID, 10), filepath.Dir(filename),
		)
		require.NoError(t, os.MkdirAll(dir, 0755))
		require.NoError(t, os.WriteFile(
			filepath.Join(tempDir, "job", strconv.FormatInt(jobID, 10), filename),
			[]byte("data"), 0644,
		))
	}

	// fileExists checks whether the file exists on disk.
	fileExists := func(jobID int64, filename string) bool {
		_, err := os.Stat(filepath.Join(
			tempDir, "job", strconv.FormatInt(jobID, 10), filename,
		))
		return err == nil
	}

	// insertJob inserts a job record with the given ID and status.
	insertJob := func(jobID int64, status jobs.State) {
		runner.Exec(t,
			`INSERT INTO system.jobs (id, status, created, job_type)
			 VALUES ($1, $2, now(), 'IMPORT')`,
			jobID, string(status),
		)
	}

	t.Run("no job directories", func(t *testing.T) {
		err := bulkutil.CleanupOrphanedFiles(ctx, factory, db)
		require.NoError(t, err)
	})

	t.Run("terminal jobs have files deleted", func(t *testing.T) {
		insertJob(100000, jobs.StateSucceeded)
		insertJob(100001, jobs.StateFailed)
		writeJobFile(t, 100000, "map/file1.sst")
		writeJobFile(t, 100000, "merge/file2.sst")
		writeJobFile(t, 100001, "map/file3.sst")

		err := bulkutil.CleanupOrphanedFiles(ctx, factory, db)
		require.NoError(t, err)

		require.False(t, fileExists(100000, "map/file1.sst"))
		require.False(t, fileExists(100000, "merge/file2.sst"))
		require.False(t, fileExists(100001, "map/file3.sst"))
	})

	t.Run("active jobs have files preserved", func(t *testing.T) {
		insertJob(100002, jobs.StateRunning)
		insertJob(100003, jobs.StatePaused)
		writeJobFile(t, 100002, "map/file1.sst")
		writeJobFile(t, 100003, "map/file2.sst")

		err := bulkutil.CleanupOrphanedFiles(ctx, factory, db)
		require.NoError(t, err)

		require.True(t, fileExists(100002, "map/file1.sst"))
		require.True(t, fileExists(100003, "map/file2.sst"))
	})

	t.Run("nonexistent jobs have files deleted", func(t *testing.T) {
		// Job 100004 is not in system.jobs.
		writeJobFile(t, 100004, "map/file1.sst")

		err := bulkutil.CleanupOrphanedFiles(ctx, factory, db)
		require.NoError(t, err)

		require.False(t, fileExists(100004, "map/file1.sst"))
	})

	t.Run("non-numeric directory names skipped", func(t *testing.T) {
		// Create a directory with a non-numeric name.
		nonNumericDir := filepath.Join(tempDir, "job", "notanumber")
		require.NoError(t, os.MkdirAll(nonNumericDir, 0755))
		require.NoError(t, os.WriteFile(
			filepath.Join(nonNumericDir, "file1.sst"), []byte("data"), 0644,
		))

		insertJob(100005, jobs.StateSucceeded)
		writeJobFile(t, 100005, "map/file1.sst")

		err := bulkutil.CleanupOrphanedFiles(ctx, factory, db)
		require.NoError(t, err)

		// Job 100005 files deleted; "notanumber" directory ignored.
		require.False(t, fileExists(100005, "map/file1.sst"))
		_, err = os.Stat(filepath.Join(nonNumericDir, "file1.sst"))
		require.NoError(t, err, "non-numeric directory file should be preserved")
	})

	t.Run("mixed terminal and active jobs", func(t *testing.T) {
		insertJob(100010, jobs.StateSucceeded)
		insertJob(100011, jobs.StateRunning)
		insertJob(100012, jobs.StateCanceled)
		insertJob(100013, jobs.StateReverting)
		writeJobFile(t, 100010, "map/file1.sst")
		writeJobFile(t, 100011, "map/file2.sst")
		writeJobFile(t, 100012, "map/file3.sst")
		writeJobFile(t, 100013, "merge/file4.sst")

		err := bulkutil.CleanupOrphanedFiles(ctx, factory, db)
		require.NoError(t, err)

		require.False(t, fileExists(100010, "map/file1.sst"))
		require.True(t, fileExists(100011, "map/file2.sst"))
		require.False(t, fileExists(100012, "map/file3.sst"))
		require.True(t, fileExists(100013, "merge/file4.sst"))
	})

	t.Run("revert-failed jobs have files deleted", func(t *testing.T) {
		insertJob(100020, jobs.StateRevertFailed)
		writeJobFile(t, 100020, "map/file1.sst")

		err := bulkutil.CleanupOrphanedFiles(ctx, factory, db)
		require.NoError(t, err)

		require.False(t, fileExists(100020, "map/file1.sst"))
	})
}
