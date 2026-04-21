// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestRevlogSiblingJobE2E exercises the full WITH REVISION STREAM path:
//
//  1. A regular `BACKUP INTO ... WITH REVISION STREAM` runs to
//     completion. As part of its execution it performs the
//     create-or-noop dance (see maybeCreateRevlogSiblingJob) and
//     creates a sibling BACKUP job whose details have RevLogJob=true.
//  2. The sibling job is adopted automatically; its resumer dispatches
//     to runRevlogJob, which calls revlogjob.Run. That spins up the
//     producer-side DistSQL flow, opens a rangefeed over the tenant's
//     keyspace, and starts writing data files + manifests under
//     `log/` at the BACKUP collection root.
//  3. The test waits for at least one closed-tick manifest to appear
//     at the destination (`log/resolved/<day>/<time>.pb`) and then
//     cancels the sibling job.
//
// The test runs against a single-node test cluster — no production
// rangefeed setup beyond the defaults the test server gives us.
func TestRevlogSiblingJobE2E(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, sqlDB, dir, cleanup := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanup()

	const destSubdir = "revlog-e2e"
	dest := "nodelocal://1/" + destSubdir

	// The revlog producer uses a rangefeed over the tenant's keyspace,
	// which requires the kv.rangefeed.enabled cluster setting.
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	// Fire the parent BACKUP. The parent finishes when its data is
	// written (it doesn't wait on the sibling). Once it returns, the
	// sibling revlog job has been created and is being adopted.
	sqlDB.Exec(t, `BACKUP INTO $1 WITH REVISION STREAM`, dest)

	// Locate the sibling job ID via the marker file the parent wrote.
	collectionDir := filepath.Join(dir, destSubdir)
	siblingID := readSiblingJobID(t, collectionDir)
	t.Logf("sibling revlog job ID = %d", siblingID)

	// Wait for the sibling to be running.
	jobutils.WaitForJobToRun(t, sqlDB, siblingID)

	// Wait for at least one closed-tick manifest to land in
	// log/resolved/. Default tick width is 10s plus rangefeed
	// checkpoint cadence, so allow generous time.
	resolvedDir := filepath.Join(collectionDir, "log", "resolved")
	require.NoError(t, testutils.SucceedsWithinError(func() error {
		count, err := countManifests(resolvedDir)
		if err != nil {
			return err
		}
		if count == 0 {
			return errors.Newf("no manifests yet under %s", resolvedDir)
		}
		t.Logf("found %d closed-tick manifest(s) under %s", count, resolvedDir)
		return nil
	}, 90*time.Second))

	// Cancel the sibling so the test cleanup doesn't hang.
	sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
	jobutils.WaitForJobToCancel(t, sqlDB, siblingID)
}

// readSiblingJobID reads the `log/job.latest-1` marker file written by
// maybeCreateRevlogSiblingJob and returns the embedded job ID.
func readSiblingJobID(t *testing.T, collectionDir string) jobspb.JobID {
	t.Helper()
	markerPath := filepath.Join(collectionDir, "log", "job.latest-1")
	body, err := os.ReadFile(markerPath)
	require.NoError(t, err, "reading sibling job marker %s", markerPath)
	id, err := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
	require.NoError(t, err, "parsing sibling job marker contents %q", body)
	return jobspb.JobID(id)
}

// countManifests walks log/resolved/ and counts the .pb files (closed
// tick markers). Returns 0 if the directory does not yet exist.
func countManifests(resolvedDir string) (int, error) {
	count := 0
	err := filepath.Walk(resolvedDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".pb") {
			count++
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return 0, err
	}
	return count, nil
}
