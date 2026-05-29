// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/tests"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var diskFillBackupTables = []string{
	"warehouse", "district", "item", "new_order",
}

const (
	// pressureDuration is how long the RESTORE job runs under disk
	// pressure before we explicitly pause it.
	pressureDuration = 30 * time.Second
	// stabilizeWait is how long to wait after removing ballast files
	// before resuming the job, giving the node time to recover.
	stabilizeWait = 30 * time.Second
	// jobResumeTimeout is the maximum time to wait for the RESTORE
	// job to succeed after being resumed.
	jobResumeTimeout = 30 * time.Minute
)

type cleanupDiskFillBulkIngest struct {
	node          option.NodeListOption
	peerNodeID    int
	storeCount    int
	restoreDBName string
	bucket        string
	jobID         jobspb.JobID
}

func (cl *cleanupDiskFillBulkIngest) Cleanup(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) {
	if cl.node != nil {
		o.Status("cleanup: removing ballast files")
		removeFillFiles(ctx, o, c, cl.node, cl.storeCount)
	}

	conn := c.Conn(ctx, o.L(), cl.peerNodeID, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	// Cancel any in-flight backup/restore jobs created by this operation.
	if cl.bucket != "" {
		o.Status("cleanup: cancelling in-flight backup/restore jobs")
		cancelQuery := `
			WITH jobs AS (SHOW JOBS)
			SELECT job_id FROM jobs
			WHERE job_type IN ('BACKUP', 'RESTORE')
			AND status IN ('running', 'paused', 'pause-requested', 'reverting')
			AND (description LIKE '%' || $1 || '%' OR description LIKE '%' || $2 || '%')
		`
		rows, err := conn.QueryContext(ctx, cancelQuery, cl.bucket, cl.restoreDBName)
		if err != nil {
			o.Errorf("failed to query for in-flight jobs: %v", err)
		} else {
			defer rows.Close()
			for rows.Next() {
				var jobID string
				if err := rows.Scan(&jobID); err != nil {
					o.Errorf("failed to scan job ID: %v", err)
					continue
				}
				o.Status(fmt.Sprintf("cleanup: cancelling job %s", jobID))
				if _, err := conn.ExecContext(ctx, fmt.Sprintf("CANCEL JOB %s", jobID)); err != nil {
					o.Errorf("failed to cancel job %s: %v", jobID, err)
				}
			}
		}
	}

	// Best-effort resume in case the job is still paused.
	if cl.jobID != 0 {
		o.Status(fmt.Sprintf("cleanup: resuming job %d (best-effort)", cl.jobID))
		_, _ = conn.ExecContext(ctx, fmt.Sprintf("RESUME JOB %d", cl.jobID))
	}

	o.Status(fmt.Sprintf("cleanup: dropping database %s", cl.restoreDBName))
	_, err := conn.ExecContext(ctx,
		fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", cl.restoreDBName))
	if err != nil {
		o.Errorf("failed to drop database %s: %v", cl.restoreDBName, err)
	}

	// Delete backup data from cloud storage.
	if cl.bucket != "" {
		cleanBucket := strings.SplitN(cl.bucket, "?", 2)[0]
		var rmCmd string
		switch {
		case strings.HasPrefix(cleanBucket, "gs://"):
			rmCmd = fmt.Sprintf("gsutil -m rm -r %s", cleanBucket)
		case strings.HasPrefix(cleanBucket, "s3://"):
			rmCmd = fmt.Sprintf("aws s3 rm --recursive %s", cleanBucket)
		}
		if rmCmd != "" {
			o.Status(fmt.Sprintf("cleanup: deleting backup data at %s", cleanBucket))
			if err := c.RunE(ctx, option.WithNodes(c.Node(cl.peerNodeID)), rmCmd); err != nil {
				o.Errorf("failed to delete backup data: %v", err)
			}
		}
	}

	if cl.node == nil {
		return
	}

	if helpers.CheckNodeHealth(ctx, o, c, cl.node[0], 5, 5*time.Second) {
		o.Status(fmt.Sprintf("cleanup: node %d healthy", cl.node[0]))
		return
	}

	o.Status(fmt.Sprintf("cleanup: node %d unresponsive, restarting", cl.node[0]))
	if err := c.RunE(ctx, option.WithNodes(cl.node), "./cockroach.sh"); err != nil {
		o.Errorf("failed to restart node %d: %v", cl.node[0], err)
		return
	}
	if !helpers.CheckNodeHealth(ctx, o, c, cl.node[0], 5, 10*time.Second) {
		o.Errorf("node %d still unresponsive after restart", cl.node[0])
	}
}

// findBackupDatabase finds a suitable database to back up from the
// cct_tpcc/tpcc whitelist (same pattern as backup_restore.go).
func findBackupDatabase(ctx context.Context, o operation.Operation, c cluster.Cluster) string {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	dbWhitelist := []string{"cct_tpcc", "tpcc"}
	dbs, err := conn.QueryContext(ctx, "SELECT database_name FROM [SHOW DATABASES]")
	if err != nil {
		o.Fatal(err)
	}
	defer dbs.Close()

	for dbs.Next() {
		var dbStr string
		if err := dbs.Scan(&dbStr); err != nil {
			o.Fatal(err)
		}
		for _, w := range dbWhitelist {
			if w == dbStr {
				return dbStr
			}
		}
	}
	return ""
}

// backupTables runs a single full BACKUP of the small TPCC tables to
// cloud storage and returns the bucket URL.
func backupTables(
	ctx context.Context, o operation.Operation, c cluster.Cluster, dbName string,
) string {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	var scheme string
	switch c.Cloud() {
	case spec.AWS:
		scheme = "s3"
	case spec.Azure:
		scheme = "azure"
	case spec.GCE:
		scheme = "gs"
	default:
		scheme = ""
	}
	bucket := fmt.Sprintf(
		"%s://%s/disk-fill-bulk-ingest/%d/?AUTH=implicit",
		scheme, testutils.BackupTestingBucket(), timeutil.Now().UnixNano(),
	)

	qualifiedTables := make([]string, len(diskFillBackupTables))
	for i, t := range diskFillBackupTables {
		qualifiedTables[i] = fmt.Sprintf("%s.%s", dbName, t)
	}
	tableList := strings.Join(qualifiedTables, ", ")

	backupTS := hlc.Timestamp{WallTime: timeutil.Now().Add(-10 * time.Second).UTC().UnixNano()}
	o.Status(fmt.Sprintf("backing up tables %s", tableList))
	_, err := conn.ExecContext(ctx, fmt.Sprintf(
		"BACKUP TABLE %s INTO '%s' AS OF SYSTEM TIME '%s'",
		tableList, bucket, backupTS.AsOfSystemTime(),
	))
	if err != nil {
		o.Fatal(err)
	}
	o.Status("backup complete")
	return bucket
}

// startDetachedRestore creates the target database and starts a
// table-level RESTORE in detached mode, returning the job ID.
func startDetachedRestore(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	peerNodeID int,
	dbName string,
	bucket string,
	restoreDBName string,
) jobspb.JobID {
	conn := c.Conn(ctx, o.L(), peerNodeID, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", restoreDBName))
	if err != nil {
		o.Fatal(err)
	}

	qualifiedTables := make([]string, len(diskFillBackupTables))
	for i, t := range diskFillBackupTables {
		qualifiedTables[i] = fmt.Sprintf("%s.%s", dbName, t)
	}
	tableList := strings.Join(qualifiedTables, ", ")

	var jobID int64
	o.Status(fmt.Sprintf("starting detached RESTORE into %s", restoreDBName))
	err = conn.QueryRowContext(ctx, fmt.Sprintf(
		"RESTORE TABLE %s FROM LATEST IN '%s' WITH into_db = '%s', skip_missing_foreign_keys, detached",
		tableList, bucket, restoreDBName,
	)).Scan(&jobID)
	if err != nil {
		o.Fatal(err)
	}
	o.Status(fmt.Sprintf("RESTORE job %d started", jobID))
	return jobspb.JobID(jobID)
}

func runDiskFillBulkIngest(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) (cleanup registry.OperationCleanup) {
	// Phase 1: Find a database and back it up on the healthy cluster.
	dbName := findBackupDatabase(ctx, o, c)
	if dbName == "" {
		o.Status("skipping: no suitable database (cct_tpcc/tpcc) found")
		return nil
	}
	bucket := backupTables(ctx, o, c, dbName)

	rng, _ := randutil.NewPseudoRand()
	restoreDBName := fmt.Sprintf("disk_fill_restore_%d", rng.Int63())

	// Assign cleanup immediately after backup so that cloud storage
	// data, jobs, and the restore database are cleaned up on all exit
	// paths — including early returns below.
	cl := &cleanupDiskFillBulkIngest{
		restoreDBName: restoreDBName,
		bucket:        bucket,
		peerNodeID:    1,
	}
	cleanup = cl

	// Phase 2: Fill disk on a random node.
	liveNodes := helpers.GetLiveNodes(ctx, o, c)
	if len(liveNodes) < 2 {
		o.Status(fmt.Sprintf(
			"skipping: need at least 2 live nodes, found %d", len(liveNodes),
		))
		return cleanup
	}

	targetNode := liveNodes.SeededRandNode(rng)

	for _, n := range liveNodes {
		if n != targetNode[0] {
			cl.peerNodeID = n
			break
		}
	}

	db, err := c.ConnE(ctx, o.L(), targetNode[0])
	if err != nil {
		o.Fatalf("failed to connect to target node %d: %v", targetNode[0], err)
	}
	storeCount := helpers.GetStoreCount(ctx, o, db, targetNode[0])
	db.Close()

	cl.node = targetNode
	cl.storeCount = storeCount

	o.Status(fmt.Sprintf("filling disk on node %d (%d stores)", targetNode[0], storeCount))
	if !fillStores(ctx, o, c, targetNode, storeCount) {
		o.Status("no stores filled — cleaning up")
		removeFillFiles(ctx, o, c, targetNode, storeCount)
		return cleanup
	}

	// Phase 3: Start RESTORE under disk pressure.
	jobID := startDetachedRestore(ctx, o, c, cl.peerNodeID, dbName, bucket, restoreDBName)
	cl.jobID = jobID

	// Phase 4: Wait for job to start running, let it run under
	// pressure, then explicitly pause.
	conn := c.Conn(ctx, o.L(), cl.peerNodeID, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status("waiting for RESTORE job to reach running state")
	if err := tests.WaitForRunning(ctx, conn, jobID, 5*time.Minute); err != nil {
		o.Fatalf("RESTORE job did not start running: %v", err)
	}

	o.Status(fmt.Sprintf(
		"job running under disk pressure, waiting %s before pausing", pressureDuration,
	))
	select {
	case <-time.After(pressureDuration):
	case <-ctx.Done():
		return cleanup
	}

	// Check if the job already finished before we try to pause it.
	var jobStatus string
	err = conn.QueryRowContext(ctx,
		`SELECT status FROM [SHOW JOB $1]`, jobID).Scan(&jobStatus)
	if err != nil {
		o.Fatalf("failed to check job status: %v", err)
	}

	switch jobStatus {
	case "succeeded":
		o.Status(fmt.Sprintf(
			"WARNING: RESTORE job %d finished before pause; pause/resume recovery path was not exercised",
			jobID,
		))
	case "failed", "canceled", "revert-failed":
		o.Fatalf("RESTORE job %d entered terminal state %q under disk pressure", jobID, jobStatus)
	default:
		o.Status(fmt.Sprintf("pausing RESTORE job %d (status: %s)", jobID, jobStatus))
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"PAUSE JOB %d WITH REASON = 'disk-fill/bulk-ingest-recovery: simulating operator pause under disk pressure'",
			jobID,
		))
		if err != nil {
			o.Fatalf("failed to pause job %d: %v", jobID, err)
		}

		o.Status("waiting for job to reach paused state")
		if err := tests.WaitForPaused(ctx, conn, jobID, 5*time.Minute); err != nil {
			o.Fatalf("job did not reach paused state: %v", err)
		}
		o.Status("RESTORE job paused")

		// Phase 5: Remove ballast (simulate disk resize).
		o.Status("removing ballast files (simulating disk resize)")
		removeFillFiles(ctx, o, c, targetNode, storeCount)

		o.Status(fmt.Sprintf("waiting %s for node to stabilize", stabilizeWait))
		select {
		case <-time.After(stabilizeWait):
		case <-ctx.Done():
			return cleanup
		}

		// Phase 6: Resume and verify.
		o.Status(fmt.Sprintf("resuming RESTORE job %d", jobID))
		_, err = conn.ExecContext(ctx, fmt.Sprintf("RESUME JOB %d", jobID))
		if err != nil {
			o.Fatalf("failed to resume job %d: %v", jobID, err)
		}

		o.Status("waiting for RESTORE job to transition to running")
		if err := tests.WaitForResume(ctx, conn, jobID, 5*time.Minute); err != nil {
			o.Fatalf("RESTORE job did not resume: %v", err)
		}

		o.Status("waiting for RESTORE job to succeed")
		if err := tests.WaitForSucceeded(ctx, conn, jobID, jobResumeTimeout); err != nil {
			o.Fatalf("RESTORE job did not succeed after resume: %v", err)
		}
		o.Status("RESTORE job completed successfully after disk resize and resume")
	}

	return cleanup
}

func registerDiskFillBulkIngest(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "disk-fill/bulk-ingest-recovery",
		Owner:              registry.OwnerTestEng,
		Timeout:            90 * time.Minute,
		CompatibleClouds:   registry.AllExceptAzure,
		CanRunConcurrently: registry.OperationCannotRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runDiskFillBulkIngest,
	})
}
