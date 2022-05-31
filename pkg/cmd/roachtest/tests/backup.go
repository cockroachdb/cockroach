// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	cloudstorage "github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/cloud/gcp"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// The following env variable names match those specified in the TeamCity
// configuration for the nightly roachtests. Any changes must be made to both
// references of the name.
const (
	KMSRegionAEnvVar  = "AWS_KMS_REGION_A"
	KMSRegionBEnvVar  = "AWS_KMS_REGION_B"
	KMSKeyARNAEnvVar  = "AWS_KMS_KEY_ARN_A"
	KMSKeyARNBEnvVar  = "AWS_KMS_KEY_ARN_B"
	KMSKeyNameAEnvVar = "GOOGLE_KMS_KEY_A"
	KMSKeyNameBEnvVar = "GOOGLE_KMS_KEY_B"
	KMSGCSCredentials = "GOOGLE_EPHEMERAL_CREDENTIALS"

	// rows2TiB is the number of rows to import to load 2TB of data (when
	// replicated).
	rows2TiB   = 65_104_166
	rows100GiB = rows2TiB / 20
	rows30GiB  = rows2TiB / 66
	rows15GiB  = rows30GiB / 2
	rows5GiB   = rows100GiB / 20
	rows3GiB   = rows30GiB / 10
)

func destinationName(c cluster.Cluster) string {
	dest := c.Name()
	if c.IsLocal() {
		dest += fmt.Sprintf("%d", timeutil.Now().UnixNano())
	}
	return dest
}

func importBankDataSplit(
	ctx context.Context, rows, ranges int, t test.Test, c cluster.Cluster,
) string {
	dest := destinationName(c)

	c.Put(ctx, t.DeprecatedWorkload(), "./workload")
	c.Put(ctx, t.Cockroach(), "./cockroach")

	// NB: starting the cluster creates the logs dir as a side effect,
	// needed below.
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	runImportBankDataSplit(ctx, rows, ranges, t, c)
	return dest
}

func runImportBankDataSplit(ctx context.Context, rows, ranges int, t test.Test, c cluster.Cluster) {
	c.Run(ctx, c.All(), `./workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)
	time.Sleep(time.Second) // wait for csv server to open listener
	importArgs := []string{
		"./workload", "fixtures", "import", "bank",
		"--db=bank",
		"--payload-bytes=10240",
		"--csv-server", "http://localhost:8081",
		"--seed=1",
		fmt.Sprintf("--ranges=%d", ranges),
		fmt.Sprintf("--rows=%d", rows),
		"{pgurl:1}",
	}
	c.Run(ctx, c.Node(1), importArgs...)
}

func importBankData(ctx context.Context, rows int, t test.Test, c cluster.Cluster) string {
	return importBankDataSplit(ctx, rows, 0 /* ranges */, t, c)
}

func registerBackupNodeShutdown(r registry.Registry) {
	// backupNodeRestartSpec runs a backup and randomly shuts down a node during
	// the backup.
	backupNodeRestartSpec := r.MakeClusterSpec(4)
	loadBackupData := func(ctx context.Context, t test.Test, c cluster.Cluster) string {
		// This aught to be enough since this isn't a performance test.
		rows := rows15GiB
		if c.IsLocal() {
			// Needs to be sufficiently large to give each processor a good chunk of
			// works so the job doesn't complete immediately.
			rows = rows5GiB
		}
		return importBankData(ctx, rows, t, c)
	}

	r.Add(registry.TestSpec{
		Name:              fmt.Sprintf("backup/nodeShutdown/worker/%s", backupNodeRestartSpec),
		Owner:             registry.OwnerBulkIO,
		Cluster:           backupNodeRestartSpec,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := 2
			nodeToShutdown := 3
			dest := loadBackupData(ctx, t, c)
			backupQuery := `BACKUP bank.bank TO 'nodelocal://1/` + dest + `' WITH DETACHED`
			startBackup := func(c cluster.Cluster, t test.Test) (jobID string, err error) {
				gatewayDB := c.Conn(ctx, t.L(), gatewayNode)
				defer gatewayDB.Close()

				err = gatewayDB.QueryRowContext(ctx, backupQuery).Scan(&jobID)
				return
			}

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, startBackup)
		},
	})
	r.Add(registry.TestSpec{
		Name:              fmt.Sprintf("backup/nodeShutdown/coordinator/%s", backupNodeRestartSpec),
		Owner:             registry.OwnerBulkIO,
		Cluster:           backupNodeRestartSpec,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := 2
			nodeToShutdown := 2
			dest := loadBackupData(ctx, t, c)
			backupQuery := `BACKUP bank.bank TO 'nodelocal://1/` + dest + `' WITH DETACHED`
			startBackup := func(c cluster.Cluster, t test.Test) (jobID string, err error) {
				gatewayDB := c.Conn(ctx, t.L(), gatewayNode)
				defer gatewayDB.Close()

				err = gatewayDB.QueryRowContext(ctx, backupQuery).Scan(&jobID)
				return
			}

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, startBackup)
		},
	})

}

// removeJobClaimsForNodes nullifies the `claim_session_id` for the job
// corresponding to jobID, if it has been incorrectly claimed by one of the
// `nodes`.
func removeJobClaimsForNodes(
	ctx context.Context, t test.Test, db *gosql.DB, nodes option.NodeListOption, jobID jobspb.JobID,
) {
	if len(nodes) == 0 {
		return
	}

	n := make([]string, 0)
	for _, node := range nodes {
		n = append(n, strconv.Itoa(node))
	}
	nodesStr := strings.Join(n, ",")

	removeClaimQuery := `
UPDATE system.jobs
   SET claim_session_id = NULL
WHERE claim_instance_id IN (%s)
AND id = $1
`
	_, err := db.ExecContext(ctx, fmt.Sprintf(removeClaimQuery, nodesStr), jobID)
	require.NoError(t, err)
}

// waitForJobToHaveStatus waits for the job with jobID to reach the
// expectedStatus.
func waitForJobToHaveStatus(
	ctx context.Context,
	t test.Test,
	db *gosql.DB,
	jobID jobspb.JobID,
	expectedStatus jobs.Status,
	nodesWithAdoptionDisabled option.NodeListOption,
) {
	if err := retry.ForDuration(time.Minute*1, func() error {
		// TODO(adityamaru): This is unfortunate and can be deleted once
		// https://github.com/cockroachdb/cockroach/pull/79666 is backported to
		// 21.2 and the mixed version map for roachtests is bumped to the 21.2
		// patch release with the backport.
		//
		// The bug above means that nodes for which we have disabled adoption may
		// still lay claim on the job, and then not clear their claim on realizing
		// that adoption is disabled. To prevent the job from getting wedged, we
		// manually clear the claim session on the job for instances where job
		// adoption is disabled. This will allow other node's in the cluster to
		// adopt the job and run it.
		removeJobClaimsForNodes(ctx, t, db, nodesWithAdoptionDisabled, jobID)

		var status string
		var payloadBytes []byte
		err := db.QueryRow(`SELECT status, payload FROM system.jobs WHERE id = $1`, jobID).Scan(&status, &payloadBytes)
		require.NoError(t, err)
		if jobs.Status(status) == jobs.StatusFailed {
			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				t.Fatalf("job failed: %s", payload.Error)
			}
			t.Fatalf("job failed")
		}
		if e, a := expectedStatus, jobs.Status(status); e != a {
			return errors.Errorf("expected job status %s, but got %s", e, a)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// fingerprint returns a fingerprint of `db.table`.
func fingerprint(ctx context.Context, conn *gosql.DB, db, table string) (string, error) {
	var b strings.Builder

	query := fmt.Sprintf("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s.%s", db, table)
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var name, fp string
		if err := rows.Scan(&name, &fp); err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "%s: %s\n", name, fp)
	}

	return b.String(), rows.Err()
}

func registerBackupMixedVersion(r registry.Registry) {
	// setShortJobIntervalsStep increases the frequency of the adopt and cancel
	// loops in the job registry. This enables changes to job state to be observed
	// faster, and the test to run quicker.
	setShortJobIntervalsStep := func(node int) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			db := u.conn(ctx, t, node)
			_, err := db.ExecContext(ctx, `SET CLUSTER SETTING jobs.registry.interval.cancel = '1s'`)
			if err != nil {
				t.Fatal(err)
			}

			_, err = db.ExecContext(ctx, `SET CLUSTER SETTING jobs.registry.interval.adopt = '1s'`)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	loadBackupDataStep := func(c cluster.Cluster) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			rows := rows3GiB
			if c.IsLocal() {
				rows = 100
			}
			runImportBankDataSplit(ctx, rows, 0 /* ranges */, t, u.c)
		}
	}

	// disableJobAdoptionStep writes the sentinel file to prevent a node's
	// registry from adopting a job.
	disableJobAdoptionStep := func(c cluster.Cluster, nodeIDs option.NodeListOption) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			for _, nodeID := range nodeIDs {
				result, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(nodeID), "echo", "-n", "{store-dir}")
				if err != nil {
					t.L().Printf("Failed to retrieve store directory from node %d: %v\n", nodeID, err.Error())
				}
				storeDirectory := result.Stdout
				disableJobAdoptionSentinelFilePath := filepath.Join(storeDirectory, jobs.PreventAdoptionFile)
				c.Run(ctx, nodeIDs, fmt.Sprintf("touch %s", disableJobAdoptionSentinelFilePath))

				// Wait for no jobs to be running on the node that we have halted
				// adoption on.
				testutils.SucceedsSoon(t, func() error {
					gatewayDB := c.Conn(ctx, t.L(), nodeID)
					defer gatewayDB.Close()

					row := gatewayDB.QueryRow(`SELECT count(*) FROM [SHOW JOBS] WHERE status = 'running'`)
					var count int
					require.NoError(t, row.Scan(&count))
					if count != 0 {
						return errors.Newf("node is still running %d jobs", count)
					}
					return nil
				})
			}

			// TODO(adityamaru): This is unfortunate and can be deleted once
			// https://github.com/cockroachdb/cockroach/pull/79666 is backported to
			// 21.2 and the mixed version map for roachtests is bumped to the 21.2
			// patch release with the backport.
			//
			// The bug above means that nodes for which we have disabled adoption may
			// still lay claim on the job, and then not clear their claim on realizing
			// that adoption is disabled. To get around this we set the env variable
			// to disable the registries from even laying claim on the jobs.
			_, err := c.RunWithDetails(ctx, t.L(), nodeIDs, "export COCKROACH_JOB_ADOPTIONS_PER_PERIOD=0")
			require.NoError(t, err)
		}
	}

	// enableJobAdoptionStep clears the sentinel file that prevents a node's
	// registry from adopting a job.
	enableJobAdoptionStep := func(c cluster.Cluster,
		nodeIDs option.NodeListOption) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			for _, nodeID := range nodeIDs {
				result, err := c.RunWithDetailsSingleNode(ctx, t.L(),
					c.Node(nodeID), "echo", "-n", "{store-dir}")
				if err != nil {
					t.L().Printf("Failed to retrieve store directory from node %d: %v\n", nodeID, err.Error())
				}
				storeDirectory := result.Stdout
				disableJobAdoptionSentinelFilePath := filepath.Join(storeDirectory, jobs.PreventAdoptionFile)
				c.Run(ctx, nodeIDs, fmt.Sprintf("rm -f %s", disableJobAdoptionSentinelFilePath))
			}

			// Reset the env variable that controls how many jobs are claimed by the
			// registry.
			_, err := c.RunWithDetails(ctx, t.L(), nodeIDs, "export COCKROACH_JOB_ADOPTIONS_PER_PERIOD=10")
			require.NoError(t, err)
		}
	}

	planAndRunBackup := func(t test.Test, c cluster.Cluster, nodeToPlanBackup option.NodeListOption,
		nodesWithAdoptionDisabled option.NodeListOption, backupStmt string) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			gatewayDB := c.Conn(ctx, t.L(), nodeToPlanBackup[0])
			defer gatewayDB.Close()
			t.Status("Running: ", backupStmt)
			var jobID jobspb.JobID
			err := gatewayDB.QueryRow(backupStmt).Scan(&jobID)
			require.NoError(t, err)
			waitForJobToHaveStatus(ctx, t, gatewayDB, jobID, jobs.StatusSucceeded, nodesWithAdoptionDisabled)
		}
	}

	writeToBankStep := func(node int) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			db := u.conn(ctx, t, node)
			_, err := db.ExecContext(ctx, `UPSERT INTO bank.bank (id,balance) SELECT generate_series(1,100), random()*100;`)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// saveFingerprintStep computes the fingerprint of the `bank.bank` and adds it
	// to `fingerprints` slice that is passed in. The fingerprints slice should be
	// pre-allocated because of the structure of the versionUpgradeTest.
	saveFingerprintStep := func(node int, fingerprints map[string]string, key string) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			db := u.conn(ctx, t, node)
			f, err := fingerprint(ctx, db, "bank", "bank")
			if err != nil {
				t.Fatal(err)
			}
			fingerprints[key] = f
		}
	}

	// verifyBackupStep compares the backed up and restored table fingerprints and
	// ensures they're the same.
	verifyBackupStep := func(
		node option.NodeListOption,
		backupLoc string,
		dbName, tableName, intoDB string,
		fingerprints map[string]string,
	) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			db := u.conn(ctx, t, node[0])

			// Restore the backup.
			_, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE %s`, intoDB))
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, fmt.Sprintf(`RESTORE TABLE %s.%s FROM LATEST IN '%s' WITH into_db = '%s'`,
				dbName, tableName, backupLoc, intoDB))
			require.NoError(t, err)

			restoredFingerPrint, err := fingerprint(ctx, db, intoDB, tableName)
			require.NoError(t, err)
			if fingerprints[backupLoc] != restoredFingerPrint {
				log.Infof(ctx, "original %s \n\n restored %s", fingerprints[backupLoc],
					restoredFingerPrint)
				t.Fatal("expected backup and restore fingerprints to match")
			}
		}
	}

	// backup/mixed-version-basic tests different states of backup in a mixed
	// version cluster.
	//
	// This test can serve as a template for more targeted testing of features
	// that require careful consideration of mixed version states.
	r.Add(registry.TestSpec{
		Name:              "backup/mixed-version-basic",
		Owner:             registry.OwnerBulkIO,
		Cluster:           r.MakeClusterSpec(4),
		EncryptionSupport: registry.EncryptionMetamorphic,
		RequiresLicense:   true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// An empty string means that the cockroach binary specified by flag
			// `cockroach` will be used.
			const mainVersion = ""
			roachNodes := c.All()
			upgradedNodes := c.Nodes(1, 2)
			oldNodes := c.Nodes(3, 4)
			predV, err := PredecessorVersion(*t.BuildVersion())
			require.NoError(t, err)
			c.Put(ctx, t.DeprecatedWorkload(), "./workload")

			// fingerprints stores the fingerprint of the `bank.bank` table at
			// different points of this test to compare against the restored table at
			// the end of the test.
			fingerprints := make(map[string]string)
			u := newVersionUpgradeTest(c,
				uploadAndStartFromCheckpointFixture(roachNodes, predV),
				waitForUpgradeStep(roachNodes),
				preventAutoUpgradeStep(1),
				setShortJobIntervalsStep(1),
				loadBackupDataStep(c),
				// Upgrade some nodes.
				binaryUpgradeStep(upgradedNodes, mainVersion),

				// Let us first test planning and executing a backup on different node
				// versions.
				//
				// NB: All backups in this test are writing to node 1's ExternalIODir
				// for simplicity.

				// Case 1: plan backup    -> old node
				//         execute backup -> upgraded node
				//
				// Halt job execution on older nodes.
				disableJobAdoptionStep(c, oldNodes),

				// Run a backup from an old node so that it is planned on the old node
				// but the job is adopted on a new node.
				planAndRunBackup(t, c, oldNodes.RandNode(), oldNodes,
					`BACKUP TABLE bank.bank INTO 'nodelocal://1/plan-old-resume-new' WITH detached`),

				// Write some data between backups.
				writeToBankStep(1),

				// Run an incremental backup from an old node so that it is planned on
				// the old node but the job is adopted on a new node.
				planAndRunBackup(t, c, oldNodes.RandNode(), oldNodes,
					`BACKUP TABLE bank.bank INTO LATEST IN 'nodelocal://1/plan-old-resume-new' WITH detached`),

				// Save fingerprint to compare against restore below.
				saveFingerprintStep(1, fingerprints, "nodelocal://1/plan-old-resume-new"),

				enableJobAdoptionStep(c, oldNodes),

				// Case 2: plan backup    -> upgraded node
				//         execute backup -> old node
				//
				// Halt job execution on upgraded nodes.
				disableJobAdoptionStep(c, upgradedNodes),

				// Run a backup from a new node so that it is planned on the new node
				// but the job is adopted on an old node.
				planAndRunBackup(t, c, upgradedNodes.RandNode(), upgradedNodes,
					`BACKUP TABLE bank.bank INTO 'nodelocal://1/plan-new-resume-old' WITH detached`),

				writeToBankStep(1),

				// Run an incremental backup from a new node so that it is planned on
				// the new node but the job is adopted on an old node.
				planAndRunBackup(t, c, upgradedNodes.RandNode(), upgradedNodes,
					`BACKUP TABLE bank.bank INTO LATEST IN 'nodelocal://1/plan-new-resume-old' WITH detached`),

				// Save fingerprint to compare against restore below.
				saveFingerprintStep(1, fingerprints, "nodelocal://1/plan-new-resume-old"),

				enableJobAdoptionStep(c, upgradedNodes),

				// Now let us test building an incremental chain on top of a full backup
				// created by a node of a different version.
				//
				// Case 1: full backup -> new nodes
				//         inc backup  -> old nodes
				disableJobAdoptionStep(c, oldNodes),
				// Plan and run a full backup on the new nodes.
				planAndRunBackup(t, c, upgradedNodes.RandNode(), oldNodes,
					`BACKUP TABLE bank.bank INTO 'nodelocal://1/new-node-full-backup' WITH detached`),

				writeToBankStep(1),

				// Set up the cluster so that only the old nodes plan and run the
				// incremental backup.
				enableJobAdoptionStep(c, oldNodes),
				disableJobAdoptionStep(c, upgradedNodes),

				// Run an incremental (on old nodes) on top of a full backup taken by
				// nodes on the upgraded version.
				planAndRunBackup(t, c, oldNodes.RandNode(), upgradedNodes,
					`BACKUP TABLE bank.bank INTO LATEST IN 'nodelocal://1/new-node-full-backup' WITH detached`),

				// Save fingerprint to compare against restore below.
				saveFingerprintStep(1, fingerprints, "nodelocal://1/new-node-full-backup"),

				// Case 2: full backup -> old nodes
				//         inc backup  -> new nodes

				// Plan and run a full backup on the old nodes.
				planAndRunBackup(t, c, oldNodes.RandNode(), upgradedNodes,
					`BACKUP TABLE bank.bank INTO 'nodelocal://1/old-node-full-backup' WITH detached`),

				writeToBankStep(1),

				enableJobAdoptionStep(c, upgradedNodes),

				// Allow all the nodes to now finalize their cluster version.
				binaryUpgradeStep(oldNodes, mainVersion),
				allowAutoUpgradeStep(1),
				waitForUpgradeStep(roachNodes),

				// Run an incremental on top of a full backup taken by nodes on the
				// old version.
				planAndRunBackup(t, c, roachNodes.RandNode(), nil,
					`BACKUP TABLE bank.bank INTO LATEST IN 'nodelocal://1/old-node-full-backup' WITH detached`),

				// Save fingerprint to compare against restore below.
				saveFingerprintStep(1, fingerprints, "nodelocal://1/old-node-full-backup"),

				// Verify all the backups are actually restoreable.
				verifyBackupStep(roachNodes.RandNode(), "nodelocal://1/plan-old-resume-new",
					"bank", "bank", "bank1", fingerprints),
				verifyBackupStep(roachNodes.RandNode(), "nodelocal://1/plan-new-resume-old",
					"bank", "bank", "bank2", fingerprints),
				verifyBackupStep(roachNodes.RandNode(), "nodelocal://1/new-node-full-backup",
					"bank", "bank", "bank3", fingerprints),
				verifyBackupStep(roachNodes.RandNode(), "nodelocal://1/old-node-full-backup",
					"bank", "bank", "bank4", fingerprints),
			)
			u.run(ctx, t)
		},
	})

	// TODO(adityamaru): Delete in 22.2 since nodes will no longer rely on
	// BACKUP-CHECKPOINT to lock their location.
	r.Add(registry.TestSpec{
		Name:              "backup/mixed-version-concurrent-backups",
		Owner:             registry.OwnerBulkIO,
		Cluster:           r.MakeClusterSpec(4),
		EncryptionSupport: registry.EncryptionMetamorphic,
		RequiresLicense:   true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// An empty string means that the cockroach binary specified by flag
			// `cockroach` will be used.
			const mainVersion = ""
			roachNodes := c.All()
			upgradedNodes := c.Nodes(1, 2)
			oldNodes := c.Nodes(3, 4)
			predV, err := PredecessorVersion(*t.BuildVersion())
			require.NoError(t, err)
			c.Put(ctx, t.DeprecatedWorkload(), "./workload")

			u := newVersionUpgradeTest(c,
				uploadAndStartFromCheckpointFixture(roachNodes, predV),
				waitForUpgradeStep(roachNodes),
				preventAutoUpgradeStep(1),
				setShortJobIntervalsStep(1),
				loadBackupDataStep(c),
				// Upgrade a node.
				binaryUpgradeStep(upgradedNodes, mainVersion),
				disableJobAdoptionStep(c, oldNodes),

				// Plan and run a backup on an upgraded node.
				//
				// Since the cluster is in a mixed-version state, the backup will lock
				// its bucket, and perform destination resolution during planning.
				func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
					t.L().Printf("running and pausing BACKUP on upgraded node")
					gatewayDB := c.Conn(ctx, t.L(), upgradedNodes[0])
					defer gatewayDB.Close()
					_, err := gatewayDB.Exec(`SET CLUSTER SETTING jobs.debug.pausepoints = 'backup.before.flow'`)
					require.NoError(t, err)
					var jobID jobspb.JobID
					err = gatewayDB.QueryRow(`BACKUP TABLE bank.bank TO 'userfile:///upgraded-node-checkpoint' WITH detached `).Scan(&jobID)
					require.NoError(t, err)
					waitForJobToHaveStatus(ctx, t, gatewayDB, jobID, jobs.StatusPaused, oldNodes)
					_, err = gatewayDB.Exec(`SET CLUSTER SETTING jobs.debug.pausepoints = ''`)
					require.NoError(t, err)
				},

				// Now, run a backup to the same location but from a 21.2 node. This
				// backup should fail because it sees a BACKUP-CHECKPOINT written by the
				// previous backup job.
				enableJobAdoptionStep(c, oldNodes),
				func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
					t.L().Printf("running BACKUP to the same location on old node; waiting for failure")
					gatewayDB := c.Conn(ctx, t.L(), oldNodes[0])
					defer gatewayDB.Close()
					var jobID jobspb.JobID
					err := gatewayDB.QueryRow(`BACKUP TABLE bank.bank TO 'userfile:///upgraded-node-checkpoint' WITH detached `).Scan(&jobID)
					testutils.IsError(err, "userfile:///upgraded-node-checkpoint already contains a BACKUP-CHECKPOINT file (is another operation already in progress?)")
				},
				disableJobAdoptionStep(c, oldNodes),
				// Resume the paused backup job and get an upgraded node to complete it.
				func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
					t.L().Printf("resuming BACKUP on upgraded node; waiting for success")
					gatewayDB := c.Conn(ctx, t.L(), upgradedNodes[0])
					defer gatewayDB.Close()
					var jobID jobspb.JobID
					err = gatewayDB.QueryRow(`SELECT job_id FROM [SHOW JOBS] WHERE status = 'paused' AND job_type = 'BACKUP'`).Scan(&jobID)
					require.NoError(t, err)
					_, err = gatewayDB.ExecContext(ctx, `RESUME JOB $1`, jobID)
					require.NoError(t, err)
					waitForJobToHaveStatus(ctx, t, gatewayDB, jobID, jobs.StatusSucceeded, oldNodes)
				},
			)

			u.run(ctx, t)
		},
	})
}

// initBulkJobPerfArtifacts registers a histogram, creates a performance
// artifact directory and returns a method that when invoked records a tick.
func initBulkJobPerfArtifacts(testName string, timeout time.Duration) (func(), *bytes.Buffer) {
	// Register a named histogram to track the total time the bulk job took.
	// Roachperf uses this information to display information about this
	// roachtest.
	reg := histogram.NewRegistry(
		timeout,
		histogram.MockWorkloadName,
	)
	reg.GetHandle().Get(testName)

	bytesBuf := bytes.NewBuffer([]byte{})
	jsonEnc := json.NewEncoder(bytesBuf)
	tick := func() {
		reg.Tick(func(tick histogram.Tick) {
			_ = jsonEnc.Encode(tick.Snapshot())
		})
	}

	return tick, bytesBuf
}

func registerBackup(r registry.Registry) {

	backup2TBSpec := r.MakeClusterSpec(10)
	r.Add(registry.TestSpec{
		Name:              fmt.Sprintf("backup/2TB/%s", backup2TBSpec),
		Owner:             registry.OwnerBulkIO,
		Cluster:           backup2TBSpec,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			rows := rows2TiB
			if c.IsLocal() {
				rows = 100
			}
			dest := importBankData(ctx, rows, t, c)
			tick, perfBuf := initBulkJobPerfArtifacts("backup/2TB", 2*time.Hour)

			m := c.NewMonitor(ctx)
			m.Go(func(ctx context.Context) error {
				t.Status(`running backup`)
				// Tick once before starting the backup, and once after to capture the
				// total elapsed time. This is used by roachperf to compute and display
				// the average MB/sec per node.
				tick()
				c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "
				BACKUP bank.bank TO 'gs://cockroachdb-backup-testing/`+dest+`?AUTH=implicit'"`)
				tick()

				// Upload the perf artifacts to any one of the nodes so that the test
				// runner copies it into an appropriate directory path.
				dest := filepath.Join(t.PerfArtifactsDir(), "stats.json")
				if err := c.RunE(ctx, c.Node(1), "mkdir -p "+filepath.Dir(dest)); err != nil {
					log.Errorf(ctx, "failed to create perf dir: %+v", err)
				}
				if err := c.PutString(ctx, perfBuf.String(), dest, 0755, c.Node(1)); err != nil {
					log.Errorf(ctx, "failed to upload perf artifacts to node: %s", err.Error())
				}
				return nil
			})
			m.Wait()
		},
	})

	KMSSpec := r.MakeClusterSpec(3)
	for _, item := range []struct {
		kmsProvider string
		machine     string
	}{
		{kmsProvider: "GCS", machine: spec.GCE},
		{kmsProvider: "AWS", machine: spec.AWS},
	} {
		item := item
		r.Add(registry.TestSpec{
			Name:              fmt.Sprintf("backup/KMS/%s/%s", item.kmsProvider, KMSSpec.String()),
			Owner:             registry.OwnerBulkIO,
			Cluster:           KMSSpec,
			EncryptionSupport: registry.EncryptionMetamorphic,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.Spec().Cloud != item.machine {
					t.Skip("backupKMS roachtest is only configured to run on "+item.machine, "")
				}

				// ~10GiB - which is 30Gib replicated.
				rows := rows30GiB
				if c.IsLocal() {
					rows = 100
				}
				dest := importBankData(ctx, rows, t, c)

				conn := c.Conn(ctx, t.L(), 1)
				m := c.NewMonitor(ctx)
				m.Go(func(ctx context.Context) error {
					_, err := conn.ExecContext(ctx, `
					CREATE DATABASE restoreA;
					CREATE DATABASE restoreB;
				`)
					return err
				})
				m.Wait()
				var kmsURIA, kmsURIB string
				var err error
				backupPath := fmt.Sprintf("nodelocal://1/kmsbackup/%s/%s", item.kmsProvider, dest)

				m = c.NewMonitor(ctx)
				m.Go(func(ctx context.Context) error {
					switch item.kmsProvider {
					case "AWS":
						t.Status(`running encrypted backup with AWS KMS`)
						kmsURIA, err = getAWSKMSURI(KMSRegionAEnvVar, KMSKeyARNAEnvVar)
						if err != nil {
							return err
						}

						kmsURIB, err = getAWSKMSURI(KMSRegionBEnvVar, KMSKeyARNBEnvVar)
						if err != nil {
							return err
						}
					case "GCS":
						t.Status(`running encrypted backup with GCS KMS`)
						kmsURIA, err = getGCSKMSURI(KMSKeyNameAEnvVar)
						if err != nil {
							return err
						}

						kmsURIB, err = getGCSKMSURI(KMSKeyNameBEnvVar)
						if err != nil {
							return err
						}
					}

					kmsOptions := fmt.Sprintf("KMS=('%s', '%s')", kmsURIA, kmsURIB)
					_, err := conn.ExecContext(ctx, `BACKUP bank.bank TO '`+backupPath+`' WITH `+kmsOptions)
					return err
				})
				m.Wait()

				// Restore the encrypted BACKUP using each of KMS URI A and B separately.
				m = c.NewMonitor(ctx)
				m.Go(func(ctx context.Context) error {
					t.Status(`restore using KMSURIA`)
					if _, err := conn.ExecContext(ctx,
						`RESTORE bank.bank FROM $1 WITH into_db=restoreA, kms=$2`,
						backupPath, kmsURIA,
					); err != nil {
						return err
					}

					t.Status(`restore using KMSURIB`)
					if _, err := conn.ExecContext(ctx,
						`RESTORE bank.bank FROM $1 WITH into_db=restoreB, kms=$2`,
						backupPath, kmsURIB,
					); err != nil {
						return err
					}

					t.Status(`fingerprint`)
					fingerprint := func(db string) (string, error) {
						var b strings.Builder

						query := fmt.Sprintf("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s.%s", db, "bank")
						rows, err := conn.QueryContext(ctx, query)
						if err != nil {
							return "", err
						}
						defer rows.Close()
						for rows.Next() {
							var name, fp string
							if err := rows.Scan(&name, &fp); err != nil {
								return "", err
							}
							fmt.Fprintf(&b, "%s: %s\n", name, fp)
						}

						return b.String(), rows.Err()
					}

					originalBank, err := fingerprint("bank")
					if err != nil {
						return err
					}
					restoreA, err := fingerprint("restoreA")
					if err != nil {
						return err
					}
					restoreB, err := fingerprint("restoreB")
					if err != nil {
						return err
					}

					if originalBank != restoreA {
						return errors.Errorf("got %s, expected %s while comparing restoreA with originalBank", restoreA, originalBank)
					}
					if originalBank != restoreB {
						return errors.Errorf("got %s, expected %s while comparing restoreB with originalBank", restoreB, originalBank)
					}
					return nil
				})
				m.Wait()
			},
		})
	}

	// backupTPCC continuously runs TPCC, takes a full backup after some time,
	// and incremental after more time. It then restores the two backups and
	// verifies them with a fingerprint.
	r.Add(registry.TestSpec{
		Name:              `backupTPCC`,
		Owner:             registry.OwnerBulkIO,
		Cluster:           r.MakeClusterSpec(3),
		Timeout:           1 * time.Hour,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Put(ctx, t.DeprecatedWorkload(), "./workload")
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
			conn := c.Conn(ctx, t.L(), 1)

			duration := 5 * time.Minute
			if c.IsLocal() {
				duration = 5 * time.Second
			}
			warehouses := 10

			backupDir := "gs://cockroachdb-backup-testing/" + c.Name() + "?AUTH=implicit"
			// Use inter-node file sharing on 20.1+.
			if t.BuildVersion().AtLeast(version.MustParse(`v20.1.0-0`)) {
				backupDir = "nodelocal://1/" + c.Name()
			}
			fullDir := backupDir + "/full"
			incDir := backupDir + "/inc"

			t.Status(`workload initialization`)
			cmd := []string{fmt.Sprintf(
				"./workload init tpcc --warehouses=%d {pgurl:1-%d}",
				warehouses, c.Spec().NodeCount,
			)}
			if !t.BuildVersion().AtLeast(version.MustParse("v20.2.0")) {
				cmd = append(cmd, "--deprecated-fk-indexes")
			}
			c.Run(ctx, c.Node(1), cmd...)

			m := c.NewMonitor(ctx)
			m.Go(func(ctx context.Context) error {
				_, err := conn.ExecContext(ctx, `
					CREATE DATABASE restore_full;
					CREATE DATABASE restore_inc;
				`)
				return err
			})
			m.Wait()

			t.Status(`run tpcc`)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			cmdDone := make(chan error)
			go func() {
				cmd := fmt.Sprintf(
					"./workload run tpcc --warehouses=%d {pgurl:1-%d}",
					warehouses, c.Spec().NodeCount,
				)

				cmdDone <- c.RunE(ctx, c.Node(1), cmd)
			}()

			select {
			case <-time.After(duration):
			case <-ctx.Done():
				return
			}

			// Use a time slightly in the past to avoid "cannot specify timestamp in the future" errors.
			tFull := fmt.Sprint(timeutil.Now().Add(time.Second * -2).UnixNano())
			m = c.NewMonitor(ctx)
			m.Go(func(ctx context.Context) error {
				t.Status(`full backup`)
				_, err := conn.ExecContext(ctx,
					`BACKUP tpcc.* TO $1 AS OF SYSTEM TIME `+tFull,
					fullDir,
				)
				return err
			})
			m.Wait()

			t.Status(`continue tpcc`)
			select {
			case <-time.After(duration):
			case <-ctx.Done():
				return
			}

			tInc := fmt.Sprint(timeutil.Now().Add(time.Second * -2).UnixNano())
			m = c.NewMonitor(ctx)
			m.Go(func(ctx context.Context) error {
				t.Status(`incremental backup`)
				_, err := conn.ExecContext(ctx,
					`BACKUP tpcc.* TO $1 AS OF SYSTEM TIME `+tInc+` INCREMENTAL FROM $2`,
					incDir,
					fullDir,
				)
				if err != nil {
					return err
				}

				// Backups are done, make sure workload is still running.
				select {
				case err := <-cmdDone:
					// Workload exited before it should have.
					return err
				default:
					return nil
				}
			})
			m.Wait()

			m = c.NewMonitor(ctx)
			m.Go(func(ctx context.Context) error {
				t.Status(`restore full`)
				if _, err := conn.ExecContext(ctx,
					`RESTORE tpcc.* FROM $1 WITH into_db='restore_full'`,
					fullDir,
				); err != nil {
					return err
				}

				t.Status(`restore incremental`)
				if _, err := conn.ExecContext(ctx,
					`RESTORE tpcc.* FROM $1, $2 WITH into_db='restore_inc'`,
					fullDir,
					incDir,
				); err != nil {
					return err
				}

				t.Status(`fingerprint`)
				// TODO(adityamaru): Pull the fingerprint logic into a utility method
				// which can be shared by multiple roachtests.
				fingerprint := func(db string, asof string) (string, error) {
					var b strings.Builder

					var tables []string
					rows, err := conn.QueryContext(
						ctx,
						fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s] ORDER BY table_name", db),
					)
					if err != nil {
						return "", err
					}
					defer rows.Close()
					for rows.Next() {
						var name string
						if err := rows.Scan(&name); err != nil {
							return "", err
						}
						tables = append(tables, name)
					}

					for _, table := range tables {
						fmt.Fprintf(&b, "table %s\n", table)
						query := fmt.Sprintf("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s.%s", db, table)
						if asof != "" {
							query = fmt.Sprintf("SELECT * FROM [%s] AS OF SYSTEM TIME %s", query, asof)
						}
						rows, err = conn.QueryContext(ctx, query)
						if err != nil {
							return "", err
						}
						defer rows.Close()
						for rows.Next() {
							var name, fp string
							if err := rows.Scan(&name, &fp); err != nil {
								return "", err
							}
							fmt.Fprintf(&b, "%s: %s\n", name, fp)
						}
					}

					return b.String(), rows.Err()
				}

				tpccFull, err := fingerprint("tpcc", tFull)
				if err != nil {
					return err
				}
				tpccInc, err := fingerprint("tpcc", tInc)
				if err != nil {
					return err
				}
				restoreFull, err := fingerprint("restore_full", "")
				if err != nil {
					return err
				}
				restoreInc, err := fingerprint("restore_inc", "")
				if err != nil {
					return err
				}

				if tpccFull != restoreFull {
					return errors.Errorf("got %s, expected %s", restoreFull, tpccFull)
				}
				if tpccInc != restoreInc {
					return errors.Errorf("got %s, expected %s", restoreInc, tpccInc)
				}

				return nil
			})
			m.Wait()
		},
	})

}

func getAWSKMSURI(regionEnvVariable, keyIDEnvVariable string) (string, error) {
	q := make(url.Values)
	expect := map[string]string{
		"AWS_ACCESS_KEY_ID":     amazon.AWSAccessKeyParam,
		"AWS_SECRET_ACCESS_KEY": amazon.AWSSecretParam,
		regionEnvVariable:       amazon.KMSRegionParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			return "", errors.Newf("env variable %s must be present to run the KMS test", env)
		}
		q.Add(param, v)
	}

	// Get AWS Key ARN from env variable.
	keyARN := os.Getenv(keyIDEnvVariable)
	if keyARN == "" {
		return "", errors.Newf("env variable %s must be present to run the KMS test", keyIDEnvVariable)
	}

	// Set AUTH to specified
	q.Add(cloudstorage.AuthParam, cloudstorage.AuthParamSpecified)
	correctURI := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())

	return correctURI, nil
}

func getGCSKMSURI(keyIDEnvVariable string) (string, error) {
	q := make(url.Values)
	expect := map[string]string{
		KMSGCSCredentials: gcp.CredentialsParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			return "", errors.Newf("env variable %s must be present to run the KMS test", env)
		}
		// Nightlies load in json file of credentials but we want base64 encoded
		q.Add(param, base64.StdEncoding.EncodeToString([]byte(v)))
	}

	keyID := os.Getenv(keyIDEnvVariable)
	if keyID == "" {
		return "", errors.Newf("", "%s env var must be set", keyIDEnvVariable)
	}

	// Set AUTH to specified
	q.Set(cloudstorage.AuthParam, cloudstorage.AuthParamSpecified)
	correctURI := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())

	return correctURI, nil
}
