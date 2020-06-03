// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func runBackupMixedVersions(
	ctx context.Context, t *test, c *cluster, warehouses int, predecessorVersion string,
) {
	type backup struct {
		location string
		time     string
	}
	const backupPrefix = "nodelocal://1/mixed-version-backups/"
	takenBackups := make([]backup, 0)

	tpccLoad := func(warehouses int, version string) versionStep {
		return func(ctx context.Context, t *test, u *versionUpgradeTest) {
			// TODO: Also support running on the current versions?
			version = "v" + version
			target := filepath.Join(version, "cockroach")
			importCmd := fmt.Sprintf("./%s workload fixtures import tpcc --warehouses=%d",
				target, warehouses)
			if err := u.c.RunE(ctx, u.c.Node(1), importCmd); err != nil {
				t.Fatal(err)
			}
		}
	}

	backupStep := func(backupName string, nodeIdx int) versionStep {
		return func(ctx context.Context, t *test, u *versionUpgradeTest) {
			// Run a full cluster backup with the given backup name.
			// Move me out to a helper.
			backupLocationBase := backupPrefix + backupName
			db := u.conn(ctx, t, nodeIdx)
			timestamp := fmt.Sprint(timeutil.Now().Add(-1 * time.Second).UnixNano())

			backupLocation := backupLocationBase + "/cluster"
			backupQuery := fmt.Sprintf("BACKUP TO $1 AS OF SYSTEM TIME %s", timestamp)
			if _, err := db.ExecContext(ctx, backupQuery, backupLocation); err != nil {
				t.Fatal(err)
			}
			t.l.Printf("Completed cluster backup '%s'", backupLocation)
			takenBackups = append(takenBackups,
				backup{location: backupLocation, time: timestamp})

			backupLocation = backupLocationBase + "/database"
			backupQuery = fmt.Sprintf("BACKUP DATABASE tpcc TO $1 AS OF SYSTEM TIME %s", timestamp)
			if _, err := db.ExecContext(ctx, backupQuery, backupLocation); err != nil {
				t.Fatal(err)
			}
			t.l.Printf("Completed database backup '%s'", backupName)
			takenBackups = append(takenBackups,
				backup{location: backupLocation, time: timestamp})
		}
	}

	verifyRestoreStep := func() versionStep {
		return func(ctx context.Context, t *test, u *versionUpgradeTest) {
			t.Status("Verifying backups")
			for _, backup := range takenBackups {
				db := u.conn(ctx, t, 1)
				if _, err := db.Exec(`CREATE DATABASE tpcc_restore;`); err != nil {
					t.Fatal(err)
				}
				t.l.Printf("Restoring backup '%s'", backup.location)
				if _, err := db.Exec(
					`RESTORE tpcc.* FROM $1 WITH into_db='tpcc_restore'`, backup.location,
				); err != nil {
					t.Fatal(err)
				}
				t.l.Printf("Restored backup '%s'", backup.location)
				restoreFingerprint, err := fingerprint(ctx, db, "tpcc_restore", "")
				if err != nil {
					t.Fatal(err)
				}
				backupFingerprint, err := fingerprint(ctx, db, "tpcc", backup.time)
				if err != nil {
					t.Fatal(err)
				}
				if restoreFingerprint != backupFingerprint {
					t.Fatalf("restore fingerprint %s did not match backup fingerprint %s for backup",
						restoreFingerprint, backupFingerprint, backup.location)
				}

				if _, err := db.Exec(`DROP DATABASE tpcc_restore;`); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""
	roachNodes := c.All()
	c.Put(ctx, workload, "./workload", c.Node(1))

	u := newVersionUpgradeTest(c,
		uploadAndStartFromCheckpointFixture(roachNodes, predecessorVersion),
		waitForUpgradeStep(roachNodes),
		preventAutoUpgradeStep(1),

		tpccLoad(warehouses, predecessorVersion),
		func(ctx context.Context, _ *test, u *versionUpgradeTest) {
			time.Sleep(10 * time.Second)
		},

		// Roll the nodes into the new version one by one, while repeatedly pausing
		// and resuming all jobs.
		binaryUpgradeStep(c.Node(3), mainVersion),

		binaryUpgradeStep(c.Node(2), mainVersion),
		backupStep("forward_old", 1), // backup on an old version node
		backupStep("forward_new", 2), // backup on a new version node

		binaryUpgradeStep(c.Node(1), mainVersion),

		binaryUpgradeStep(c.Node(4), mainVersion),

		// Roll back again, which ought to be fine because the cluster upgrade was
		// not finalized.
		binaryUpgradeStep(c.Node(2), predecessorVersion),

		binaryUpgradeStep(c.Node(4), predecessorVersion),
		backupStep("backward_new", 1), // backup on a new version node
		backupStep("backward_old", 2), // backup on an old version node

		binaryUpgradeStep(c.Node(3), predecessorVersion),

		binaryUpgradeStep(c.Node(1), predecessorVersion),

		// Roll nodes forward and finalize upgrade.
		binaryUpgradeStep(c.Node(4), mainVersion),

		binaryUpgradeStep(c.Node(3), mainVersion),

		binaryUpgradeStep(c.Node(1), mainVersion),

		binaryUpgradeStep(c.Node(2), mainVersion),

		allowAutoUpgradeStep(1),
		waitForUpgradeStep(roachNodes),

		verifyRestoreStep(),
	)
	u.run(ctx, t)
}

func registerBackupMixedVersion(r *testRegistry) {
	r.Add(testSpec{
		Name:       "backup/mixed-versions",
		Owner:      OwnerBulkIO,
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			predV, err := PredecessorVersion(r.buildVersion)
			if err != nil {
				t.Fatal(err)
			}
			warehouses := 200
			if local {
				warehouses = 2
			}
			runBackupMixedVersions(ctx, t, c, warehouses, predV)
		},
	})
}
