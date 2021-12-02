// Copyright 2018 The Cockroach Authors.
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
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// The following env variable names match those specified in the TeamCity
// configuration for the nightly roachtests. Any changes must be made to both
// references of the name.
const (
	KMSRegionAEnvVar = "AWS_KMS_REGION_A"
	KMSRegionBEnvVar = "AWS_KMS_REGION_B"
	KMSKeyARNAEnvVar = "AWS_KMS_KEY_ARN_A"
	KMSKeyARNBEnvVar = "AWS_KMS_KEY_ARN_B"

	// rows2TiB is the number of rows to import to load 2TB of data (when
	// replicated).
	rows2TiB   = 65_104_166
	rows100GiB = rows2TiB / 20
	rows30GiB  = rows2TiB / 66
	rows15GiB  = rows30GiB / 2
	rows5GiB   = rows100GiB / 20
	rows3GiB   = rows30GiB / 10
)

func destinationName(c *cluster) string {
	dest := c.name
	if c.isLocal() {
		dest += fmt.Sprintf("%d", timeutil.Now().UnixNano())
	}
	return dest
}

func importBankDataSplit(ctx context.Context, rows, ranges int, t *test, c *cluster) string {
	dest := destinationName(c)
	// Randomize starting with encryption-at-rest enabled.
	c.encryptAtRandom = true

	c.Put(ctx, workload, "./workload")
	c.Put(ctx, cockroach, "./cockroach")

	// NB: starting the cluster creates the logs dir as a side effect,
	// needed below.
	c.Start(ctx, t)
	runImportBankDataSplit(ctx, rows, ranges, t, c)
	return dest
}

func runImportBankDataSplit(ctx context.Context, rows, ranges int, t *test, c *cluster) {
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

func importBankData(ctx context.Context, rows int, t *test, c *cluster) string {
	return importBankDataSplit(ctx, rows, 0 /* ranges */, t, c)
}

func registerBackupNodeShutdown(r *testRegistry) {
	// backupNodeRestartSpec runs a backup and randomly shuts down a node during
	// the backup.
	backupNodeRestartSpec := makeClusterSpec(4)
	loadBackupData := func(ctx context.Context, t *test, c *cluster) string {
		// This aught to be enough since this isn't a performance test.
		rows := rows15GiB
		if local {
			// Needs to be sufficiently large to give each processor a good chunk of
			// works so the job doesn't complete immediately.
			rows = rows5GiB
		}
		return importBankData(ctx, rows, t, c)
	}

	r.Add(testSpec{
		Name:       fmt.Sprintf("backup/nodeShutdown/worker/%s", backupNodeRestartSpec),
		Owner:      OwnerBulkIO,
		Cluster:    backupNodeRestartSpec,
		MinVersion: "v21.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			gatewayNode := 2
			nodeToShutdown := 3
			dest := loadBackupData(ctx, t, c)
			backupQuery := `BACKUP bank.bank TO 'nodelocal://1/` + dest + `' WITH DETACHED`
			startBackup := func(c *cluster) (jobID string, err error) {
				gatewayDB := c.Conn(ctx, gatewayNode)
				defer gatewayDB.Close()

				err = gatewayDB.QueryRowContext(ctx, backupQuery).Scan(&jobID)
				return
			}

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, startBackup)
		},
	})
	r.Add(testSpec{
		Name:       fmt.Sprintf("backup/nodeShutdown/coordinator/%s", backupNodeRestartSpec),
		Owner:      OwnerBulkIO,
		Cluster:    backupNodeRestartSpec,
		MinVersion: "v21.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			gatewayNode := 2
			nodeToShutdown := 2
			dest := loadBackupData(ctx, t, c)
			backupQuery := `BACKUP bank.bank TO 'nodelocal://1/` + dest + `' WITH DETACHED`
			startBackup := func(c *cluster) (jobID string, err error) {
				gatewayDB := c.Conn(ctx, gatewayNode)
				defer gatewayDB.Close()

				err = gatewayDB.QueryRowContext(ctx, backupQuery).Scan(&jobID)
				return
			}

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, startBackup)
		},
	})

}

func registerBackupMixedVersion(r *testRegistry) {
	r.Add(testSpec{
		Name:    "backup/mixed-version",
		Owner:   OwnerBulkIO,
		Cluster: makeClusterSpec(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			// An empty string means that the cockroach binary specified by flag
			// `cockroach` will be used.
			const mainVersion = ""
			roachNodes := c.All()
			predV, err := PredecessorVersion(r.buildVersion)
			require.NoError(t, err)
			c.Put(ctx, workload, "./workload")

			loadBackupDataStep := func(ctx context.Context, t *test, u *versionUpgradeTest) {
				rows := rows3GiB
				if c.isLocal() {
					rows = 100
				}
				runImportBankDataSplit(ctx, rows, 0 /* ranges */, t, u.c)
			}
			successfulBackupStep := func(nodeID int, opts ...string) versionStep {
				return func(ctx context.Context, t *test, u *versionUpgradeTest) {
					backupOpts := ""
					if len(opts) > 0 {
						backupOpts = fmt.Sprintf("WITH %s", strings.Join(opts, ", "))
					}
					backupQuery := fmt.Sprintf("BACKUP bank.bank TO 'nodelocal://%d/%s' %s",
						nodeID, destinationName(c), backupOpts)
					gatewayDB := c.Conn(ctx, nodeID)
					defer gatewayDB.Close()
					t.Status("Running: ", backupQuery)
					_, err := gatewayDB.ExecContext(ctx, backupQuery)
					require.NoError(t, err)
				}
			}

			// workaroundForAncientData attempts to clean up a range that has data that results in a failed
			// backup. In the v20.2 checkpoint data, we have 3 KVs for the eventlog row in the descriptor
			// table. The following are the MVCC timestamps and modification_time in the deserialized table
			// descriptor for these entries:
			//
			// | mvcc_timestamp         | modification_time   |
			// |----------------------------------------------|
			// | 1585824927.878520565,0 | 1585824876057272000 |
			// | 1585824876.059029000,0 | 1585824876057272000 |
			// | 1585824876.057140000,0 | 1585824876057272000 | <- not ok
			//
			// The last entry violates an invariant that the modification time should be at or before the
			// mvcc_timestamp. Because of this, the node will crash with a fatal log message when we attempt
			// to process this KV in getAllDescChanges in pkg/ccl/backupcc/targets.go:
			//
			//     sql/catalog/descpb/descriptor.go:191 ⋮ [-] 173 read table descriptor ‹"eventlog"› (12)
			//     which has a ModificationTime after its MVCC timestamp: has 1585824876.057272000,0,
			//     expected 1585824876.057140000,0
			//
			// We believe that this data is the result of a v1.0 bug that was fixed in
			// https://github.com/cockroachdb/cockroach/pull/16842.
			//
			// Given this, we are comfortable working around this problem rather than explicitly handling it
			// in the backup code. Here, we force the range containing this data through the gc queue which
			// will GC the old revision as it is not the latest and well past the gc.ttl.
			workaroundForAncientData := func(ctx context.Context, t *test, u *versionUpgradeTest) {
				db := c.Conn(ctx, 1)
				waitForFullReplication(t, db)
				t.Status("sending enqueue_range request for queue=gc and range=5")
				req := &serverpb.EnqueueRangeRequest{
					Queue:           "gc",
					RangeID:         5,
					SkipShouldQueue: true,
				}
				host := c.ExternalAdminUIAddr(ctx, c.Node(1))[0]
				url := fmt.Sprintf("http://%s/_admin/v1/enqueue_range", host)
				var resp serverpb.EnqueueRangeResponse
				err := httputil.PostJSON(*http.DefaultClient, url, req, &resp)
				require.NoError(t, err)
				t.l.Printf("EnqueueRangeResponse: %v", resp)
			}

			u := newVersionUpgradeTest(c,
				uploadAndStartFromCheckpointFixture(roachNodes, predV),
				waitForUpgradeStep(roachNodes),
				preventAutoUpgradeStep(1),
				workaroundForAncientData,
				loadBackupDataStep,
				// Upgrade some of the nodes.
				binaryUpgradeStep(c.Nodes(1, 2), mainVersion),
				// Backup from new node should succeed
				successfulBackupStep(1),
				// Backup from new node with revision history should succeed
				successfulBackupStep(2, "revision_history"),
				// Backup from old node should succeed
				successfulBackupStep(3),
				// Backup from old node with revision history should succeed
				successfulBackupStep(4, "revision_history"),
			)
			u.run(ctx, t)
		},
	})
}

func registerBackup(r *testRegistry) {
	backup2TBSpec := makeClusterSpec(10)
	r.Add(testSpec{
		Name:       fmt.Sprintf("backup/2TB/%s", backup2TBSpec),
		Owner:      OwnerBulkIO,
		Cluster:    backup2TBSpec,
		MinVersion: "v2.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			rows := rows2TiB
			if local {
				rows = 100
			}
			dest := importBankData(ctx, rows, t, c)
			m := newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				t.Status(`running backup`)
				c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "
				BACKUP bank.bank TO 'gs://cockroachdb-backup-testing/`+dest+`'"`)
				return nil
			})
			m.Wait()
		},
	})

	KMSSpec := makeClusterSpec(3)
	r.Add(testSpec{
		Name:       fmt.Sprintf("backup/KMS/%s", KMSSpec.String()),
		Owner:      OwnerBulkIO,
		Cluster:    KMSSpec,
		MinVersion: "v20.2.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			if cloud == gce {
				t.Skip("backupKMS roachtest is only configured to run on AWS", "")
			}

			// ~10GiB - which is 30Gib replicated.
			rows := rows30GiB
			if local {
				rows = 100
			}
			dest := importBankData(ctx, rows, t, c)

			conn := c.Conn(ctx, 1)
			m := newMonitor(ctx, c)
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
			m = newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				t.Status(`running encrypted backup`)
				kmsURIA, err = getAWSKMSURI(KMSRegionAEnvVar, KMSKeyARNAEnvVar)
				if err != nil {
					return err
				}

				kmsURIB, err = getAWSKMSURI(KMSRegionBEnvVar, KMSKeyARNBEnvVar)
				if err != nil {
					return err
				}

				kmsOptions := fmt.Sprintf("KMS=('%s', '%s')", kmsURIA, kmsURIB)
				_, err := conn.ExecContext(ctx,
					`BACKUP bank.bank TO 'nodelocal://1/kmsbackup/`+dest+`' WITH `+kmsOptions)
				return err
			})
			m.Wait()

			// Restore the encrypted BACKUP using each of KMS URI A and B separately.
			m = newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				t.Status(`restore using KMSURIA`)
				if _, err := conn.ExecContext(ctx,
					`RESTORE bank.bank FROM $1 WITH into_db=restoreA, kms=$2`,
					`nodelocal://1/kmsbackup/`+dest, kmsURIA,
				); err != nil {
					return err
				}

				t.Status(`restore using KMSURIB`)
				if _, err := conn.ExecContext(ctx,
					`RESTORE bank.bank FROM $1 WITH into_db=restoreB, kms=$2`,
					`nodelocal://1/kmsbackup/`+dest, kmsURIB,
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

	// backupTPCC continuously runs TPCC, takes a full backup after some time,
	// and incremental after more time. It then restores the two backups and
	// verifies them with a fingerprint.
	r.Add(testSpec{
		Name:    `backupTPCC`,
		Owner:   OwnerBulkIO,
		Cluster: makeClusterSpec(3),
		Timeout: 1 * time.Hour,
		Run: func(ctx context.Context, t *test, c *cluster) {
			// Randomize starting with encryption-at-rest enabled.
			c.encryptAtRandom = true
			c.Put(ctx, cockroach, "./cockroach")
			c.Put(ctx, workload, "./workload")
			c.Start(ctx, t)
			conn := c.Conn(ctx, 1)

			duration := 5 * time.Minute
			if local {
				duration = 5 * time.Second
			}
			warehouses := 10

			backupDir := "gs://cockroachdb-backup-testing/" + c.name
			// Use inter-node file sharing on 20.1+.
			if r.buildVersion.AtLeast(version.MustParse(`v20.1.0-0`)) {
				backupDir = "nodelocal://1/" + c.name
			}
			fullDir := backupDir + "/full"
			incDir := backupDir + "/inc"

			t.Status(`workload initialization`)
			cmd := []string{fmt.Sprintf(
				"./workload init tpcc --warehouses=%d {pgurl:1-%d}",
				warehouses, c.spec.NodeCount,
			)}
			if !t.buildVersion.AtLeast(version.MustParse("v20.2.0")) {
				cmd = append(cmd, "--deprecated-fk-indexes")
			}
			c.Run(ctx, c.Node(1), cmd...)

			m := newMonitor(ctx, c)
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
					warehouses, c.spec.NodeCount,
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
			m = newMonitor(ctx, c)
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
			m = newMonitor(ctx, c)
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

			m = newMonitor(ctx, c)
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
		"AWS_ACCESS_KEY_ID":     cloudimpl.AWSAccessKeyParam,
		"AWS_SECRET_ACCESS_KEY": cloudimpl.AWSSecretParam,
		regionEnvVariable:       cloudimpl.KMSRegionParam,
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

	// Set AUTH to implicit
	q.Add(cloudimpl.AuthParam, cloudimpl.AuthParamSpecified)
	correctURI := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())

	return correctURI, nil
}
