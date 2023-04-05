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
	"strings"
	"time"

	cloudstorage "github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/cloud/gcp"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// This file contains roach tests that test backup and restore. Be careful about
// changing the names of existing tests as it risks breaking roachperf or tests
// that are run on AWS.
//
// The following env variable names match those specified in the TeamCity
// configuration for the nightly roachtests. Any changes must be made to both
// references of the name.
const (
	KMSRegionAEnvVar             = "AWS_KMS_REGION_A"
	KMSRegionBEnvVar             = "AWS_KMS_REGION_B"
	KMSKeyARNAEnvVar             = "AWS_KMS_KEY_ARN_A"
	KMSKeyARNBEnvVar             = "AWS_KMS_KEY_ARN_B"
	AssumeRoleAWSKeyIDEnvVar     = "AWS_ACCESS_KEY_ID_ASSUME_ROLE"
	AssumeRoleAWSSecretKeyEnvVar = "AWS_SECRET_ACCESS_KEY_ASSUME_ROLE"
	AssumeRoleAWSRoleEnvVar      = "AWS_ROLE_ARN"

	KMSKeyNameAEnvVar           = "GOOGLE_KMS_KEY_A"
	KMSKeyNameBEnvVar           = "GOOGLE_KMS_KEY_B"
	KMSGCSCredentials           = "GOOGLE_EPHEMERAL_CREDENTIALS"
	AssumeRoleGCSCredentials    = "GOOGLE_CREDENTIALS_ASSUME_ROLE"
	AssumeRoleGCSServiceAccount = "GOOGLE_SERVICE_ACCOUNT"

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
	c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), install.MakeClusterSettings())
	runImportBankDataSplit(ctx, rows, ranges, t, c)
	return dest
}

// setShortJobIntervalsCommon exists because this functionality is
// shared across the `backup/mixed-version` and
// `declarative_schema_changer/job-compatibility-mixed-version`.
// TODO(renato): Delete this duplicated code once both tests have been
// migrated to the new mixed-version testing framework.
func setShortJobIntervalsCommon(runQuery func(query string, args ...interface{}) error) error {
	settings := map[string]string{
		"jobs.registry.interval.cancel": "1s",
		"jobs.registry.interval.adopt":  "1s",
	}

	for name, val := range settings {
		query := fmt.Sprintf("SET CLUSTER SETTING %s = $1", name)
		if err := runQuery(query, val); err != nil {
			return err
		}
	}

	return nil
}

// importBankCSVServerCommand returns the command to start a csv
// server on the specified port, using the given cockroach binary.
func importBankCSVServerCommand(cockroach string, port int) string {
	return roachtestutil.
		NewCommand("%s workload csv-server", cockroach).
		Flag("port", port).
		String()
}

// importBankCommand returns the command to import `bank` fixtures
// according to the parameters passed.
func importBankCommand(cockroach string, rows, ranges, csvPort, node int) string {
	return roachtestutil.
		NewCommand("%s workload fixtures import bank", cockroach).
		Arg("{pgurl:%d}", node).
		Flag("db", "bank").
		Flag("payload-bytes", 10240).
		Flag("csv-server", fmt.Sprintf("http://localhost:%d", csvPort)).
		Flag("seed", 1).
		Flag("ranges", ranges).
		Flag("rows", rows).
		String()
}

// waitForPort waits until the given `port` is ready to receive
// connections on all `nodes` given.
func waitForPort(
	ctx context.Context, nodes option.NodeListOption, port int, c cluster.Cluster,
) error {
	if err := retry.WithMaxAttempts(ctx, retry.Options{
		MaxBackoff: 500 * time.Millisecond,
	}, 10, func() error {
		return c.RunE(ctx, nodes, fmt.Sprintf("sudo lsof -i:%d | grep -q LISTEN", port))
	}); err != nil {
		return fmt.Errorf("timed out waiting for port %d: %w", port, err)
	}

	return nil
}

func runImportBankDataSplit(ctx context.Context, rows, ranges int, t test.Test, c cluster.Cluster) {
	csvPort := 8081
	csvCmd := importBankCSVServerCommand("./cockroach", csvPort)
	c.Run(ctx, c.All(), csvCmd+` &> logs/workload-csv-server.log < /dev/null &`)
	if err := waitForPort(ctx, c.All(), csvPort, c); err != nil {
		t.Fatal(err)
	}
	importNode := 1
	c.Run(ctx, c.Node(importNode), importBankCommand("./cockroach", rows, ranges, csvPort, importNode))
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
		Owner:             registry.OwnerDisasterRecovery,
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
		Owner:             registry.OwnerDisasterRecovery,
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

// disableJobAdoptionStep writes the sentinel file to prevent a node's
// registry from adopting a job.
//
// TODO(renato): remove this duplicated function once
// `declarative_schema_changer/job-compatibility-mixed-version` is
// migrated to the new mixed-version testing framework.
func disableJobAdoptionStep(c cluster.Cluster, nodeIDs option.NodeListOption) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		for _, nodeID := range nodeIDs {
			result, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(nodeID), "echo", "-n", "{store-dir}")
			if err != nil {
				t.L().Printf("Failed to retrieve store directory from node %d: %v\n", nodeID, err.Error())
			}
			storeDirectory := result.Stdout
			disableJobAdoptionSentinelFilePath := filepath.Join(storeDirectory, jobs.PreventAdoptionFile)
			cmd := fmt.Sprintf("touch %s", disableJobAdoptionSentinelFilePath)
			c.Run(ctx, nodeIDs, cmd)
			t.L().Printf("Disabling job adoption on node %v: > %v\n", nodeID, cmd)

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
//
// TODO(renato): remove this duplicated function once
// `declarative_schema_changer/job-compatibility-mixed-version` is
// migrated to the new mixed-version testing framework.
func enableJobAdoptionStep(c cluster.Cluster, nodeIDs option.NodeListOption) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		for _, nodeID := range nodeIDs {
			result, err := c.RunWithDetailsSingleNode(ctx, t.L(),
				c.Node(nodeID), "echo", "-n", "{store-dir}")
			if err != nil {
				t.L().Printf("Failed to retrieve store directory from node %d: %v\n", nodeID, err.Error())
			}
			storeDirectory := result.Stdout
			disableJobAdoptionSentinelFilePath := filepath.Join(storeDirectory, jobs.PreventAdoptionFile)
			cmd := fmt.Sprintf("rm -f %s", disableJobAdoptionSentinelFilePath)
			c.Run(ctx, nodeIDs, cmd)
			t.L().Printf("Enabling job adoption on node %v: > %v\n", nodeID, cmd)
		}

		// Reset the env variable that controls how many jobs are claimed by the
		// registry.
		_, err := c.RunWithDetails(ctx, t.L(), nodeIDs, "export COCKROACH_JOB_ADOPTIONS_PER_PERIOD=10")
		require.NoError(t, err)
	}
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
		Owner:             registry.OwnerDisasterRecovery,
		Cluster:           backup2TBSpec,
		EncryptionSupport: registry.EncryptionAlwaysDisabled,
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

	for _, item := range []struct {
		cloudProvider string
		machine       string
	}{
		{cloudProvider: "GCS", machine: spec.GCE},
		{cloudProvider: "AWS", machine: spec.AWS},
	} {
		item := item
		r.Add(registry.TestSpec{
			Name:              fmt.Sprintf("backup/assume-role/%s", item.cloudProvider),
			Owner:             registry.OwnerDisasterRecovery,
			Cluster:           r.MakeClusterSpec(3),
			EncryptionSupport: registry.EncryptionMetamorphic,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.Spec().Cloud != item.machine {
					t.Skip("backup assumeRole is only configured to run on "+item.machine, "")
				}

				rows := 100

				dest := importBankData(ctx, rows, t, c)

				var backupPath, kmsURI string
				var err error
				switch item.cloudProvider {
				case "AWS":
					if backupPath, err = getAWSBackupPath(dest); err != nil {
						t.Fatal(err)
					}
					if kmsURI, err = getAWSKMSAssumeRoleURI(); err != nil {
						t.Fatal(err)
					}
				case "GCS":
					if backupPath, err = getGCSBackupPath(dest); err != nil {
						t.Fatal(err)
					}
					if kmsURI, err = getGCSKMSAssumeRoleURI(); err != nil {
						t.Fatal(err)
					}
				}

				conn := c.Conn(ctx, t.L(), 1)
				m := c.NewMonitor(ctx)
				m.Go(func(ctx context.Context) error {
					t.Status(`running backup`)
					_, err := conn.ExecContext(ctx, "BACKUP bank.bank INTO $1 WITH KMS=$2",
						backupPath, kmsURI)
					return err
				})
				m.Wait()

				m = c.NewMonitor(ctx)
				m.Go(func(ctx context.Context) error {
					t.Status(`restoring from backup`)
					if _, err := conn.ExecContext(ctx, "CREATE DATABASE restoreDB"); err != nil {
						return err
					}

					if _, err := conn.ExecContext(ctx,
						`RESTORE bank.bank FROM LATEST IN $1 WITH into_db=restoreDB, kms=$2`,
						backupPath, kmsURI,
					); err != nil {
						return err
					}

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
					restore, err := fingerprint("restoreDB")
					if err != nil {
						return err
					}

					if originalBank != restore {
						return errors.Errorf("got %s, expected %s while comparing restoreDB with originalBank", restore, originalBank)
					}
					return nil
				})

				m.Wait()
			},
		})
	}
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
			Owner:             registry.OwnerDisasterRecovery,
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
		Owner:             registry.OwnerDisasterRecovery,
		Cluster:           r.MakeClusterSpec(3),
		Timeout:           1 * time.Hour,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Put(ctx, t.DeprecatedWorkload(), "./workload")
			c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), install.MakeClusterSettings())
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

	r.Add(registry.TestSpec{
		Name:              "backup/mvcc-range-tombstones",
		Owner:             registry.OwnerDisasterRecovery,
		Timeout:           4 * time.Hour,
		Cluster:           r.MakeClusterSpec(3, spec.CPU(8)),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runBackupMVCCRangeTombstones(ctx, t, c, mvccRangeTombstoneConfig{})
		},
	})
}

type mvccRangeTombstoneConfig struct {
	tenantName       string
	skipClusterSetup bool

	// TODO(msbutler): delete once tenants can back up to nodelocal.
	skipBackupRestore bool
}

// runBackupMVCCRangeTombstones tests that backup and restore works in the
// presence of MVCC range tombstones. It uses data from TPCH's order table, 16
// GB across 8 CSV files.
//
//  1. Import half of the tpch.orders table (odd-numbered files).
//  2. Take fingerprint, time 'initial'.
//  3. Take a full database backup.
//  4. Import the other half (even-numbered files), but cancel the import
//     and roll the data back using MVCC range tombstones. Done twice.
//  5. Take fingerprint, time 'canceled'.
//  6. Successfully import the other half.
//  7. Take fingerprint, time 'completed'.
//  8. Drop the table and wait for deletion with MVCC range tombstone.
//  9. Take an incremental database backup with revision history.
//
// We then do point-in-time restores of the database at times 'initial',
// 'canceled', 'completed', and the latest time, and compare the fingerprints to
// the original data.
func runBackupMVCCRangeTombstones(
	ctx context.Context, t test.Test, c cluster.Cluster, config mvccRangeTombstoneConfig,
) {
	if !config.skipClusterSetup {
		c.Put(ctx, t.Cockroach(), "./cockroach")
		c.Put(ctx, t.DeprecatedWorkload(), "./workload") // required for tpch
		c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), install.MakeClusterSettings())
	}
	t.Status("starting csv servers")
	c.Run(ctx, c.All(), `./cockroach workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)

	conn := c.Conn(ctx, t.L(), 1, option.TenantName(config.tenantName))

	// Configure cluster.
	t.Status("configuring cluster")
	_, err := conn.Exec(`SET CLUSTER SETTING kv.bulk_ingest.max_index_buffer_size = '2gb'`)
	require.NoError(t, err)
	if config.tenantName == "" {
		_, err = conn.Exec(`SET CLUSTER SETTING storage.mvcc.range_tombstones.enabled = 't'`)
		require.NoError(t, err)
	}
	_, err = conn.Exec(`SET CLUSTER SETTING server.debug.default_vmodule = 'txn=2,sst_batcher=4,
revert=2'`)
	require.NoError(t, err)
	// Wait for ranges to upreplicate.
	require.NoError(t, WaitFor3XReplication(ctx, t, conn))

	// Create the orders table. It's about 16 GB across 8 files.
	t.Status("creating table")
	_, err = conn.Exec(`CREATE DATABASE tpch`)
	require.NoError(t, err)
	_, err = conn.Exec(`USE tpch`)
	require.NoError(t, err)
	createStmt, err := readCreateTableFromFixture(
		"gs://cockroach-fixtures/tpch-csv/schema/orders.sql?AUTH=implicit", conn)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, createStmt)
	require.NoError(t, err)

	// Set up some helpers.
	waitForStatus := func(
		jobID string,
		expectStatus jobs.Status,
		expectRunningStatus jobs.RunningStatus,
		duration time.Duration,
	) {
		ctx, cancel := context.WithTimeout(ctx, duration)
		defer cancel()
		require.NoError(t, retry.Options{}.Do(ctx, func(ctx context.Context) error {
			var status string
			var payloadBytes, progressBytes []byte
			require.NoError(t, conn.QueryRowContext(
				ctx, jobutils.InternalSystemJobsBaseQuery, jobID).
				Scan(&status, &payloadBytes, &progressBytes))
			if jobs.Status(status) == jobs.StatusFailed {
				var payload jobspb.Payload
				require.NoError(t, protoutil.Unmarshal(payloadBytes, &payload))
				t.Fatalf("job failed: %s", payload.Error)
			}
			if jobs.Status(status) != expectStatus {
				return errors.Errorf("expected job status %s, but got %s", expectStatus, status)
			}
			if expectRunningStatus != "" {
				var progress jobspb.Progress
				require.NoError(t, protoutil.Unmarshal(progressBytes, &progress))
				if jobs.RunningStatus(progress.RunningStatus) != expectRunningStatus {
					return errors.Errorf("expected running status %s, but got %s",
						expectRunningStatus, progress.RunningStatus)
				}
			}
			return nil
		}))
	}

	fingerprint := func(name, database, table string) (string, string, string) {
		if config.skipBackupRestore {
			return "", "", ""
		}
		var ts string
		require.NoError(t, conn.QueryRowContext(ctx, `SELECT now()`).Scan(&ts))

		t.Status(fmt.Sprintf("fingerprinting %s.%s at time '%s'", database, table, name))
		fp, err := fingerprint(ctx, conn, database, table)
		require.NoError(t, err)
		t.Status("fingerprint:\n", fp)

		return name, ts, fp
	}

	// restores track point-in-time restores to execute and validate.
	type restore struct {
		name              string
		time              string
		expectFingerprint string
		expectNoTables    bool
	}
	var restores []restore

	// Import the odd-numbered files.
	t.Status("importing odd-numbered files")
	files := []string{
		`gs://cockroach-fixtures/tpch-csv/sf-100/orders.tbl.1?AUTH=implicit`,
		`gs://cockroach-fixtures/tpch-csv/sf-100/orders.tbl.3?AUTH=implicit`,
		`gs://cockroach-fixtures/tpch-csv/sf-100/orders.tbl.5?AUTH=implicit`,
		`gs://cockroach-fixtures/tpch-csv/sf-100/orders.tbl.7?AUTH=implicit`,
	}
	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		`IMPORT INTO orders CSV DATA ('%s') WITH delimiter='|'`, strings.Join(files, "', '")))
	require.NoError(t, err)

	// Fingerprint for restore comparison.
	name, ts, fpInitial := fingerprint("initial", "tpch", "orders")
	restores = append(restores, restore{
		name:              name,
		time:              ts,
		expectFingerprint: fpInitial,
	})

	// Take a full backup, using a database backup in order to perform a final
	// incremental backup after the table has been dropped.
	dest := "nodelocal://1/" + destinationName(c)
	if !config.skipBackupRestore {
		t.Status("taking full backup")
		_, err = conn.ExecContext(ctx, `BACKUP DATABASE tpch INTO $1 WITH revision_history`, dest)
		require.NoError(t, err)
	}

	// Import and cancel even-numbered files twice.
	files = []string{
		`gs://cockroach-fixtures/tpch-csv/sf-100/orders.tbl.2?AUTH=implicit`,
		`gs://cockroach-fixtures/tpch-csv/sf-100/orders.tbl.4?AUTH=implicit`,
		`gs://cockroach-fixtures/tpch-csv/sf-100/orders.tbl.6?AUTH=implicit`,
		`gs://cockroach-fixtures/tpch-csv/sf-100/orders.tbl.8?AUTH=implicit`,
	}

	_, err = conn.ExecContext(ctx,
		`SET CLUSTER SETTING jobs.debug.pausepoints = 'import.after_ingest'`)
	require.NoError(t, err)

	var jobID string
	for i := 0; i < 2; i++ {
		t.Status("importing even-numbered files")
		require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
			`IMPORT INTO orders CSV DATA ('%s') WITH delimiter='|', detached`,
			strings.Join(files, "', '")),
		).Scan(&jobID))
		waitForStatus(jobID, jobs.StatusPaused, "", 30*time.Minute)

		t.Status("canceling import")
		_, err = conn.ExecContext(ctx, fmt.Sprintf(`CANCEL JOB %s`, jobID))
		require.NoError(t, err)
		waitForStatus(jobID, jobs.StatusCanceled, "", 30*time.Minute)
	}

	_, err = conn.ExecContext(ctx, `SET CLUSTER SETTING jobs.debug.pausepoints = ''`)
	require.NoError(t, err)

	// Check that we actually wrote MVCC range tombstones.
	var rangeKeys int
	require.NoError(t, conn.QueryRowContext(ctx, `
		SELECT sum((crdb_internal.range_stats(raw_start_key)->'range_key_count')::INT)
		FROM [SHOW RANGES FROM TABLE tpch.orders WITH KEYS]
`).Scan(&rangeKeys))
	require.NotZero(t, rangeKeys, "no MVCC range tombstones found")

	// Fingerprint for restore comparison, and assert that it matches the initial
	// import.
	name, ts, fp := fingerprint("canceled", "tpch", "orders")
	restores = append(restores, restore{
		name:              name,
		time:              ts,
		expectFingerprint: fp,
	})
	require.Equal(t, fpInitial, fp, "fingerprint mismatch between initial and canceled")

	// Now actually import the even-numbered files.
	t.Status("importing even-numbered files")
	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		`IMPORT INTO orders CSV DATA ('%s') WITH delimiter='|'`, strings.Join(files, "', '")))
	require.NoError(t, err)

	// Fingerprint for restore comparison.
	name, ts, fp = fingerprint("completed", "tpch", "orders")
	restores = append(restores, restore{
		name:              name,
		time:              ts,
		expectFingerprint: fp,
	})

	// Drop the table, and wait for it to be deleted (but not GCed).
	t.Status("dropping table")
	_, err = conn.ExecContext(ctx, `DROP TABLE orders`)
	require.NoError(t, err)
	require.NoError(t, conn.QueryRowContext(ctx,
		`SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE GC'`).Scan(&jobID))
	waitForStatus(jobID, jobs.StatusRunning, sql.RunningStatusWaitingForMVCCGC, 2*time.Minute)

	// Check that the data has been deleted. We don't write MVCC range tombstones
	// unless the range contains live data, so only assert their existence if the
	// range has any user keys.
	var rangeID int
	var stats string
	err = conn.QueryRowContext(ctx, `
		SELECT range_id, stats::STRING
		FROM [
			SELECT range_id, crdb_internal.range_stats(raw_start_key) AS stats
			FROM [SHOW RANGES FROM DATABASE tpch WITH KEYS]
		]
		WHERE (stats->'live_count')::INT != 0 OR (
			(stats->'key_count')::INT > 0 AND (stats->'range_key_count')::INT = 0
		)
	`).Scan(&rangeID, &stats)
	require.NotNil(t, err, "range %d not fully deleted, stats: %s", rangeID, stats)
	require.ErrorIs(t, err, gosql.ErrNoRows)

	// Take a final incremental backup.
	if !config.skipBackupRestore {
		t.Status("taking incremental backup")
		_, err = conn.ExecContext(ctx, `BACKUP DATABASE tpch INTO LATEST IN $1 WITH revision_history`,
			dest)
		require.NoError(t, err)

		// Schedule a final restore of the latest backup (above).
		restores = append(restores, restore{
			name:           "dropped",
			expectNoTables: true,
		})

		// Restore backups at specific times and verify them.
		for _, r := range restores {
			t.Status(fmt.Sprintf("restoring backup at time '%s'", r.name))
			db := "restore_" + r.name
			if r.time != "" {
				_, err = conn.ExecContext(ctx, fmt.Sprintf(
					`RESTORE DATABASE tpch FROM LATEST IN '%s' AS OF SYSTEM TIME '%s' WITH new_db_name = '%s'`,
					dest, r.time, db))
				require.NoError(t, err)
			} else {
				_, err = conn.ExecContext(ctx, fmt.Sprintf(
					`RESTORE DATABASE tpch FROM LATEST IN '%s' WITH new_db_name = '%s'`, dest, db))
				require.NoError(t, err)
			}

			if expect := r.expectFingerprint; expect != "" {
				_, _, fp = fingerprint(r.name, db, "orders")
				require.Equal(t, expect, fp, "fingerprint mismatch for restore at time '%s'", r.name)
			}
			if r.expectNoTables {
				var tableCount int
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
					`SELECT count(*) FROM [SHOW TABLES FROM %s]`, db)).Scan(&tableCount))
				require.Zero(t, tableCount, "found tables in restore at time '%s'", r.name)
				t.Status("confirmed no tables in database " + db)
			}
		}
	}
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

func getAWSKMSAssumeRoleURI() (string, error) {
	q := make(url.Values)
	expect := map[string]string{
		AssumeRoleAWSKeyIDEnvVar:     amazon.AWSAccessKeyParam,
		AssumeRoleAWSSecretKeyEnvVar: amazon.AWSSecretParam,
		AssumeRoleAWSRoleEnvVar:      amazon.AssumeRoleParam,
		KMSRegionAEnvVar:             amazon.KMSRegionParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			return "", errors.Newf("env variable %s must be present to run the KMS test", env)
		}
		q.Add(param, v)
	}

	// Get AWS Key ARN from env variable.
	keyARN := os.Getenv(KMSKeyARNAEnvVar)
	if keyARN == "" {
		return "", errors.Newf("env variable %s must be present to run the KMS test", KMSKeyARNAEnvVar)
	}

	// Set AUTH to specified.
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

func getGCSKMSAssumeRoleURI() (string, error) {
	q := make(url.Values)
	expect := map[string]string{
		AssumeRoleGCSCredentials:    gcp.CredentialsParam,
		AssumeRoleGCSServiceAccount: gcp.AssumeRoleParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			return "", errors.Newf("env variable %s must be present to run the KMS test", env)
		}
		// Nightly env uses JSON credentials, which have to be base64 encoded.
		if param == gcp.CredentialsParam {
			v = base64.StdEncoding.EncodeToString([]byte(v))
		}
		q.Add(param, v)
	}

	// Get AWS Key ARN from env variable.
	keyName := os.Getenv(KMSKeyNameAEnvVar)
	if keyName == "" {
		return "", errors.Newf("env variable %s must be present to run the KMS test", KMSKeyNameAEnvVar)
	}

	// Set AUTH to specified
	q.Add(cloudstorage.AuthParam, cloudstorage.AuthParamSpecified)
	correctURI := fmt.Sprintf("gs:///%s?%s", keyName, q.Encode())

	return correctURI, nil
}

func getGCSBackupPath(dest string) (string, error) {
	q := make(url.Values)
	expect := map[string]string{
		AssumeRoleGCSCredentials:    gcp.CredentialsParam,
		AssumeRoleGCSServiceAccount: gcp.AssumeRoleParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			return "", errors.Errorf("env variable %s must be present to run the assume role test", env)
		}

		// Nightly env uses JSON credentials, which have to be base64 encoded.
		if param == gcp.CredentialsParam {
			v = base64.StdEncoding.EncodeToString([]byte(v))
		}
		q.Add(param, v)
	}

	// Set AUTH to specified
	q.Add(cloudstorage.AuthParam, cloudstorage.AuthParamSpecified)
	uri := fmt.Sprintf("gs://cockroachdb-backup-testing/gcs/%s?%s", dest, q.Encode())

	return uri, nil
}

func getAWSBackupPath(dest string) (string, error) {
	q := make(url.Values)
	expect := map[string]string{
		AssumeRoleAWSKeyIDEnvVar:     amazon.AWSAccessKeyParam,
		AssumeRoleAWSSecretKeyEnvVar: amazon.AWSSecretParam,
		AssumeRoleAWSRoleEnvVar:      amazon.AssumeRoleParam,
		KMSRegionAEnvVar:             amazon.KMSRegionParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			return "", errors.Errorf("env variable %s must be present to run the KMS test", env)
		}
		q.Add(param, v)
	}
	q.Add(cloudstorage.AuthParam, cloudstorage.AuthParamSpecified)

	return fmt.Sprintf("s3://cockroachdb-backup-testing/%s?%s", dest, q.Encode()), nil
}
