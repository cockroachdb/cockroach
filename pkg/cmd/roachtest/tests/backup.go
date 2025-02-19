// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
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
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
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

var backupTestingBucket = testutils.BackupTestingBucket()

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

	// NB: starting the cluster creates the logs dir as a side effect,
	// needed below.
	c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), install.MakeClusterSettings())
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
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, port int, c cluster.Cluster,
) error {
	ips, err := c.ExternalIP(ctx, l, nodes)
	if err != nil {
		return fmt.Errorf("failed to get nodes external IPs: %w", err)
	}

	retryConfig := retry.Options{MaxBackoff: 500 * time.Millisecond}
	for j, ip := range ips {
		if err := retry.WithMaxAttempts(ctx, retryConfig, 10, func() error {
			addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return fmt.Errorf("error connecting to node %d (%s): %w", nodes[j], addr, err)
			}

			if err := conn.Close(); err != nil {
				return fmt.Errorf("error closing connection to node %d (%s): %w", nodes[j], addr, err)
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

func runImportBankDataSplit(ctx context.Context, rows, ranges int, t test.Test, c cluster.Cluster) {
	csvPort := 8081
	csvCmd := importBankCSVServerCommand("./cockroach", csvPort)
	c.Run(ctx, option.WithNodes(c.All()), csvCmd+` &> logs/workload-csv-server.log < /dev/null &`)
	if err := waitForPort(ctx, t.L(), c.All(), csvPort, c); err != nil {
		t.Fatal(err)
	}
	importNode := 1
	c.Run(ctx, option.WithNodes(c.Node(importNode)), importBankCommand("./cockroach", rows, ranges, csvPort, importNode))
}

func importBankData(ctx context.Context, rows int, t test.Test, c cluster.Cluster) string {
	return importBankDataSplit(ctx, rows, 0, t, c)
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
		Name:                      fmt.Sprintf("backup/nodeShutdown/worker/%s", backupNodeRestartSpec),
		Owner:                     registry.OwnerDisasterRecovery,
		Cluster:                   backupNodeRestartSpec,
		EncryptionSupport:         registry.EncryptionMetamorphic,
		Leases:                    registry.MetamorphicLeases,
		CompatibleClouds:          registry.AllExceptAWS,
		Suites:                    registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := 2
			nodeToShutdown := 3
			dest := loadBackupData(ctx, t, c)
			backupQuery := `BACKUP bank.bank INTO 'nodelocal://1/` + dest + `' WITH DETACHED`
			startBackup := func(c cluster.Cluster, l *logger.Logger) (jobID jobspb.JobID, err error) {
				gatewayDB := c.Conn(ctx, l, gatewayNode)
				defer gatewayDB.Close()

				err = gatewayDB.QueryRowContext(ctx, backupQuery).Scan(&jobID)
				return
			}

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, startBackup)
		},
	})
	r.Add(registry.TestSpec{
		Name:                      fmt.Sprintf("backup/nodeShutdown/coordinator/%s", backupNodeRestartSpec),
		Owner:                     registry.OwnerDisasterRecovery,
		Cluster:                   backupNodeRestartSpec,
		EncryptionSupport:         registry.EncryptionMetamorphic,
		Leases:                    registry.MetamorphicLeases,
		CompatibleClouds:          registry.AllExceptAWS,
		Suites:                    registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := 2
			nodeToShutdown := 2
			dest := loadBackupData(ctx, t, c)
			backupQuery := `BACKUP bank.bank INTO 'nodelocal://1/` + dest + `' WITH DETACHED`
			startBackup := func(c cluster.Cluster, l *logger.Logger) (jobID jobspb.JobID, err error) {
				gatewayDB := c.Conn(ctx, l, gatewayNode)
				defer gatewayDB.Close()

				err = gatewayDB.QueryRowContext(ctx, backupQuery).Scan(&jobID)
				return
			}

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, startBackup)
		},
	})

}

// initBulkJobPerfArtifacts registers a histogram, creates a performance
// artifact directory and returns a method that when invoked records a tick.
func initBulkJobPerfArtifacts(
	timeout time.Duration, t test.Test, e exporter.Exporter,
) (func(), *bytes.Buffer) {
	// Register a named histogram to track the total time the bulk job took.
	// Roachperf uses this information to display information about this
	// roachtest.

	reg := histogram.NewRegistryWithExporter(
		timeout,
		histogram.MockWorkloadName,
		e,
	)
	reg.GetHandle().Get(t.Name())

	bytesBuf := bytes.NewBuffer([]byte{})
	writer := io.Writer(bytesBuf)

	e.Init(&writer)

	tick := func() {
		reg.Tick(func(tick histogram.Tick) {
			_ = tick.Exporter.SnapshotAndWrite(tick.Hist, tick.Now, tick.Elapsed, &tick.Name)
		})
	}

	return tick, bytesBuf
}

func registerBackup(r registry.Registry) {
	backup2TBSpec := r.MakeClusterSpec(10)
	r.Add(registry.TestSpec{
		Name:      fmt.Sprintf("backup/2TB/%s", backup2TBSpec),
		Owner:     registry.OwnerDisasterRecovery,
		Benchmark: true,
		Cluster:   backup2TBSpec,
		// The default storage on Azure Standard_D4ds_v5 is only 150 GiB compared
		// to 400 on GCE, which is not enough for this test. We could request a
		// larger volume size and set spec.LocalSSDDisable if we wanted to run
		// this on Azure.
		CompatibleClouds:          registry.OnlyGCE,
		Suites:                    registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
		EncryptionSupport:         registry.EncryptionAlwaysDisabled,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			rows := rows2TiB
			if c.IsLocal() {
				rows = 100
			}
			dest := importBankData(ctx, rows, t, c)
			exporter := roachtestutil.CreateWorkloadHistogramExporter(t, c)
			tick, perfBuf := initBulkJobPerfArtifacts(2*time.Hour, t, exporter)
			defer roachtestutil.CloseExporter(ctx, exporter, t, c, perfBuf, c.Node(1), "")

			m := c.NewMonitor(ctx)
			m.Go(func(ctx context.Context) error {
				t.Status(`running backup`)
				// Tick once before starting the backup, and once after to capture the
				// total elapsed time. This is used by roachperf to compute and display
				// the average MB/sec per node.
				tick()
				conn := c.Conn(ctx, t.L(), 1)
				defer conn.Close()
				var jobID jobspb.JobID
				uri := `gs://` + backupTestingBucket + `/` + dest + `?AUTH=implicit`
				if err := conn.QueryRowContext(ctx, fmt.Sprintf("BACKUP bank.bank INTO '%s' WITH detached", uri)).Scan(&jobID); err != nil {
					return err
				}
				if err := AssertReasonableFractionCompleted(ctx, t.L(), c, jobID, 2); err != nil {
					return err
				}
				tick()
				return nil
			})
			m.Wait()
		},
	})

	// Skip running on aws because the roachtest env does not have the proper
	// credentials. See 127062
	for _, cloudProvider := range []spec.Cloud{spec.GCE} {
		r.Add(registry.TestSpec{
			Name:                      fmt.Sprintf("backup/assume-role/%s", cloudProvider),
			Owner:                     registry.OwnerDisasterRecovery,
			Cluster:                   r.MakeClusterSpec(3),
			EncryptionSupport:         registry.EncryptionMetamorphic,
			Leases:                    registry.MetamorphicLeases,
			CompatibleClouds:          registry.Clouds(cloudProvider),
			Suites:                    registry.Suites(registry.Nightly),
			TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				rows := 100

				dest := importBankData(ctx, rows, t, c)

				var backupPath, kmsURI string
				var err error
				switch cloudProvider {
				case spec.AWS:
					if backupPath, err = getAWSBackupPath(dest); err != nil {
						t.Fatal(err)
					}
					if kmsURI, err = getAWSKMSAssumeRoleURI(); err != nil {
						t.Fatal(err)
					}
				case spec.GCE:
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

					table := "bank"
					originalBank, err := roachtestutil.Fingerprint(ctx, conn, "bank" /* db */, table)
					if err != nil {
						return err
					}
					restore, err := roachtestutil.Fingerprint(ctx, conn, "restoreDB" /* db */, table)
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
	for _, cloudProvider := range []spec.Cloud{spec.GCE, spec.AWS} {
		r.Add(registry.TestSpec{
			Name:                      fmt.Sprintf("backup/KMS/%s/%s", cloudProvider, KMSSpec.String()),
			Owner:                     registry.OwnerDisasterRecovery,
			Cluster:                   KMSSpec,
			EncryptionSupport:         registry.EncryptionMetamorphic,
			Leases:                    registry.MetamorphicLeases,
			CompatibleClouds:          registry.Clouds(cloudProvider),
			Suites:                    registry.Suites(registry.Nightly),
			TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
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
				backupPath := fmt.Sprintf("nodelocal://1/kmsbackup/%s/%s", cloudProvider, dest)

				m = c.NewMonitor(ctx)
				m.Go(func(ctx context.Context) error {
					switch cloudProvider {
					case spec.AWS:
						t.Status(`running encrypted backup with AWS KMS`)
						kmsURIA, err = getAWSKMSURI(KMSRegionAEnvVar, KMSKeyARNAEnvVar)
						if err != nil {
							return err
						}

						kmsURIB, err = getAWSKMSURI(KMSRegionBEnvVar, KMSKeyARNBEnvVar)
						if err != nil {
							return err
						}
					case spec.GCE:
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
					_, err := conn.ExecContext(ctx, `BACKUP bank.bank INTO '`+backupPath+`' WITH `+kmsOptions)
					return err
				})
				m.Wait()

				// Restore the encrypted BACKUP using each of KMS URI A and B separately.
				m = c.NewMonitor(ctx)
				m.Go(func(ctx context.Context) error {
					t.Status(`restore using KMSURIA`)
					if _, err := conn.ExecContext(ctx,
						`RESTORE TABLE bank.bank FROM LATEST IN $1 WITH into_db=restoreA, kms=$2`,
						backupPath, kmsURIA,
					); err != nil {
						return err
					}

					t.Status(`restore using KMSURIB`)
					if _, err := conn.ExecContext(ctx,
						`RESTORE TABLE bank.bank FROM LATEST IN $1 WITH into_db=restoreB, kms=$2`,
						backupPath, kmsURIB,
					); err != nil {
						return err
					}

					t.Status(`fingerprint`)
					table := "bank"
					originalBank, err := roachtestutil.Fingerprint(ctx, conn, "bank" /* db */, table)
					if err != nil {
						return err
					}
					restoreA, err := roachtestutil.Fingerprint(ctx, conn, "restoreA" /* db */, table)
					if err != nil {
						return err
					}
					restoreB, err := roachtestutil.Fingerprint(ctx, conn, "restoreB" /* db */, table)
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

	r.Add(registry.TestSpec{
		Name:              "backup/mvcc-range-tombstones",
		Owner:             registry.OwnerDisasterRecovery,
		Timeout:           4 * time.Hour,
		Cluster:           r.MakeClusterSpec(3, spec.CPU(8)),
		Leases:            registry.MetamorphicLeases,
		EncryptionSupport: registry.EncryptionMetamorphic,
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds:          registry.Clouds(spec.GCE, spec.Local),
		Suites:                    registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
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

	// short configures the test to only read from the first 3 tpch files, and conduct
	// 1 import rollback.
	short bool

	// debugSkipRollback configures the test to return after the first import,
	// skipping rollback steps.
	debugSkipRollback bool
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
		c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), install.MakeClusterSettings())
	}
	t.Status("starting csv servers")
	c.Run(ctx, option.WithNodes(c.All()), `./cockroach workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)

	// c2c tests still use the old multitenant API, which does not support non root authentication
	conn := c.Conn(ctx, t.L(), 1, option.VirtualClusterName(config.tenantName), option.AuthMode(install.AuthRootCert))

	// Configure cluster.
	t.Status("configuring cluster")
	_, err := conn.Exec(`SET CLUSTER SETTING server.debug.default_vmodule = 'txn=2,sst_batcher=4,revert=2'`)
	require.NoError(t, err)
	// Wait for ranges to upreplicate.
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

	// Create the orders table. It's about 16 GB across 8 files.
	t.Status("creating table")
	_, err = conn.Exec(`CREATE DATABASE tpch`)
	require.NoError(t, err)
	_, err = conn.Exec(`USE tpch`)
	require.NoError(t, err)
	createStmt, err := readCreateTableFromFixture(
		"gs://cockroach-fixtures-us-east1/tpch-csv/schema/orders.sql?AUTH=implicit", conn)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, createStmt)
	require.NoError(t, err)

	// Set up some helpers.
	waitForState := func(
		jobID string,
		exxpectedState jobs.State,
		expectStatus jobs.StatusMessage,
		duration time.Duration,
	) {
		ctx, cancel := context.WithTimeout(ctx, duration)
		defer cancel()
		require.NoError(t, retry.Options{}.Do(ctx, func(ctx context.Context) error {
			var statusMessage string
			var payloadBytes, progressBytes []byte
			require.NoError(t, conn.QueryRowContext(
				ctx, jobutils.InternalSystemJobsBaseQuery, jobID).
				Scan(&statusMessage, &payloadBytes, &progressBytes))
			if jobs.State(statusMessage) == jobs.StateFailed {
				var payload jobspb.Payload
				require.NoError(t, protoutil.Unmarshal(payloadBytes, &payload))
				t.Fatalf("job failed: %s", payload.Error)
			}
			if jobs.State(statusMessage) != exxpectedState {
				return errors.Errorf("expected job state %s, but got %s", exxpectedState, statusMessage)
			}
			if expectStatus != "" {
				var progress jobspb.Progress
				require.NoError(t, protoutil.Unmarshal(progressBytes, &progress))
				if jobs.StatusMessage(progress.StatusMessage) != expectStatus {
					return errors.Errorf("expected running status %s, but got %s",
						expectStatus, progress.StatusMessage)
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
		fp, err := roachtestutil.Fingerprint(ctx, conn, database, table)
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
		`gs://cockroach-fixtures-us-east1/tpch-csv/sf-100/orders.tbl.1?AUTH=implicit`,
		`gs://cockroach-fixtures-us-east1/tpch-csv/sf-100/orders.tbl.3?AUTH=implicit`,
		`gs://cockroach-fixtures-us-east1/tpch-csv/sf-100/orders.tbl.5?AUTH=implicit`,
		`gs://cockroach-fixtures-us-east1/tpch-csv/sf-100/orders.tbl.7?AUTH=implicit`,
	}
	if config.short {
		files = files[:2]
	}
	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		`IMPORT INTO orders CSV DATA ('%s') WITH delimiter='|'`, strings.Join(files, "', '")))
	require.NoError(t, err)

	if config.debugSkipRollback {
		return
	}
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
		`gs://cockroach-fixtures-us-east1/tpch-csv/sf-100/orders.tbl.2?AUTH=implicit`,
		`gs://cockroach-fixtures-us-east1/tpch-csv/sf-100/orders.tbl.4?AUTH=implicit`,
		`gs://cockroach-fixtures-us-east1/tpch-csv/sf-100/orders.tbl.6?AUTH=implicit`,
		`gs://cockroach-fixtures-us-east1/tpch-csv/sf-100/orders.tbl.8?AUTH=implicit`,
	}
	if config.short {
		files = files[:1]
	}

	_, err = conn.ExecContext(ctx,
		`SET CLUSTER SETTING jobs.debug.pausepoints = 'import.after_ingest'`)
	require.NoError(t, err)

	var jobID string
	for i := 0; i < 2; i++ {
		if i > 0 && config.short {
			t.L().Printf("skipping import rollback")
			continue
		}
		t.Status("importing even-numbered files")
		require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
			`IMPORT INTO orders CSV DATA ('%s') WITH delimiter='|', detached`,
			strings.Join(files, "', '")),
		).Scan(&jobID))
		waitForState(jobID, jobs.StatePaused, "", 30*time.Minute)

		t.Status("canceling import")
		_, err = conn.ExecContext(ctx, fmt.Sprintf(`CANCEL JOB %s`, jobID))
		require.NoError(t, err)
		waitForState(jobID, jobs.StateCanceled, "", 30*time.Minute)
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
	waitForState(jobID, jobs.StateRunning, sql.StatusWaitingForMVCCGC, 2*time.Minute)

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
	uri := fmt.Sprintf("gs://"+backupTestingBucket+"/gcs/%s?%s", dest, q.Encode())

	return uri, nil
}

func getAWSBackupPath(dest string) (string, error) {
	q := make(url.Values)
	expect := map[string]string{
		AssumeRoleAWSKeyIDEnvVar:     amazon.AWSAccessKeyParam,
		AssumeRoleAWSSecretKeyEnvVar: amazon.AWSSecretParam,
		AssumeRoleAWSRoleEnvVar:      amazon.AssumeRoleParam,
		KMSRegionAEnvVar:             amazon.S3RegionParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			return "", errors.Errorf("env variable %s must be present to run the KMS test", env)
		}
		q.Add(param, v)
	}
	q.Add(cloudstorage.AuthParam, cloudstorage.AuthParamSpecified)

	return fmt.Sprintf("s3://"+backupTestingBucket+"/%s?%s", dest, q.Encode()), nil
}
