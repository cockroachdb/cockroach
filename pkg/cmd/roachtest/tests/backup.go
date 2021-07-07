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
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	cloudstorage "github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
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

func importBankDataSplit(
	ctx context.Context, rows, ranges int, t test.Test, c cluster.Cluster,
) string {
	dest := c.Name()
	// Randomize starting with encryption-at-rest enabled.
	c.EncryptAtRandom(true)

	if c.IsLocal() {
		dest += fmt.Sprintf("%d", timeutil.Now().UnixNano())
	}

	c.Put(ctx, t.DeprecatedWorkload(), "./workload")
	c.Put(ctx, t.Cockroach(), "./cockroach")

	// NB: starting the cluster creates the logs dir as a side effect,
	// needed below.
	c.Start(ctx)
	c.Run(ctx, c.All(), `./workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)
	time.Sleep(time.Second) // wait for csv server to open listener

	importArgs := []string{
		"./workload", "fixtures", "import", "bank",
		"--db=bank", "--payload-bytes=10240", fmt.Sprintf("--ranges=%d", ranges), "--csv-server", "http://localhost:8081",
		fmt.Sprintf("--rows=%d", rows), "--seed=1", "{pgurl:1}",
	}
	c.Run(ctx, c.Node(1), importArgs...)

	return dest
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
		Name:    fmt.Sprintf("backup/nodeShutdown/worker/%s", backupNodeRestartSpec),
		Owner:   registry.OwnerBulkIO,
		Cluster: backupNodeRestartSpec,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := 2
			nodeToShutdown := 3
			dest := loadBackupData(ctx, t, c)
			backupQuery := `BACKUP bank.bank TO 'nodelocal://1/` + dest + `' WITH DETACHED`
			startBackup := func(c cluster.Cluster) (jobID string, err error) {
				gatewayDB := c.Conn(ctx, gatewayNode)
				defer gatewayDB.Close()

				err = gatewayDB.QueryRowContext(ctx, backupQuery).Scan(&jobID)
				return
			}

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, startBackup)
		},
	})
	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("backup/nodeShutdown/coordinator/%s", backupNodeRestartSpec),
		Owner:   registry.OwnerBulkIO,
		Cluster: backupNodeRestartSpec,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := 2
			nodeToShutdown := 2
			dest := loadBackupData(ctx, t, c)
			backupQuery := `BACKUP bank.bank TO 'nodelocal://1/` + dest + `' WITH DETACHED`
			startBackup := func(c cluster.Cluster) (jobID string, err error) {
				gatewayDB := c.Conn(ctx, gatewayNode)
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
	ctx context.Context, t test.Test, testName string, timeout time.Duration,
) func() {
	// Register a named histogram to track the total time the bulk job took.
	// Roachperf uses this information to display information about this
	// roachtest.
	reg := histogram.NewRegistry(
		timeout,
		histogram.MockWorkloadName,
	)
	reg.GetHandle().Get(testName)

	// Create the stats file where the roachtest will write perf artifacts.
	// We probably don't want to fail the roachtest if we are unable to
	// collect perf stats.
	statsFile := t.PerfArtifactsDir() + "/stats.json"
	err := os.MkdirAll(filepath.Dir(statsFile), 0755)
	if err != nil {
		log.Errorf(ctx, "%s failed to create perf artifacts directory %s: %s", testName,
			statsFile, err.Error())
	}
	jsonF, err := os.Create(statsFile)
	if err != nil {
		log.Errorf(ctx, "%s failed to create perf artifacts directory %s: %s", testName,
			statsFile, err.Error())
	}
	jsonEnc := json.NewEncoder(jsonF)
	tick := func() {
		reg.Tick(func(tick histogram.Tick) {
			_ = jsonEnc.Encode(tick.Snapshot())
		})
	}
	return tick
}

func registerBackup(r registry.Registry) {

	backup2TBSpec := r.MakeClusterSpec(10)
	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("backup/2TB/%s", backup2TBSpec),
		Owner:   registry.OwnerBulkIO,
		Cluster: backup2TBSpec,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			rows := rows2TiB
			if c.IsLocal() {
				rows = 100
			}
			dest := importBankData(ctx, rows, t, c)
			tick := initBulkJobPerfArtifacts(ctx, t, "backup/2TB", 2*time.Hour)

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
				if err := c.PutE(ctx, t.L(), t.PerfArtifactsDir(), t.PerfArtifactsDir(), c.Node(1)); err != nil {
					log.Errorf(ctx, "failed to upload perf artifacts to node: %s", err.Error())
				}
				return nil
			})
			m.Wait()
		},
	})

	KMSSpec := r.MakeClusterSpec(3)
	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("backup/KMS/%s", KMSSpec.String()),
		Owner:   registry.OwnerBulkIO,
		Cluster: KMSSpec,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().Cloud == spec.GCE {
				t.Skip("backupKMS roachtest is only configured to run on AWS", "")
			}

			// ~10GiB - which is 30Gib replicated.
			rows := rows30GiB
			if c.IsLocal() {
				rows = 100
			}
			dest := importBankData(ctx, rows, t, c)

			conn := c.Conn(ctx, 1)
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
			m = c.NewMonitor(ctx)
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
			m = c.NewMonitor(ctx)
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
	r.Add(registry.TestSpec{
		Name:    `backupTPCC`,
		Owner:   registry.OwnerBulkIO,
		Cluster: r.MakeClusterSpec(3),
		Timeout: 1 * time.Hour,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// Randomize starting with encryption-at-rest enabled.
			c.EncryptAtRandom(true)
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Put(ctx, t.DeprecatedWorkload(), "./workload")
			c.Start(ctx)
			conn := c.Conn(ctx, 1)

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

	// Set AUTH to implicit
	q.Add(cloudstorage.AuthParam, cloudstorage.AuthParamSpecified)
	correctURI := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())

	return correctURI, nil
}
