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
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
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
)

func registerBackup(r *testRegistry) {
	importBankData := func(ctx context.Context, rows int, t *test, c *cluster) string {
		dest := c.name
		// Randomize starting with encryption-at-rest enabled.
		c.encryptAtRandom = true

		if local {
			rows = 100
			dest += fmt.Sprintf("%d", timeutil.Now().UnixNano())
		}

		c.Put(ctx, workload, "./workload")
		c.Put(ctx, cockroach, "./cockroach")

		// NB: starting the cluster creates the logs dir as a side effect,
		// needed below.
		c.Start(ctx, t)
		c.Run(ctx, c.All(), `./workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)
		time.Sleep(time.Second) // wait for csv server to open listener

		importArgs := []string{
			"./workload", "fixtures", "import", "bank",
			"--db=bank", "--payload-bytes=10240", "--ranges=0", "--csv-server", "http://localhost:8081",
			fmt.Sprintf("--rows=%d", rows), "--seed=1", "{pgurl:1}",
		}
		c.Run(ctx, c.Node(1), importArgs...)

		return dest
	}

	backup2TBSpec := makeClusterSpec(10)
	r.Add(testSpec{
		Name:       fmt.Sprintf("backup/2TB/%s", backup2TBSpec),
		Owner:      OwnerBulkIO,
		Cluster:    backup2TBSpec,
		MinVersion: "v2.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			rows := 65104166
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
			rows := 976562
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
