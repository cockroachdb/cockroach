// Copyright 2021 The Cockroach Authors.
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
	gosql "database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func startTenantSQLServer(
	ctx context.Context,
	t *test,
	c *cluster,
	node int,
	tenantID int,
	kvAddrs []string,
	httpPort, tenantPort string,
) string {
	tenantCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.RunE(tenantCtx, c.Node(node),
			"./cockroach", "mt", "start-sql",
			// TODO(tbg): make this test secure.
			// "--certs-dir", "certs",
			"--insecure",
			"--tenant-id", strconv.Itoa(tenantID),
			"--http-addr", "127.0.0.1:"+httpPort,
			"--kv-addrs", strings.Join(kvAddrs, ","),
			// Don't bind to external interfaces when running locally.
			"--sql-addr", ifLocal("127.0.0.1", "0.0.0.0")+":"+tenantPort,
		)
		close(errCh)
	}()
	u, err := url.Parse(c.ExternalPGUrl(ctx, c.Node(node))[0])
	if err != nil {
		t.Fatal(err)
	}
	u.Host = c.ExternalIP(ctx, c.Node(node))[0] + ":" + tenantPort
	url := u.String()
	c.l.Printf("sql server should be running at %s", url)

	time.Sleep(time.Second)

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	return url
}

func initBankData(ctx context.Context, rows int, node int, c *cluster, pgurl string) {
	if local {
		rows = 100
	}

	importArgs := []string{
		"./workload", "init", "bank",
		"--db=defaultdb", "--payload-bytes=10240", "--ranges=0", "--data-loader=insert",
		fmt.Sprintf("--rows=%d", rows), "--seed=1", pgurl,
	}
	c.Run(ctx, c.Node(node), importArgs...)
}

func registerStreaming(r *testRegistry) {
	r.Add(testSpec{
		Name:       "tenantToTenantStreaming",
		Owner:      OwnerBulkIO,
		Timeout:    1 * time.Hour,
		Cluster:    makeClusterSpec(3),
		MinVersion: "v21.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			const sourceNode = 1
			const destNode = 2
			const workloadNode = 3
			const tenantID = 123
			c.Put(ctx, cockroach, "./cockroach", c.Nodes(sourceNode, destNode))
			c.Put(ctx, workload, "./workload")

			t.Status(`starting source kv server`)
			sourceSystemPGUrl := c.ExternalPGUrl(ctx, c.Node(sourceNode))[0]
			c.Run(ctx, c.Node(sourceNode),
				`./cockroach start-single-node --insecure --http-addr :8081 --background`)
			sourceKVAddrs := c.ExternalAddr(ctx, c.Node(sourceNode))

			t.Status(`starting destination kv server`)
			c.Run(ctx, c.Node(destNode),
				`./cockroach start-single-node --insecure --http-addr :8082 --background --listen-addr :26259`)
			destKVAddrs := c.ExternalAddr(ctx, c.Node(destNode))

			// Create and start a tenant in the source cluster.
			t.Status(`creating tenant on source cluster`)
			_, err := c.Conn(ctx, sourceNode).Exec(`SELECT crdb_internal.create_tenant($1)`, tenantID)
			require.NoError(t, err)

			sourceTenantUrl := startTenantSQLServer(ctx, t, c, sourceNode, tenantID, sourceKVAddrs, "8083",
				"36257")
			t.Status("checking that a client can connect to the source tenant server")
			sourceTenantDB, err := gosql.Open("postgres", sourceTenantUrl)
			require.NoError(t, err)
			defer sourceTenantDB.Close()

			// Init bank data into the tenant.
			t.Status(`initializing bank data in the tenant`)
			rows := 10000
			initBankData(ctx, rows, sourceNode, c, sourceTenantUrl)

			sourceSystemDB := c.Conn(ctx, sourceNode)
			destSystemDB := c.Conn(ctx, destNode)

			// Since we are not using `roachprod start` as explained above, we have to
			// explicitly set appropriate cluster settings.
			setClusterSettings := func(db *gosql.DB) {
				license := envutil.EnvOrDefaultString("COCKROACH_DEV_LICENSE", "")
				if license == "" {
					fmt.Printf("%s: COCKROACH_DEV_LICENSE unset: enterprise features will be unavailable\n",
						c.name)
				}
				_, err := db.Exec(fmt.Sprintf(`
SET CLUSTER SETTING server.remote_debugging.mode = 'any';
SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing';
SET CLUSTER SETTING enterprise.license = '%s';
`, license))
				require.NoError(t, err)
			}
			setClusterSettings(sourceSystemDB)
			setClusterSettings(destSystemDB)

			backupDestination := "gs://cockroachdb-backup-testing/" + c.name + `?AUTH=implicit`

			// Backup the source tenant.
			m := newMonitor(ctx, c, c.Node(sourceNode))
			m.Go(func(ctx context.Context) error {
				t.Status(`running backup on source cluster`)
				_, err := sourceSystemDB.Exec(fmt.Sprintf(`BACKUP TENANT 123 TO '%s'`, backupDestination))
				require.NoError(t, err)
				return nil
			})
			m.Wait()

			// Save the time until which the data is covered by the backup.
			var asOf string
			err = destSystemDB.QueryRow(fmt.Sprintf(`SELECT end_time from [SHOW BACKUP '%s']`,
				backupDestination)).Scan(&asOf)
			require.NoError(t, err)

			// Restore the backup into the destination cluster to achieve a consistent
			// view of the tenant as of the above time.
			m = newMonitor(ctx, c, c.Node(destNode))
			m.Go(func(ctx context.Context) error {
				t.Status(`restoring tenant on destination cluster`)
				_, err := destSystemDB.Exec(fmt.Sprintf(
					`RESTORE TENANT $1 FROM '%s' AS OF SYSTEM TIME '%s'`, backupDestination, asOf),
					tenantID)
				require.NoError(t, err)
				return nil
			})
			m.Wait()

			// Start the ingestion job on the destination cluster.
			var ingestionJobID int
			m = newMonitor(ctx, c, c.Node(destNode))
			m.Go(func(ctx context.Context) error {
				t.Status(`running restore from replication stream`)
				_, err := destSystemDB.Exec(`
SET CLUSTER SETTING bulkio.stream_ingestion.minimum_flush_interval = '5us';
SET CLUSTER SETTING bulkio.stream_ingestion.cutover_signal_poll_interval = '100ms';
SET enable_experimental_stream_replication = true;
`)
				require.NoError(t, err)
				err = destSystemDB.QueryRow(fmt.Sprintf(
					`RESTORE TENANT $1 FROM REPLICATION STREAM FROM $2 AS OF SYSTEM TIME '%s'`, asOf),
					tenantID, sourceSystemPGUrl).Scan(&ingestionJobID)
				require.NoError(t, err)
				return nil
			})
			m.Wait()

			// Start a workload on the source tenant, so as to test that
			// changes are streamed and ingested by the above job.
			t.Status(`run bank workload`)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			duration := 1 * time.Minute
			cmdDone := make(chan error)
			go func() {
				cmd := fmt.Sprintf("./workload run bank '%s' --db=defaultdb", sourceTenantUrl)
				cmdDone <- c.RunE(ctx, c.Node(workloadNode), cmd)
			}()

			select {
			case <-time.After(duration):
			case <-ctx.Done():
				return
			}

			cutoverTime := timeutil.Now().Round(time.Microsecond)
			t.Status(`waiting for job to hit cutover time`)
			err = testutils.SucceedsSoonError(func() error {
				progress := &jobspb.Progress{}
				var buf []byte
				err := destSystemDB.QueryRow(`SELECT progress FROM system.jobs WHERE id = $1`,
					ingestionJobID).Scan(&buf)
				require.NoError(t, err)
				err = protoutil.Unmarshal(buf, progress)
				require.NoError(t, err)
				if progress.GetHighWater() == nil {
					return errors.Newf("stream ingestion has not recorded any progress yet, waiting to advance pos %s",
						cutoverTime.String())
				}
				highwater := timeutil.Unix(0, progress.GetHighWater().WallTime)
				if highwater.Before(cutoverTime) {
					return errors.Newf("waiting for stream ingestion job progress %s to advance beyond %s",
						highwater.String(), cutoverTime.String())
				}
				return nil
			})
			require.NoError(t, err)

			t.Status(`signaling cutover`)
			_, err = destSystemDB.Exec(`SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`,
				ingestionJobID, cutoverTime)
			require.NoError(t, err)

			// Wait for the ingestion job to revert to a consistent state.
			t.Status(`waiting for ingestion job to complete`)
			for {
				q := "SHOW JOBS WHEN COMPLETE $1;"
				_, err = destSystemDB.ExecContext(ctx, q, ingestionJobID)
				if testutils.IsError(err, "pq: restart transaction:.*") {
					t.l.Printf("SHOW JOBS WHEN COMPLETE returned %s, retrying", err.Error())
					time.Sleep(10 * time.Second)
					continue
				}
				break
			}

			select {
			case err := <-cmdDone:
				// Workload exited before it should have.
				require.NoError(t, err)
			default:
			}

			// Start the destination tenant.
			destTenantURL := startTenantSQLServer(ctx, t, c, destNode, tenantID, destKVAddrs, "8084",
				"36259")
			t.Status("checking that a client can connect to the destination tenant server")
			destTenantDB, err := gosql.Open("postgres", destTenantURL)
			require.NoError(t, err)
			defer destTenantDB.Close()

			// Fingerprint the source tenant and the destination tenant as of the
			// cutover time.
			sourceTenantFingerprint, err := fingerprint(ctx, sourceTenantDB, "defaultdb",
				fmt.Sprint(cutoverTime.UnixNano()))
			require.NoError(t, err)
			destTenantFingerprint, err := fingerprint(ctx, destTenantDB, "defaultdb",
				fmt.Sprint(cutoverTime.UnixNano()))
			require.NoError(t, err)
			require.Equal(t, sourceTenantFingerprint, destTenantFingerprint)
		},
	})
}
