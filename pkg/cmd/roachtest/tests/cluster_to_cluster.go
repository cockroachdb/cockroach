// Copyright 2022 The Cockroach Authors.
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
	gosql "database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/local"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func registerClusterToCluster(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:            "c2c/kv0",
		Owner:           registry.OwnerDisasterRecovery,
		Cluster:         r.MakeClusterSpec(7),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.Cockroach(), "./cockroach")
			srcCluster := c.Range(1, 3)
			dstCluster := c.Range(4, 6)
			srcTenantNode := 7
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(srcTenantNode))

			srcStartOps := option.DefaultStartOpts()
			srcStartOps.RoachprodOpts.InitTarget = 1
			srcClusterSetting := install.MakeClusterSettings(install.SecureOption(true))
			c.Start(ctx, t.L(), srcStartOps, srcClusterSetting, srcCluster)

			dstStartOps := option.DefaultStartOpts()
			dstStartOps.RoachprodOpts.InitTarget = 4
			dstClusterSetting := install.MakeClusterSettings(install.SecureOption(true))
			c.Start(ctx, t.L(), dstStartOps, dstClusterSetting, dstCluster)

			srcNode := srcCluster.RandNode()
			destNode := dstCluster.RandNode()

			addr, err := c.ExternalPGUrl(ctx, t.L(), srcNode)
			require.NoError(t, err)

			srcSQL := sqlutils.MakeSQLRunner(c.Conn(ctx, t.L(), srcNode[0]))
			destDB := c.Conn(ctx, t.L(), destNode[0])
			destSQL := sqlutils.MakeSQLRunner(destDB)

			srcClusterSettings(t, srcSQL)
			destClusterSettings(t, destSQL)

			srcTenantID, destTenantID := 10, 20
			srcSQL.Exec(t, `SELECT crdb_internal.create_tenant($1::int)`, srcTenantID)
			pgURL, cleanup, err := copyPGCertsAndMakeURL(ctx, t, c, srcNode, dstCluster, srcClusterSetting.PGUrlCertsDir, addr[0])
			defer cleanup()
			require.NoError(t, err)

			const (
				tenantHTTPPort = 8081
				tenantSQLPort  = 30258
			)
			t.Status("creating tenant node")
			srcTenant := createTenantNode(ctx, t, c, srcCluster, srcTenantID, srcTenantNode, tenantHTTPPort, tenantSQLPort)
			srcTenant.start(ctx, t, c, "./cockroach")
			defer srcTenant.stop(ctx, t, c)

			var (
				kvWorkloadDuration = "15m"
				kvWorkloadReadPerc = 0
			)
			m := c.NewMonitor(ctx, c.Range(1, 6))
			workloadDoneCh := make(chan struct{})
			m.Go(func(ctx context.Context) error {
				defer close(workloadDoneCh)
				cmd := fmt.Sprintf("./workload run kv --tolerate-errors --init --duration %s --read-percent %d '%s'",
					kvWorkloadDuration,
					kvWorkloadReadPerc,
					srcTenant.secureURL())
				c.Run(ctx, c.Node(srcTenantNode), cmd)
				return nil
			})

			t.Status("starting replication stream")
			streamReplStmt := fmt.Sprintf("RESTORE TENANT %d FROM REPLICATION STREAM FROM '%s' AS TENANT %d", srcTenantID, pgURL, destTenantID)
			var ingestionJobID, streamProducerJobID int
			destSQL.QueryRow(t, streamReplStmt).Scan(&ingestionJobID, &streamProducerJobID)

			// TODO(ssd): The job doesn't record the initial
			// statement time, so we can't correctly measure the
			// initial scan time here.
			lv := makeLatencyVerifier("stream-ingestion", 0, 2*time.Minute, t.L(), getStreamIngestionJobInfo, t.Status, false)
			defer lv.maybeLogLatencyHist()

			m.Go(func(ctx context.Context) error {
				return lv.pollLatency(ctx, destDB, ingestionJobID, time.Second, workloadDoneCh)
			})

			t.Status("waiting for replication stream to return high watermark")
			waitForHighWatermark(t, destDB, ingestionJobID)
			m.Wait()

			t.Status("waiting for replication stream to cutover")
			cutoverTime := stopReplicationStream(t, destSQL, ingestionJobID)
			compareTenantFingerprintsAtTimestamp(t, srcSQL, destSQL, srcTenantID, destTenantID, hlc.Timestamp{WallTime: cutoverTime.UnixNano()})
			lv.assertValid(t)
		},
	})
}

func compareTenantFingerprintsAtTimestamp(
	t test.Test, srcSQL, destSQL *sqlutils.SQLRunner, srcTenantID, destTenantID int, ts hlc.Timestamp,
) {
	fingerprintQuery := fmt.Sprintf(`
SELECT
    xor_agg(
        fnv64(crdb_internal.trim_tenant_prefix(key),
              substring(value from 5))
    ) AS fingerprint
FROM crdb_internal.scan(crdb_internal.tenant_span($1))
AS OF SYSTEM TIME '%s'`, ts.AsOfSystemTime())

	var srcFingerprint int64
	srcSQL.QueryRow(t, fingerprintQuery, srcTenantID).Scan(&srcFingerprint)

	var destFingerprint int64
	destSQL.QueryRow(t, fingerprintQuery, destTenantID).Scan(&destFingerprint)

	require.Equal(t, srcFingerprint, destFingerprint)
}

type streamIngesitonJobInfo struct {
	status        string
	errMsg        string
	highwaterTime time.Time
}

func (c *streamIngesitonJobInfo) GetHighWater() time.Time { return c.highwaterTime }
func (c *streamIngesitonJobInfo) GetStatus() string       { return c.status }
func (c *streamIngesitonJobInfo) GetError() string        { return c.status }

var _ jobInfo = (*streamIngesitonJobInfo)(nil)

func getStreamIngestionJobInfo(db *gosql.DB, jobID int) (jobInfo, error) {
	var status string
	var payloadBytes []byte
	var progressBytes []byte
	if err := db.QueryRow(
		`SELECT status, payload, progress FROM system.jobs WHERE id = $1`, jobID,
	).Scan(&status, &payloadBytes, &progressBytes); err != nil {
		return nil, err
	}
	var payload jobspb.Payload
	if err := protoutil.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, err
	}
	var progress jobspb.Progress
	if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
		return nil, err
	}
	var highwaterTime time.Time
	highwater := progress.GetHighWater()
	if highwater != nil {
		highwaterTime = highwater.GoTime()
	}
	return &streamIngesitonJobInfo{
		status:        status,
		errMsg:        payload.Error,
		highwaterTime: highwaterTime,
	}, nil
}

func waitForHighWatermark(t test.Test, db *gosql.DB, ingestionJobID int) {
	testutils.SucceedsWithin(t, func() error {
		info, err := getStreamIngestionJobInfo(db, ingestionJobID)
		if err != nil {
			return err
		}
		if info.GetHighWater().IsZero() {
			return errors.New("no high watermark")
		}
		return nil
	}, 5*time.Minute)
}

func stopReplicationStream(t test.Test, destSQL *sqlutils.SQLRunner, ingestionJob int) time.Time {
	// Cut over the ingestion job and the job will stop eventually.
	var cutoverTime time.Time
	destSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)
	destSQL.Exec(t, `SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`, ingestionJob, cutoverTime)
	err := retry.ForDuration(time.Minute*5, func() error {
		var status string
		var payloadBytes []byte
		destSQL.QueryRow(t, `SELECT status, payload FROM system.jobs WHERE id = $1`, ingestionJob).Scan(&status, &payloadBytes)
		if jobs.Status(status) == jobs.StatusFailed {
			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				t.Fatalf("job failed: %s", payload.Error)
			}
			t.Fatalf("job failed")
		}
		if e, a := jobs.StatusSucceeded, jobs.Status(status); e != a {
			return errors.Errorf("expected job status %s, but got %s", e, a)
		}
		return nil
	})
	require.NoError(t, err)
	return cutoverTime
}

func srcClusterSettings(t test.Test, db *sqlutils.SQLRunner) {
	db.ExecMultiple(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)
}

func destClusterSettings(t test.Test, db *sqlutils.SQLRunner) {
	db.ExecMultiple(t, `SET enable_experimental_stream_replication = true;`)
}

func copyPGCertsAndMakeURL(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	srcNode option.NodeListOption,
	destNode option.NodeListOption,
	pgURLDir string,
	urlString string,
) (string, func(), error) {
	cleanup := func() {}
	pgURL, err := url.Parse(urlString)
	if err != nil {
		return "", cleanup, err
	}

	tmpDir, err := os.MkdirTemp("", "certs")
	if err != nil {
		return "", cleanup, err
	}
	cleanup = func() { _ = os.RemoveAll(tmpDir) }

	if err := c.Get(ctx, t.L(), pgURLDir, tmpDir, srcNode); err != nil {
		return "", cleanup, err
	}
	if err := c.PutE(ctx, t.L(), tmpDir, "./src-cluster-certs", destNode); err != nil {
		return "", cleanup, err
	}

	dir := "."
	if c.IsLocal() {
		dir = local.VMDir(c.Name(), destNode[0])
	}
	options := pgURL.Query()
	options.Set("sslmode", "verify-full")
	options.Set("sslrootcert", filepath.Join(dir, "src-cluster-certs", "ca.crt"))
	options.Set("sslcert", filepath.Join(dir, "src-cluster-certs", "client.root.crt"))
	options.Set("sslkey", filepath.Join(dir, "src-cluster-certs", "client.root.key"))
	pgURL.RawQuery = options.Encode()
	return pgURL.String(), cleanup, err
}
