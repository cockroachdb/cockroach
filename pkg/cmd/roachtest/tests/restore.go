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
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// HealthChecker runs a regular check that verifies that a specified subset
// of (CockroachDB) nodes look "very healthy". That is, there are no stuck
// proposals, liveness problems, or whatever else might get added in the
// future.
type HealthChecker struct {
	t      test.Test
	c      cluster.Cluster
	nodes  option.NodeListOption
	doneCh chan struct{}
}

// NewHealthChecker returns a populated HealthChecker.
func NewHealthChecker(t test.Test, c cluster.Cluster, nodes option.NodeListOption) *HealthChecker {
	return &HealthChecker{
		t:      t,
		c:      c,
		nodes:  nodes,
		doneCh: make(chan struct{}),
	}
}

// Done signals the HealthChecker's Runner to shut down.
func (hc *HealthChecker) Done() {
	close(hc.doneCh)
}

type gossipAlert struct {
	NodeID, StoreID       int
	Category, Description string
	Value                 float64
}

type gossipAlerts []gossipAlert

func (g gossipAlerts) String() string {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)

	for _, a := range g {
		fmt.Fprintf(tw, "n%d/s%d\t%.2f\t%s\t%s\n", a.NodeID, a.StoreID, a.Value, a.Category, a.Description)
	}
	_ = tw.Flush()
	return buf.String()
}

// Runner makes sure the gossip_alerts table is empty at all times.
//
// TODO(tschottdorf): actually let this fail the test instead of logging complaints.
func (hc *HealthChecker) Runner(ctx context.Context) (err error) {
	logger, err := hc.t.L().ChildLogger("health")
	if err != nil {
		return err
	}
	defer func() {
		logger.Printf("health check terminated with %v\n", err)
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hc.doneCh:
			return nil
		case <-ticker.C:
		}

		tBegin := timeutil.Now()

		nodeIdx := 1 + rand.Intn(len(hc.nodes))
		db, err := hc.c.ConnE(ctx, hc.t.L(), nodeIdx)
		if err != nil {
			return err
		}
		// TODO(tschottdorf): remove replicate queue failures when the cluster first starts.
		// Ditto queue.raftsnapshot.process.failure.
		_, err = db.Exec(`USE system`)
		if err != nil {
			return err
		}
		rows, err := db.QueryContext(ctx, `SELECT * FROM crdb_internal.gossip_alerts ORDER BY node_id ASC, store_id ASC `)
		_ = db.Close()
		if err != nil {
			return err
		}
		var rr gossipAlerts
		for rows.Next() {
			a := gossipAlert{StoreID: -1}
			var storeID gosql.NullInt64
			if err := rows.Scan(&a.NodeID, &storeID, &a.Category, &a.Description, &a.Value); err != nil {
				return err
			}
			if storeID.Valid {
				a.StoreID = int(storeID.Int64)
			}
			rr = append(rr, a)
		}
		if len(rr) > 0 {
			logger.Printf(rr.String() + "\n")
			// TODO(tschottdorf): see method comment.
			// return errors.New(rr.String())
		}

		if elapsed := timeutil.Since(tBegin); elapsed > 10*time.Second {
			err := errors.Errorf("health check against node %d took %s", nodeIdx, elapsed)
			logger.Printf("%+v", err)
			// TODO(tschottdorf): see method comment.
			// return err
		}
	}
}

// DiskUsageLogger regularly logs the disk spaced used by the nodes in the cluster.
type DiskUsageLogger struct {
	t      test.Test
	c      cluster.Cluster
	doneCh chan struct{}
}

// NewDiskUsageLogger populates a DiskUsageLogger.
func NewDiskUsageLogger(t test.Test, c cluster.Cluster) *DiskUsageLogger {
	return &DiskUsageLogger{
		t:      t,
		c:      c,
		doneCh: make(chan struct{}),
	}
}

// Done instructs the Runner to terminate.
func (dul *DiskUsageLogger) Done() {
	close(dul.doneCh)
}

// Runner runs in a loop until Done() is called and prints the cluster-wide per
// node disk usage in descending order.
func (dul *DiskUsageLogger) Runner(ctx context.Context) error {
	l, err := dul.t.L().ChildLogger("diskusage")
	if err != nil {
		return err
	}
	quietLogger, err := dul.t.L().ChildLogger("diskusage-exec", logger.QuietStdout, logger.QuietStderr)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-dul.doneCh:
			return nil
		case <-ticker.C:
		}

		type usage struct {
			nodeNum int
			bytes   int
		}

		var bytesUsed []usage
		for i := 1; i <= dul.c.Spec().NodeCount; i++ {
			cur, err := getDiskUsageInBytes(ctx, dul.c, quietLogger, i)
			if err != nil {
				// This can trigger spuriously as compactions remove files out from under `du`.
				l.Printf("%s", errors.Wrapf(err, "node #%d", i))
				cur = -1
			}
			bytesUsed = append(bytesUsed, usage{
				nodeNum: i,
				bytes:   cur,
			})
		}
		sort.Slice(bytesUsed, func(i, j int) bool { return bytesUsed[i].bytes > bytesUsed[j].bytes }) // descending

		var s []string
		for _, usage := range bytesUsed {
			s = append(s, fmt.Sprintf("n#%d: %s", usage.nodeNum, humanizeutil.IBytes(int64(usage.bytes))))
		}

		l.Printf("%s\n", strings.Join(s, ", "))
	}
}
func registerRestoreNodeShutdown(r registry.Registry) {
	makeRestoreStarter := func(ctx context.Context, t test.Test, c cluster.Cluster, gatewayNode int) jobStarter {
		return func(c cluster.Cluster, t test.Test) (string, error) {
			t.L().Printf("connecting to gateway")
			gatewayDB := c.Conn(ctx, t.L(), gatewayNode)
			defer gatewayDB.Close()

			t.L().Printf("creating bank database")
			if _, err := gatewayDB.Exec("CREATE DATABASE bank"); err != nil {
				return "", err
			}

			errCh := make(chan error, 1)
			go func() {
				defer close(errCh)

				// 10 GiB restore.
				restoreQuery := `RESTORE bank.bank FROM
					'gs://cockroach-fixtures/workload/bank/version=1.0.0,payload-bytes=100,ranges=10,rows=10000000,seed=1/bank?AUTH=implicit'`

				t.L().Printf("starting to run the restore job")
				if _, err := gatewayDB.Exec(restoreQuery); err != nil {
					errCh <- err
				}
				t.L().Printf("done running restore job")
			}()

			// Wait for the job.
			retryOpts := retry.Options{
				MaxRetries:     50,
				InitialBackoff: 1 * time.Second,
				MaxBackoff:     5 * time.Second,
			}
			for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
				var jobCount int
				if err := gatewayDB.QueryRowContext(ctx, "SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'RESTORE'").Scan(&jobCount); err != nil {
					return "", err
				}

				select {
				case err := <-errCh:
					// We got an error when starting the job.
					return "", err
				default:
				}

				if jobCount == 0 {
					t.L().Printf("waiting for restore job")
				} else if jobCount == 1 {
					t.L().Printf("found restore job")
					break
				} else {
					t.L().Printf("found multiple restore jobs -- erroring")
					return "", errors.New("unexpectedly found multiple restore jobs")
				}
			}

			var jobID string
			if err := gatewayDB.QueryRowContext(ctx, "SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'RESTORE'").Scan(&jobID); err != nil {
				return "", errors.Wrap(err, "querying the job ID")
			}
			return jobID, nil
		}
	}

	r.Add(registry.TestSpec{
		Name:    "restore/nodeShutdown/worker",
		Owner:   registry.OwnerDisasterRecovery,
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := 2
			nodeToShutdown := 3
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), install.MakeClusterSettings())

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, makeRestoreStarter(ctx, t, c, gatewayNode))
		},
	})

	r.Add(registry.TestSpec{
		Name:    "restore/nodeShutdown/coordinator",
		Owner:   registry.OwnerDisasterRecovery,
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := 2
			nodeToShutdown := 2
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), install.MakeClusterSettings())

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, makeRestoreStarter(ctx, t, c, gatewayNode))
		},
	})
}

func registerRestore(r registry.Registry) {
	// TODO(msbutler): delete the tests created by the loop below. Specifically
	// - restore2TB/nodes=10
	// - restore2TB/nodes=32
	// - restore2TB/nodes=6/cpus=8/pd-volume=2500GB
	durationGauge := r.PromFactory().NewGaugeVec(prometheus.GaugeOpts{Namespace: registry.
		PrometheusNameSpace, Subsystem: "restore", Name: "duration"}, []string{"test_name"})

	withPauseSpecs := restoreSpecs{
		hardware: makeHardwareSpecs(hardwareSpecs{}),
		backup: makeBackupSpecs(
			backupSpecs{workload: tpceRestore{customers: 5000},
				version: "v22.2.1"}),
		timeout:    3 * time.Hour,
		namePrefix: "pause",
	}
	withPauseSpecs.initTestName()

	r.Add(registry.TestSpec{
		Name:    withPauseSpecs.testName,
		Owner:   registry.OwnerDisasterRecovery,
		Cluster: withPauseSpecs.hardware.makeClusterSpecs(r),
		Timeout: withPauseSpecs.timeout,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {

			if c.Spec().Cloud != withPauseSpecs.backup.cloud {
				// For now, only run the test on the cloud provider that also stores the backup.
				t.Skip("test configured to run on %s", withPauseSpecs.backup.cloud)
			}
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), install.MakeClusterSettings())
			m := c.NewMonitor(ctx)

			withPauseSpecs.getRuntimeSpecs(ctx, t, c)
			// Run the disk usage logger in the monitor to guarantee its
			// having terminated when the test ends.
			dul := NewDiskUsageLogger(t, c)
			m.Go(dul.Runner)
			hc := NewHealthChecker(t, c, c.All())
			m.Go(hc.Runner)

			jobIDCh := make(chan jobspb.JobID)
			jobCompleteCh := make(chan struct{}, 1)

			pauseAtProgress := []float32{0.2, 0.45, 0.7}
			for i := range pauseAtProgress {
				// Add up to 10% to the pause point.
				pauseAtProgress[i] = pauseAtProgress[i] + float32(rand.Intn(10))/100
			}
			pauseIndex := 0
			// Spin up go routine which pauses and resumes the Restore job three times.
			m.Go(func(ctx context.Context) error {
				// Wait until the restore job has been created.
				conn, err := c.ConnE(ctx, t.L(), c.Node(1)[0])
				require.NoError(t, err)
				sql := sqlutils.MakeSQLRunner(conn)

				// The job should be created fairly quickly once the roachtest starts.
				done := ctx.Done()
				jobID := <-jobIDCh

				jobProgressTick := time.NewTicker(time.Minute * 1)
				defer jobProgressTick.Stop()
				for {
					if pauseIndex == len(pauseAtProgress) {
						t.L().Printf("RESTORE job was paused a maximum number of times; allowing the job to complete")
						return nil
					}
					select {
					case <-done:
						return ctx.Err()
					case <-jobCompleteCh:
						return nil
					case <-jobProgressTick.C:
						var fraction float32
						sql.QueryRow(t, `SELECT fraction_completed FROM [SHOW JOBS] WHERE job_id = $1`,
							jobID).Scan(&fraction)
						t.L().Printf("RESTORE Progress %.2f", fraction)
						if fraction < pauseAtProgress[pauseIndex] {
							continue
						}
						t.L().Printf("pausing RESTORE job since progress is greater than %.2f", pauseAtProgress[pauseIndex])
						// Pause the job and wait for it to transition to a paused state.
						_, err := conn.Query(`PAUSE JOB $1`, jobID)
						if err != nil {
							// The pause job request should not fail unless the job has already succeeded,
							// in which case, the test should gracefully succeed.
							var status string
							sql.QueryRow(t, `SELECT status FROM [SHOW JOBS] WHERE job_id = $1`, jobID).Scan(&status)
							if status == "succeeded" {
								return nil
							}
						}
						require.NoError(t, err)
						testutils.SucceedsSoon(t, func() error {
							var status string
							sql.QueryRow(t, `SELECT status FROM [SHOW JOBS] WHERE job_id = $1`, jobID).Scan(&status)
							if status != "paused" {
								return errors.Newf("expected status `paused` but found %s", status)
							}
							t.L().Printf("paused RESTORE job")
							pauseIndex++
							return nil
						})

						t.L().Printf("resuming RESTORE job")
						sql.Exec(t, `RESUME JOB $1`, jobID)
					}
				}
			})

			m.Go(func(ctx context.Context) error {
				defer dul.Done()
				defer hc.Done()
				defer close(jobCompleteCh)
				defer close(jobIDCh)
				t.Status(`running restore`)
				metricCollector := withPauseSpecs.initRestorePerfMetrics(ctx, durationGauge)
				jobID, err := withPauseSpecs.runDetached(ctx, "DATABASE tpce")
				require.NoError(t, err)
				jobIDCh <- jobID

				// Wait for the job to succeed.
				succeededJobTick := time.NewTicker(time.Minute * 1)
				defer succeededJobTick.Stop()
				done := ctx.Done()
				conn, err := c.ConnE(ctx, t.L(), c.Node(1)[0])
				require.NoError(t, err)
				var isJobComplete bool
				for {
					if isJobComplete {
						succeededJobTick.Stop()
						jobCompleteCh <- struct{}{}
						break
					}

					select {
					case <-done:
						return ctx.Err()
					case <-jobCompleteCh:
						return nil
					case <-succeededJobTick.C:
						var status string
						err := conn.QueryRow(`SELECT status FROM [SHOW JOBS] WHERE job_type = 'RESTORE'`).Scan(&status)
						require.NoError(t, err)
						if status == string(jobs.StatusSucceeded) {
							isJobComplete = true
						} else if status == string(jobs.StatusFailed) || status == string(jobs.StatusCanceled) {
							t.Fatalf("job unexpectedly found in %s state", status)
						}
					}
				}
				metricCollector()
				return nil
			})
			m.Wait()
			// All failures from the above go routines surface via a t.Fatal() within
			// the m.Wait( ) call above; therefore, at this point, the restore job
			// should have succeeded. This final check ensures this test is actually
			// doing its job: causing the restore job to pause at least once.
			require.NotEqual(t, 0, pauseIndex, "the job should have paused at least once")
		},

		// TODO(msbutler): to test the correctness of checkpointing, we should
		// restore the same fixture without pausing it and fingerprint both restored
		// databases.
	})

	for _, sp := range []restoreSpecs{
		{
			hardware: makeHardwareSpecs(hardwareSpecs{}),
			backup:   makeBackupSpecs(backupSpecs{}),
			timeout:  1 * time.Hour,
		},
		{
			// Note that the default specs in makeHardwareSpecs() spin up restore tests in aws,
			// by default.
			hardware: makeHardwareSpecs(hardwareSpecs{}),
			backup:   makeBackupSpecs(backupSpecs{cloud: spec.GCE}),
			timeout:  1 * time.Hour,
		},
		{
			// Benchmarks if per node throughput remains constant if the number of
			// nodes doubles relative to default.
			hardware: makeHardwareSpecs(hardwareSpecs{nodes: 8}),
			backup:   makeBackupSpecs(backupSpecs{}),
			timeout:  1 * time.Hour,
		},
		{
			// Benchmarks if per node throughput doubles if the vcpu count doubles
			// relative to default.
			hardware: makeHardwareSpecs(hardwareSpecs{cpus: 16}),
			backup:   makeBackupSpecs(backupSpecs{}),
			timeout:  1 * time.Hour,
		},
		{
			// Ensures we can restore a 48 length incremental chain.
			// Also benchmarks per node throughput for a long chain.
			hardware: makeHardwareSpecs(hardwareSpecs{}),
			backup:   makeBackupSpecs(backupSpecs{backupsIncluded: 48}),
			timeout:  1 * time.Hour,
		},
		{
			// The nightly 8TB Restore test.
			hardware: makeHardwareSpecs(hardwareSpecs{nodes: 10, volumeSize: 2000}),
			backup: makeBackupSpecs(backupSpecs{
				version:  "v22.2.1",
				workload: tpceRestore{customers: 500000}}),
			timeout: 5 * time.Hour,
		},
		{
			// The weekly 32TB Restore test.
			hardware: makeHardwareSpecs(hardwareSpecs{nodes: 15, cpus: 16, volumeSize: 5000}),
			backup: makeBackupSpecs(backupSpecs{
				version:  "v22.2.1",
				workload: tpceRestore{customers: 2000000}}),
			timeout: 24 * time.Hour,
			tags:    []string{"weekly", "aws-weekly"},
		},
		// TODO(msbutler): add the following tests once roachperf/grafana is hooked up and old tests are
		// removed:
		// - restore/tpce/400GB/nodes=30
		// - restore/tpce/400GB/encryption
	} {
		sp := sp
		sp.initTestName()
		r.Add(registry.TestSpec{
			Name:    sp.testName,
			Owner:   registry.OwnerDisasterRecovery,
			Cluster: sp.hardware.makeClusterSpecs(r),
			Timeout: sp.timeout,
			// These tests measure performance. To ensure consistent perf,
			// disable metamorphic encryption.
			EncryptionSupport: registry.EncryptionAlwaysDisabled,
			Tags:              sp.tags,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {

				t.L().Printf("Full test specs: %s", sp.computeName(true))

				if c.Spec().Cloud != sp.backup.cloud {
					// For now, only run the test on the cloud provider that also stores the backup.
					t.Skip("test configured to run on %s", sp.backup.cloud)
				}
				c.Put(ctx, t.Cockroach(), "./cockroach")
				c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), install.MakeClusterSettings())
				m := c.NewMonitor(ctx)

				// Run the disk usage logger in the monitor to guarantee its
				// having terminated when the test ends.
				dul := NewDiskUsageLogger(t, c)
				m.Go(dul.Runner)
				hc := NewHealthChecker(t, c, c.All())
				m.Go(hc.Runner)

				sp.getRuntimeSpecs(ctx, t, c)
				m.Go(func(ctx context.Context) error {
					defer dul.Done()
					defer hc.Done()
					t.Status(`running restore`)
					metricCollector := sp.initRestorePerfMetrics(ctx, durationGauge)
					if err := sp.run(ctx, ""); err != nil {
						return err
					}
					metricCollector()
					return nil
				})
				m.Wait()
			},
		})
	}
}

var defaultHardware = hardwareSpecs{
	cpus:       8,
	nodes:      4,
	volumeSize: 1000,
}

type hardwareSpecs struct {

	// cpus is the per node cpu count.
	cpus int

	// nodes is the number of nodes in the restore.
	nodes int

	// volumeSize indicates the size of per node block storage (pd-ssd for gcs,
	// ebs for aws). If zero, local ssd's are used.
	volumeSize int
}

func (hw hardwareSpecs) makeClusterSpecs(r registry.Registry) spec.ClusterSpec {
	clusterOpts := make([]spec.Option, 0)
	clusterOpts = append(clusterOpts, spec.CPU(hw.cpus))
	if hw.volumeSize != 0 {
		clusterOpts = append(clusterOpts, spec.VolumeSize(hw.volumeSize))
	}
	return r.MakeClusterSpec(hw.nodes, clusterOpts...)
}

// String prints the hardware specs. If verbose==true, verbose specs are printed.
func (hw hardwareSpecs) String(verbose bool) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("/nodes=%d", hw.nodes))
	builder.WriteString(fmt.Sprintf("/cpus=%d", hw.cpus))
	if verbose {
		builder.WriteString(fmt.Sprintf("/volSize=%dGB", hw.volumeSize))
	}
	return builder.String()
}

// makeHardwareSpecs instantiates hardware specs for a restore roachtest.
// Unless the caller provides any explicit specs, the default specs are used.
func makeHardwareSpecs(override hardwareSpecs) hardwareSpecs {
	specs := defaultHardware
	if override.cpus != 0 {
		specs.cpus = override.cpus
	}
	if override.nodes != 0 {
		specs.nodes = override.nodes
	}
	if override.volumeSize != 0 {
		specs.volumeSize = override.volumeSize
	}
	return specs
}

var defaultBackupSpecs = backupSpecs{
	// TODO(msbutler): write a script that automatically finds the latest versioned fixture.
	version:          "v22.2.0",
	cloud:            spec.AWS,
	backupProperties: "inc-count=48",
	fullBackupDir:    "LATEST",
	backupsIncluded:  12,
	workload:         tpceRestore{customers: 25000},
}

type backupSpecs struct {
	// version specifies the crdb version the backup was taken on.
	version string

	// cloud is the cloud storage provider the backup is stored on.
	cloud string

	// backupProperties identifies specific backup properties included in the backup fixture
	// path.
	backupProperties string

	// specifies the full backup directory in the collection to restore from.
	fullBackupDir string

	// specifies the number of backups in the chain to restore from
	backupsIncluded int

	// workload defines the backed up workload.
	workload backupWorkload

	// aost specifies the aost to restore from. Derived at runtime.
	aost string
}

// String returns a stringified version of the backup specs. Note that the
// backup version, backup directory, and AOST are never included.
func (bs backupSpecs) String(verbose bool) string {
	var builder strings.Builder
	builder.WriteString("/" + bs.workload.String())

	if verbose || bs.backupProperties != defaultBackupSpecs.backupProperties {
		builder.WriteString("/" + bs.backupProperties)
	}
	builder.WriteString("/" + bs.cloud)

	if verbose || bs.backupsIncluded != defaultBackupSpecs.backupsIncluded {
		builder.WriteString("/" + fmt.Sprintf("backupsIncluded=%d", bs.backupsIncluded))
	}
	return builder.String()
}

func (bs backupSpecs) storagePrefix() string {
	if bs.cloud == spec.AWS {
		return "s3"
	}
	return "gs"
}

func (bs backupSpecs) backupCollection() string {
	return fmt.Sprintf(`'%s://cockroach-fixtures/backups/%s/%s/%s?AUTH=implicit'`,
		bs.storagePrefix(), bs.workload.fixtureDir(), bs.version, bs.backupProperties)
}

// getAOSTCmd returns a sql cmd that will return a system time that is equal to the end time of
// the bs.backupsIncluded'th backup in the target backup chain.
func (bs backupSpecs) getAostCmd() string {
	return fmt.Sprintf(
		`SELECT max(end_time) FROM [SELECT DISTINCT end_time FROM [SHOW BACKUP FROM %s IN %s] ORDER BY end_time LIMIT %d]`,
		bs.fullBackupDir,
		bs.backupCollection(),
		bs.backupsIncluded)
}

// makeBackupSpecs initializes the default backup specs. The caller can override
// any of the default backup specs by passing any non-nil params.
func makeBackupSpecs(override backupSpecs) backupSpecs {
	specs := defaultBackupSpecs

	if override.cloud != "" {
		specs.cloud = override.cloud
	}
	if override.version != "" {
		specs.version = override.version
	}

	if override.backupProperties != "" {
		specs.backupProperties = override.backupProperties
	}

	if override.fullBackupDir != "" {
		specs.fullBackupDir = override.fullBackupDir
	}

	if override.backupsIncluded != 0 {
		specs.backupsIncluded = override.backupsIncluded
	}

	if override.workload != nil {
		specs.workload = override.workload
	}

	return specs
}

type backupWorkload interface {
	fixtureDir() string
	String() string
}

type tpceRestore struct {
	customers int
}

func (tpce tpceRestore) fixtureDir() string {
	return fmt.Sprintf(`tpc-e/customers=%d`, tpce.customers)
}

func (tpce tpceRestore) String() string {
	var builder strings.Builder
	builder.WriteString("tpce/")
	switch tpce.customers {
	case 5000:
		builder.WriteString("80GB")
	case 25000:
		builder.WriteString("400GB")
	case 500000:
		builder.WriteString("8TB")
	case 2000000:
		builder.WriteString("32TB")
	default:
		panic("tpce customer count not recognized")
	}
	return builder.String()
}

type restoreSpecs struct {
	hardware hardwareSpecs
	backup   backupSpecs
	timeout  time.Duration
	tags     []string

	// namePrefix appears in the name of the roachtest, i.e. `restore/{prefix}/{config}`.
	namePrefix string

	t        test.Test
	c        cluster.Cluster
	testName string
}

func (sp *restoreSpecs) initTestName() {
	sp.testName = sp.computeName(false)
}

func (sp *restoreSpecs) computeName(verbose bool) string {
	var prefix string
	if sp.namePrefix != "" {
		prefix = "/" + sp.namePrefix
	}
	return "restore" + prefix + sp.backup.String(verbose) + sp.hardware.String(verbose)
}

func (sp *restoreSpecs) restoreCmd(target, opts string) string {
	return fmt.Sprintf(`./cockroach sql --insecure -e "RESTORE %s FROM %s IN %s AS OF SYSTEM TIME '%s' %s"`,
		target, sp.backup.fullBackupDir, sp.backup.backupCollection(), sp.backup.aost, opts)
}

func (sp *restoreSpecs) getRuntimeSpecs(ctx context.Context, t test.Test, c cluster.Cluster) {
	sp.t = t
	sp.c = c

	var aost string
	conn := sp.c.Conn(ctx, sp.t.L(), 1)
	err := conn.QueryRowContext(ctx, sp.backup.getAostCmd()).Scan(&aost)
	require.NoError(sp.t, err)
	sp.backup.aost = aost
}

// run executes the restore, where target injects a restore target into the restore command.
// Examples:
// - "DATABASE tpce" will execute a database restore on the tpce cluster.
// - "" will execute a cluster restore.
func (sp *restoreSpecs) run(ctx context.Context, target string) error {
	return sp.c.RunE(ctx, sp.c.Node(1), sp.restoreCmd(target, ""))
}

func (sp *restoreSpecs) runDetached(ctx context.Context, target string) (jobspb.JobID, error) {
	if err := sp.c.RunE(ctx, sp.c.Node(1), sp.restoreCmd(target, "WITH DETACHED")); err != nil {
		return 0, err
	}

	db, err := sp.c.ConnE(ctx, sp.t.L(), sp.c.Node(1)[0])
	if err != nil {
		return 0, errors.Wrap(err, "failed to connect to node 1; running restore detached")
	}
	var jobID jobspb.JobID
	if err := db.QueryRow(`SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'RESTORE'`).Scan(&jobID); err != nil {
		return 0, err
	}
	return jobID, nil
}

// initRestorePerfMetrics returns a function that will collect restore throughput at the end of
// the test.
//
// TODO(msbutler): only export metrics to test-eng prometheus server once it begins scraping
// nightly roachtest runs.
func (sp *restoreSpecs) initRestorePerfMetrics(
	ctx context.Context, durationGauge *prometheus.GaugeVec,
) func() {
	dut, err := NewDiskUsageTracker(sp.c, sp.t.L())
	require.NoError(sp.t, err)
	startTime := timeutil.Now()
	startDu := dut.GetDiskUsage(ctx, sp.c.All())

	return func() {
		promLabel := registry.PromSub(strings.Replace(sp.testName, "restore/", "", 1)) + "_seconds"
		testDuration := timeutil.Since(startTime).Seconds()
		durationGauge.WithLabelValues(promLabel).Set(testDuration)

		// compute throughput as MB / node / second.
		du := dut.GetDiskUsage(ctx, sp.c.All())
		throughput := float64(du-startDu) / (float64(sp.hardware.nodes) * testDuration)
		sp.t.L().Printf("Usage %d , Nodes %d , Duration %f\n; Throughput: %f mb / node / second",
			du,
			sp.hardware.nodes,
			testDuration,
			throughput)
		exportToRoachperf(ctx, sp.t, sp.c, sp.testName, int64(throughput))
	}
}

// exportToRoachperf exports a single perf metric for the given test to roachperf.
func exportToRoachperf(
	ctx context.Context, t test.Test, c cluster.Cluster, testName string, metric int64,
) {

	// The easiest way to record a precise metric for roachperf is to caste it as a duration,
	// in seconds in the histogram's upper bound.
	reg := histogram.NewRegistry(
		time.Duration(metric)*time.Second,
		histogram.MockWorkloadName,
	)
	bytesBuf := bytes.NewBuffer([]byte{})
	jsonEnc := json.NewEncoder(bytesBuf)

	// Ensure the histogram contains the name of the roachtest
	reg.GetHandle().Get(testName)

	// Serialize the histogram into the buffer
	reg.Tick(func(tick histogram.Tick) {
		_ = jsonEnc.Encode(tick.Snapshot())
	})
	// Upload the perf artifacts to any one of the nodes so that the test
	// runner copies it into an appropriate directory path.
	dest := filepath.Join(t.PerfArtifactsDir(), "stats.json")
	if err := c.RunE(ctx, c.Node(1), "mkdir -p "+filepath.Dir(dest)); err != nil {
		log.Errorf(ctx, "failed to create perf dir: %+v", err)
	}
	if err := c.PutString(ctx, bytesBuf.String(), dest, 0755, c.Node(1)); err != nil {
		log.Errorf(ctx, "failed to upload perf artifacts to node: %s", err.Error())
	}
}

// verifyMetrics loops, retrieving the timeseries metrics specified in m every
// 10s and verifying that the most recent value is less that the limit
// specified in m. This is particularly useful for verifying that a counter
// metric does not exceed some threshold during a test. For example, the
// restore and import tests verify that the range merge queue is inactive.
func verifyMetrics(
	ctx context.Context, t test.Test, c cluster.Cluster, m map[string]float64,
) error {
	const sample = 10 * time.Second
	// Query needed information over the timespan of the query.
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Node(1))
	if err != nil {
		return err
	}
	url := "http://" + adminUIAddrs[0] + "/ts/query"

	request := tspb.TimeSeriesQueryRequest{
		// Ask for one minute intervals. We can't just ask for the whole hour
		// because the time series query system does not support downsampling
		// offsets.
		SampleNanos: sample.Nanoseconds(),
	}
	for name := range m {
		request.Queries = append(request.Queries, tspb.Query{
			Name:             name,
			Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
			SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
		})
	}

	ticker := time.NewTicker(sample)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		now := timeutil.Now()
		request.StartNanos = now.Add(-sample * 3).UnixNano()
		request.EndNanos = now.UnixNano()

		var response tspb.TimeSeriesQueryResponse
		if err := httputil.PostJSON(http.Client{}, url, &request, &response); err != nil {
			return err
		}

		for i := range request.Queries {
			name := request.Queries[i].Name
			data := response.Results[i].Datapoints
			n := len(data)
			if n == 0 {
				continue
			}
			limit := m[name]
			value := data[n-1].Value
			if value >= limit {
				return fmt.Errorf("%s: %.1f >= %.1f @ %d", name, value, limit, data[n-1].TimestampNanos)
			}
		}
	}
}

// TODO(peter): silence unused warning.
var _ = verifyMetrics
