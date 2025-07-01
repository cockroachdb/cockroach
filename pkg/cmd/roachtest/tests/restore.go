// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/blobfixture"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

var restoreAggregateFunction = func(test string, histogram *roachtestutil.HistogramMetric) (roachtestutil.AggregatedPerfMetrics, error) {
	metricValue := histogram.Summaries[0].HighestTrackableValue / 1e9

	return roachtestutil.AggregatedPerfMetrics{
		{
			Name:           fmt.Sprintf("%s_max", test),
			Value:          metricValue,
			Unit:           "MB/s/node",
			IsHigherBetter: false,
		},
	}, nil
}

func registerRestoreNodeShutdown(r registry.Registry) {
	sp := restoreSpecs{
		hardware: makeHardwareSpecs(hardwareSpecs{}),
		backup: backupSpecs{
			cloud:   spec.GCE,
			fixture: SmallFixture,
		},
		timeout: 1 * time.Hour,
	}

	makeRestoreStarter := func(ctx context.Context, t test.Test, c cluster.Cluster,
		gatewayNode int, rd restoreDriver) jobStarter {
		return func(c cluster.Cluster, l *logger.Logger) (jobspb.JobID, error) {
			return rd.runDetached(ctx, "DATABASE tpcc", gatewayNode)
		}
	}

	r.Add(registry.TestSpec{
		Name:                      "restore/nodeShutdown/worker",
		Owner:                     registry.OwnerDisasterRecovery,
		Cluster:                   sp.hardware.makeClusterSpecs(r),
		CompatibleClouds:          sp.backup.CompatibleClouds(),
		Suites:                    registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
		Leases:                    registry.MetamorphicLeases,
		Timeout:                   sp.timeout,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := 2
			nodeToShutdown := 3

			rd := makeRestoreDriver(ctx, t, c, sp)
			rd.prepareCluster(ctx)
			cfg := defaultNodeShutdownConfig(c, nodeToShutdown)
			cfg.restartSettings = rd.defaultClusterSettings()
			require.NoError(t,
				executeNodeShutdown(ctx, t, c, cfg,
					makeRestoreStarter(ctx, t, c, gatewayNode, rd)))
			rd.maybeValidateFingerprint(ctx)
		},
	})

	r.Add(registry.TestSpec{
		Name:                      "restore/nodeShutdown/coordinator",
		Owner:                     registry.OwnerDisasterRecovery,
		Cluster:                   sp.hardware.makeClusterSpecs(r),
		CompatibleClouds:          sp.backup.CompatibleClouds(),
		Suites:                    registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
		Leases:                    registry.MetamorphicLeases,
		Timeout:                   sp.timeout,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := 2
			nodeToShutdown := 2

			rd := makeRestoreDriver(ctx, t, c, sp)
			rd.prepareCluster(ctx)
			cfg := defaultNodeShutdownConfig(c, nodeToShutdown)
			cfg.restartSettings = rd.defaultClusterSettings()
			require.NoError(t,
				executeNodeShutdown(ctx, t, c, cfg,
					makeRestoreStarter(ctx, t, c, gatewayNode, rd)))
			rd.maybeValidateFingerprint(ctx)
		},
	})
}

func registerRestore(r registry.Registry) {

	durationGauge := r.PromFactory().NewGaugeVec(prometheus.GaugeOpts{Namespace: registry.
		PrometheusNameSpace, Subsystem: "restore", Name: "duration"}, []string{"test_name"})

	withPauseSpecs := restoreSpecs{
		hardware: makeHardwareSpecs(hardwareSpecs{}),
		backup: backupSpecs{
			cloud:   spec.GCE,
			fixture: SmallFixture,
		},
		timeout:    3 * time.Hour,
		namePrefix: "pause",
	}
	withPauseSpecs.initTestName()

	r.Add(registry.TestSpec{
		Name:                      withPauseSpecs.testName,
		Owner:                     registry.OwnerDisasterRecovery,
		Benchmark:                 true,
		Cluster:                   withPauseSpecs.hardware.makeClusterSpecs(r),
		Timeout:                   withPauseSpecs.timeout,
		CompatibleClouds:          withPauseSpecs.backup.CompatibleClouds(),
		Suites:                    registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
		PostProcessPerfMetrics:    restoreAggregateFunction,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {

			rd := makeRestoreDriver(ctx, t, c, withPauseSpecs)
			rd.prepareCluster(ctx)

			// Run the disk usage logger in the monitor to guarantee its
			// having terminated when the test ends.
			m := c.NewDeprecatedMonitor(ctx)
			dul := roachtestutil.NewDiskUsageLogger(t, c)
			m.Go(dul.Runner)

			jobIDCh := make(chan jobspb.JobID)
			jobCompleteCh := make(chan struct{}, 1)

			pauseAtProgress := []float64{0.2, 0.45, 0.7}
			for i := range pauseAtProgress {
				// Add up to 10% to the pause point.
				pauseAtProgress[i] = pauseAtProgress[i] + float64(rand.Intn(10))/100
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

				jobProgressTick := time.NewTicker(time.Second * 5)
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
						var fraction gosql.NullFloat64
						sql.QueryRow(t, `SELECT fraction_completed FROM [SHOW JOB $1]`,
							jobID).Scan(&fraction)
						t.L().Printf("RESTORE Progress %.2f", fraction.Float64)
						if !fraction.Valid || fraction.Float64 < pauseAtProgress[pauseIndex] {
							continue
						}
						t.L().Printf("pausing RESTORE job since progress is greater than %.2f", pauseAtProgress[pauseIndex])
						// Pause the job and wait for it to transition to a paused state.
						_, err := conn.Query(`PAUSE JOB $1`, jobID)
						if err != nil {
							// The pause job request should not fail unless the job has already succeeded,
							// in which case, the test should gracefully succeed.
							var status string
							sql.QueryRow(t, `SELECT status FROM [SHOW JOB $1]`, jobID).Scan(&status)
							if status == "succeeded" {
								return nil
							}
						}
						require.NoError(t, err)
						testutils.SucceedsWithin(t, func() error {
							var status string
							sql.QueryRow(t, `SELECT status FROM [SHOW JOB $1]`, jobID).Scan(&status)
							if status != "paused" {
								return errors.Newf("expected status `paused` but found %s", status)
							}
							t.L().Printf("paused RESTORE job")
							pauseIndex++
							return nil
						}, 2*time.Minute)

						t.L().Printf("resuming RESTORE job")
						sql.Exec(t, `RESUME JOB $1`, jobID)
					}
				}
			})

			m.Go(func(ctx context.Context) error {
				defer dul.Done()
				defer close(jobCompleteCh)
				defer close(jobIDCh)
				t.Status(`running restore`)
				metricCollector := rd.initRestorePerfMetrics(ctx, durationGauge)
				jobID, err := rd.runDetached(ctx, "DATABASE tpcc", 1)
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
						if status == string(jobs.StateSucceeded) {
							isJobComplete = true
						} else if status == string(jobs.StateFailed) || status == string(jobs.StateCanceled) {
							t.Fatalf("job unexpectedly found in %s state", status)
						}
					}
				}
				metricCollector()
				rd.maybeValidateFingerprint(ctx)
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
			backup: backupSpecs{
				cloud:   spec.GCE,
				fixture: TinyFixture,
			},
			timeout: 2 * time.Hour,
			suites:  registry.Suites(registry.Nightly),
		},
		{
			hardware: makeHardwareSpecs(hardwareSpecs{ebsThroughput: 250 /* MB/s */}),
			backup:   backupSpecs{cloud: spec.AWS, fixture: SmallFixture},
			timeout:  2 * time.Hour,
			suites:   registry.Suites(registry.Nightly),
		},
		{
			hardware: makeHardwareSpecs(hardwareSpecs{}),
			backup:   backupSpecs{cloud: spec.GCE, fixture: SmallFixture},
			timeout:  2 * time.Hour,
			suites:   registry.Suites(registry.Nightly),
		},
		{
			// Benchmarks using a low memory per core ratio - we don't expect ideal
			// performance but nodes should not OOM.
			hardware: makeHardwareSpecs(hardwareSpecs{mem: spec.Low}),
			backup:   backupSpecs{cloud: spec.GCE, fixture: SmallFixture},
			timeout:  2 * time.Hour,
			suites:   registry.Suites(registry.Nightly),
		},
		{
			// Benchmarks a wide cluster to test split and scatter perf on a multi store cluster that uses local ssds.
			hardware: makeHardwareSpecs(hardwareSpecs{nodes: 16, cpus: 4, storesPerNode: 4, useLocalSSD: true}),
			backup:   backupSpecs{cloud: spec.GCE, fixture: SmallFixture},
			timeout:  2 * time.Hour,
			suites:   registry.Suites(registry.Nightly),
			skip:     "ad hoc testing for now",
			setUpStmts: []string{
				// Create max 64 MB ranges instead.
				"SET CLUSTER SETTING backup.restore_span.target_size = '32 MiB'",
				"ALTER RANGE default CONFIGURE ZONE USING range_min_bytes = 16777216, range_max_bytes = 67108864, num_replicas = 3"},
		},
		{
			// Benchmarks if per node throughput remains constant if the number of
			// nodes doubles relative to default.
			hardware: makeHardwareSpecs(hardwareSpecs{nodes: 8}),
			backup:   backupSpecs{cloud: spec.GCE, fixture: SmallFixture},
			timeout:  2 * time.Hour,
			suites:   registry.Suites(registry.Nightly),
		},
		{
			// Benchmarks if per node throughput remains constant if the cluster
			// is multi-region.
			hardware: makeHardwareSpecs(hardwareSpecs{
				nodes: 9, ebsThroughput: 250, /* MB/s */
				zones: []string{"us-east-2b", "us-west-2b", "eu-west-1b"}}), // These zones are AWS-specific.
			backup:  backupSpecs{cloud: spec.AWS, fixture: SmallFixture},
			timeout: 3 * time.Hour,
			suites:  registry.Suites(registry.Nightly),
		},
		{
			// Benchmarks if per node throughput doubles if the vcpu count doubles
			// relative to default.
			hardware: makeHardwareSpecs(hardwareSpecs{cpus: 16}),
			backup:   backupSpecs{cloud: spec.GCE, fixture: SmallFixture},
			timeout:  2 * time.Hour,
			suites:   registry.Suites(registry.Nightly),
		},
		{
			// The weekly 20TB Restore test.
			hardware:        makeHardwareSpecs(hardwareSpecs{nodes: 15, cpus: 16, volumeSize: 5000}),
			backup:          backupSpecs{cloud: spec.GCE, fixture: LargeFixture},
			timeout:         24 * time.Hour,
			suites:          registry.Suites(registry.Weekly),
			skipFingerprint: true,
		},
		// OR Benchmarking tests
		// See benchmark plan here: https://docs.google.com/spreadsheets/d/1uPcQ1YPohXKxwFxWWDUMJrYLKQOuqSZKVrI8SJam5n8
		{
			hardware:        makeHardwareSpecs(hardwareSpecs{}),
			backup:          backupSpecs{cloud: spec.GCE, fixture: SmallFixture},
			timeout:         1 * time.Hour,
			suites:          registry.Suites(registry.Nightly),
			fullBackupOnly:  true,
			skipFingerprint: true,
			skip:            "used for adhoc benchmarking against OR",
		},
		{
			hardware:        makeHardwareSpecs(hardwareSpecs{nodes: 10, volumeSize: 1500, workloadNode: true}),
			backup:          backupSpecs{cloud: spec.GCE, fixture: MediumFixture},
			timeout:         3 * time.Hour,
			suites:          registry.Suites(registry.Nightly),
			fullBackupOnly:  true,
			skipFingerprint: true,
			skip:            "used for adhoc benchmarking against OR",
		},
		// TODO(msbutler): add the following tests once roachperf/grafana is hooked up and old tests are
		// removed:
		// - restore/tpce/400GB/nodes=30
		// - restore/tpce/400GB/encryption
	} {
		sp := sp
		sp.initTestName()

		r.Add(registry.TestSpec{
			Name:      sp.testName,
			Owner:     registry.OwnerDisasterRecovery,
			Benchmark: true,
			Cluster:   sp.hardware.makeClusterSpecs(r),
			Timeout:   sp.timeout,
			// These tests measure performance. To ensure consistent perf,
			// disable metamorphic encryption.
			EncryptionSupport:         registry.EncryptionAlwaysDisabled,
			CompatibleClouds:          sp.backup.CompatibleClouds(),
			Suites:                    sp.suites,
			TestSelectionOptOutSuites: sp.suites,
			Skip:                      sp.skip,
			PostProcessPerfMetrics:    restoreAggregateFunction,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {

				rd := makeRestoreDriver(ctx, t, c, sp)
				rd.prepareCluster(ctx)

				// Run the disk usage logger in the monitor to guarantee its
				// having terminated when the test ends.
				m := c.NewDeprecatedMonitor(ctx)
				dul := roachtestutil.NewDiskUsageLogger(t, c)
				m.Go(dul.Runner)
				m.Go(func(ctx context.Context) error {
					defer dul.Done()
					t.Status(`running setup statements`)
					db, err := rd.c.ConnE(ctx, rd.t.L(), rd.c.Node(1)[0])
					if err != nil {
						return errors.Wrapf(err, "failure to run setup statements")
					}
					defer db.Close()
					// Run set-up SQL statements. In particular, enable collecting CPU
					// profiles automatically if CPU usage is high. Historically, we
					// observed CPU going as high as 100%, e.g. see issue #111160.
					// TODO(pavelkalinnikov): enable CPU profiling in all roachtests.
					for _, stmt := range append(sp.setUpStmts,
						"SET CLUSTER SETTING server.cpu_profile.duration = '2s'",
						"SET CLUSTER SETTING server.cpu_profile.cpu_usage_combined_threshold = 80",
					) {
						_, err := db.Exec(stmt)
						if err != nil {
							return errors.Wrapf(err, "error executing setup stmt [%s]", stmt)
						}
					}

					if err := roachtestutil.WaitForReplication(ctx, t.L(), db, 3, roachtestutil.AtLeastReplicationFactor); err != nil {
						return err
					}
					if err := waitForRebalance(
						ctx, t.L(), db, 10, 60, /* stableSeconds */
					); err != nil {
						return err
					}

					t.Status(`running restore`)
					metricCollector := rd.initRestorePerfMetrics(ctx, durationGauge)
					if err := rd.run(ctx, "DATABASE "+rd.sp.backup.fixture.DatabaseName()); err != nil {
						return err
					}
					metricCollector()
					rd.maybeValidateFingerprint(ctx)
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

// hardwareSpecs define the cluster setup for a restore roachtest. These values
// should not get updated as the test runs.
type hardwareSpecs struct {

	// cpus is the per node cpu count.
	cpus int

	// nodes is the number of crdb nodes in the restore.
	nodes int

	storesPerNode int

	// addWorkloadNode is true if workload node should also get spun up
	workloadNode bool

	// volumeSize indicates the size of per node block storage (pd-ssd for gcs,
	// ebs for aws).
	volumeSize int

	useLocalSSD bool

	// ebsThroughput is the min provisioned throughput of the EBS volume, in MB/s.
	// Ignored if not running on AWS. Defaults to 125 MiB/s for the default gp3
	// volume.
	ebsThroughput int

	// ebsIOPS is the configured IOPS for the EBS volume. Ignored if not running
	// on AWS. Defaults to 3000 IOPS for the default gp3 volume.
	ebsIOPS int

	// mem is the memory per cpu.
	mem spec.MemPerCPU

	// Availability zones to use. (Values are cloud-provider-specific.)
	// If unset, the first of the default availability zones for the provider will be used.
	zones []string
}

func (hw hardwareSpecs) makeClusterSpecs(r registry.Registry) spec.ClusterSpec {
	clusterOpts := make([]spec.Option, 0)
	clusterOpts = append(clusterOpts, spec.CPU(hw.cpus))
	if hw.volumeSize != 0 {
		clusterOpts = append(clusterOpts, spec.VolumeSize(hw.volumeSize))
	}
	if hw.ebsThroughput != 0 {
		clusterOpts = append(clusterOpts, spec.AWSVolumeThroughput(hw.ebsThroughput))
	}
	if hw.ebsIOPS != 0 {
		clusterOpts = append(clusterOpts, spec.AWSVolumeIOPS(hw.ebsIOPS))
	}

	if hw.useLocalSSD {
		clusterOpts = append(clusterOpts, spec.PreferLocalSSD())
		if hw.volumeSize != 0 || hw.ebsThroughput != 0 {
			panic("cannot set volume size or ebs throughput and use local SSD")
		}
	}

	if hw.storesPerNode != 0 {
		if !hw.useLocalSSD {
			panic("cannot set stores per node and not use local SSD")
		}
		clusterOpts = append(clusterOpts, spec.SSD(hw.storesPerNode))
		clusterOpts = append(clusterOpts, spec.Arch(vm.ArchAMD64))
	}

	if hw.mem != spec.Auto {
		clusterOpts = append(clusterOpts, spec.Mem(hw.mem))
	}
	addWorkloadNode := 0
	if hw.workloadNode {
		addWorkloadNode++
		clusterOpts = append(clusterOpts, spec.WorkloadNodeCount(1))
		// If the workload node has 32 cpus, we need to specify it.
		if hw.workloadNode && hw.cpus != 0 {
			clusterOpts = append(clusterOpts, spec.WorkloadNodeCPU(min(16, hw.cpus)))
		}
	}
	if len(hw.zones) > 0 {
		// Each test is set up to run on one specific cloud, so it's ok that the
		// zones will only make sense for one of them.
		// TODO(radu): clean this up.
		clusterOpts = append(clusterOpts, spec.GCEZones(strings.Join(hw.zones, ",")))
		clusterOpts = append(clusterOpts, spec.AWSZones(strings.Join(hw.zones, ",")))
		clusterOpts = append(clusterOpts, spec.Geo())
	}

	s := r.MakeClusterSpec(hw.nodes+addWorkloadNode, clusterOpts...)

	return s
}

// String prints the hardware specs. If verbose==true, verbose specs are printed.
func (hw hardwareSpecs) String() string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("/nodes=%d", hw.nodes))
	if hw.storesPerNode != 0 {
		builder.WriteString(fmt.Sprintf("/stores=%d", hw.storesPerNode))
	}
	builder.WriteString(fmt.Sprintf("/cpus=%d", hw.cpus))
	if hw.mem != spec.Auto {
		builder.WriteString(fmt.Sprintf("/%smem", hw.mem))
	}
	if len(hw.zones) > 0 {
		builder.WriteString(fmt.Sprintf("/zones=%s", strings.Join(hw.zones, ",")))
	}
	return builder.String()
}

func (hw hardwareSpecs) getWorkloadNode() int {
	if !hw.workloadNode {
		panic(`this test does not have a workload node`)
	}
	return hw.nodes + 1
}

func (hw hardwareSpecs) getCRDBNodes() option.NodeListOption {
	nodes := make(option.NodeListOption, hw.nodes)
	for i := range nodes {
		nodes[i] = i + 1
	}
	return nodes
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
	if override.storesPerNode != 0 {
		specs.storesPerNode = override.storesPerNode
	}
	if override.mem != spec.Auto {
		specs.mem = override.mem
	}
	specs.useLocalSSD = override.useLocalSSD
	if override.volumeSize != 0 {
		specs.volumeSize = override.volumeSize
	}
	if override.ebsThroughput != 0 {
		specs.ebsThroughput = override.ebsThroughput
	}
	if override.ebsIOPS != 0 {
		specs.ebsIOPS = override.ebsIOPS
	}
	if specs.useLocalSSD {
		specs.volumeSize = 0
		specs.ebsThroughput = 0
	}
	specs.zones = override.zones
	specs.workloadNode = override.workloadNode
	return specs
}

// backupSpecs define the backup that will get restored. These values should not
// get updated during the test.
type backupSpecs struct {
	// cloud represents the cloud provider the backup is stored on.
	cloud spec.Cloud

	// fixture is the fixture that will be restored.
	fixture BackupFixture

	// allowLocal is true if the test should be allowed to run
	// locally. We don't set this by default to avoid someone
	// trying to run the very large roachtests locally.
	allowLocal bool
}

func (bs backupSpecs) CloudIsCompatible(cloud spec.Cloud) error {
	if cloud == spec.Local && bs.allowLocal {
		return nil
	}
	if cloud != bs.cloud {
		// For now, only run the test on the cloud provider
		// that also stores the backup.
		return errors.Newf("test configured to run on %q (not %q)", bs.cloud, cloud)
	}
	return nil
}

func (bs backupSpecs) CompatibleClouds() registry.CloudSet {
	r := registry.Clouds(bs.cloud)
	if bs.allowLocal {
		r = registry.Clouds(bs.cloud, spec.Local)
	}
	return r
}

// getFullBackupEndTimeCmd returns a sql cmd that will return a system time for
// a restore from the full backup of the latest chain.
func (sp *restoreSpecs) getFullBackupEndTimeCmd(
	ctx context.Context, t test.Test, collectionURI string,
) string {
	return fmt.Sprintf(
		`SELECT max(end_time) FROM [SELECT DISTINCT end_time FROM
		[SHOW BACKUP FROM LATEST IN '%s'] ORDER BY end_time ASC LIMIT %d]`,
		collectionURI,
		1, // restore either restores full chain or just the full backup
	)
}

// restoreSpecs define input parameters to a restore roachtest set during
// registration. They should not be modified within test_spec.run(), as they are
// shared across driver runs.
type restoreSpecs struct {
	hardware hardwareSpecs

	// backup describes the backup fixture from which we will restore.
	backup  backupSpecs
	timeout time.Duration
	suites  registry.SuiteSet

	// fullBackupOnly indicates if the restore should only restore the full
	// backup. If false, restore will restore the entire chain instead.
	fullBackupOnly bool

	// namePrefix appears in the name of the roachtest, i.e. `restore/{prefix}/{config}`.
	namePrefix string

	// skipFingerprint, if not set, will verify the fingerprint of the restored
	// data after the restore completes. This assumes that the fixture being
	// restored has been fingerprinted. See backup_fixtures.go for details on
	// which fixtures contain fingerprints. Keep in mind that fingerprinting is a
	// slow process, so timeouts should be adjusted accordingly.
	// Note: Must be set to true if fullBackupOnly is false, as the fingerprints
	// created in the fixture are based on the entire backup chain.
	skipFingerprint bool

	setUpStmts []string

	// extraArgs are passed to the cockroach binary at startup.
	extraArgs []string

	// skip, if non-empty, skips the test with the given reason.
	skip string

	// testname is set automatically.
	testName string
}

func (sp *restoreSpecs) initTestName() {
	sp.testName = sp.computeName()
}

func (sp *restoreSpecs) computeName() string {
	var prefix string
	if sp.namePrefix != "" {
		prefix = "/" + sp.namePrefix
	}
	return "restore" + prefix + sp.String() + sp.hardware.String()
}

func (sp *restoreSpecs) String() string {
	bs := sp.backup

	var builder strings.Builder
	builder.WriteString("/" + bs.fixture.Kind())
	builder.WriteString("/" + bs.cloud.String())

	if sp.fullBackupOnly {
		builder.WriteString("/fullOnly")
	}

	return builder.String()
}

type restoreDriver struct {
	sp restoreSpecs

	t test.Test
	c cluster.Cluster

	// aost defines the "As Of System Time" used within the restore. Because this
	// gets computed during test execution, it is stored in the restoreDriver
	// rather than the restoreSpecs.
	aost string

	// fixtureMetadata is the metadata of the fixture being restored. This is
	// computed during test execution and is set at the moment the driver has
	// chosen the fixture to restore from. This ensures that the fixture being
	// referenced throughout the test is the same, even if a newer fixture is
	// created during the test execution.
	fixtureMetadata blobfixture.FixtureMetadata

	// collectionURI is the collection URI of the fixture being restored. Set
	// during test execution.
	collectionURI string

	rng *rand.Rand
}

func makeRestoreDriver(
	ctx context.Context, t test.Test, c cluster.Cluster, sp restoreSpecs,
) restoreDriver {
	if sp.fullBackupOnly && !sp.skipFingerprint {
		t.Fatalf(
			`must skip fingerprints if fullBackupOnly is true;
			fingerprints are based on the entire backup chain, not just the full backup`,
		)
	}
	rng, seed := randutil.NewPseudoRand()
	t.L().Printf(`Random Seed is %d`, seed)
	rd := restoreDriver{
		t:   t,
		c:   c,
		sp:  sp,
		rng: rng,
	}
	rd.findAndSetBackupFixture(ctx)
	return rd
}

func (rd *restoreDriver) defaultClusterSettings() []install.ClusterSettingOption {
	return []install.ClusterSettingOption{
		install.SecureOption(false),
	}
}

func (rd *restoreDriver) roachprodOpts() option.StartOpts {
	opts := option.NewStartOpts(option.NoBackupSchedule)
	opts.RoachprodOpts.ExtraArgs = append(opts.RoachprodOpts.ExtraArgs, rd.sp.extraArgs...)
	if rd.c.Spec().SSDs > 1 && !rd.c.Spec().RAID0 {
		opts.RoachprodOpts.StoreCount = rd.c.Spec().SSDs
	}
	return opts
}

// findAndSetBackupFixture finds the fixture to restore from and sets it in the
// driver to ensure that the same fixture is being referenced throughout the
// test.
func (rd *restoreDriver) findAndSetBackupFixture(ctx context.Context) {
	if rd.sp.backup.fixture == nil {
		rd.t.Fatalf("restoreSpecs.backup.fixture is nil; cannot run restore test")
	}
	registry := GetFixtureRegistry(ctx, rd.t, rd.sp.backup.cloud)
	fixtureMeta, err := registry.GetLatest(ctx, rd.sp.backup.fixture.Kind())
	if err != nil {
		rd.t.Skipf("fixture %s is not available: %s", rd.sp.backup.fixture.Kind(), err)
	}
	rd.fixtureMetadata = fixtureMeta
	url := registry.URI(fixtureMeta.DataPath)
	rd.collectionURI = url.String()
}

func (rd *restoreDriver) prepareCluster(ctx context.Context) {
	rd.c.Start(ctx, rd.t.L(),
		rd.roachprodOpts(),
		install.MakeClusterSettings(rd.defaultClusterSettings()...),
		rd.sp.hardware.getCRDBNodes())
	rd.getAOST(ctx)
}

// getAOST gets the AOST to use in the restore cmd.
func (rd *restoreDriver) getAOST(ctx context.Context) {
	if !rd.sp.fullBackupOnly {
		rd.aost = ""
		return
	}
	var aost string
	conn := rd.c.Conn(ctx, rd.t.L(), 1)
	defer conn.Close()
	aostCmd := rd.sp.getFullBackupEndTimeCmd(ctx, rd.t, rd.collectionURI)
	err := conn.QueryRowContext(ctx, aostCmd).Scan(&aost)
	require.NoError(rd.t, err, fmt.Sprintf("aost cmd failed: %s", aostCmd))
	rd.aost = aost
}

func (rd *restoreDriver) restoreCmd(ctx context.Context, target, opts string) string {
	var aostSubCmd string
	if rd.aost != "" {
		aostSubCmd = fmt.Sprintf("AS OF SYSTEM TIME '%s'", rd.aost)
	}
	query := fmt.Sprintf(
		`RESTORE %s FROM LATEST IN '%s' %s %s`, target, rd.collectionURI, aostSubCmd, opts,
	)
	rd.t.L().Printf("Running restore cmd: %s", query)
	return query
}

// run executes the restore, where target injects a restore target into the restore command.
// Examples:
// - "DATABASE tpcc" will execute a database restore on the tpcc cluster.
// - "" will execute a cluster restore.
func (rd *restoreDriver) run(ctx context.Context, target string) error {
	conn, err := rd.c.ConnE(ctx, rd.t.L(), 1)
	if err != nil {
		return errors.Wrapf(err, "failed to connect to node 1; running restore")
	}
	defer conn.Close()
	_, err = conn.ExecContext(
		ctx, rd.restoreCmd(ctx, target, "WITH unsafe_restore_incompatible_version"),
	)
	return err
}

func (rd *restoreDriver) runDetached(
	ctx context.Context, target string, node int,
) (jobspb.JobID, error) {
	db, err := rd.c.ConnE(ctx, rd.t.L(), rd.c.Node(node)[0])
	if err != nil {
		return 0, errors.Wrapf(err, "failed to connect to node %d; running restore detached", node)
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, rd.restoreCmd(
		ctx, target, "WITH DETACHED, unsafe_restore_incompatible_version",
	)); err != nil {
		return 0, err
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
func (rd *restoreDriver) initRestorePerfMetrics(
	ctx context.Context, durationGauge *prometheus.GaugeVec,
) func() {
	dut, err := roachtestutil.NewDiskUsageTracker(rd.c, rd.t.L())
	require.NoError(rd.t, err)
	startTime := timeutil.Now()
	startDu := dut.GetDiskUsage(ctx, rd.c.All())

	return func() {
		promLabel := registry.PromSub(strings.Replace(rd.sp.testName, "restore/", "", 1)) + "_seconds"
		testDuration := timeutil.Since(startTime).Seconds()
		durationGauge.WithLabelValues(promLabel).Set(testDuration)

		// compute throughput as MB / node / second.
		du := dut.GetDiskUsage(ctx, rd.c.All())
		throughput := float64(du-startDu) / (float64(rd.sp.hardware.nodes) * testDuration)
		rd.t.L().Printf("Usage %d , Nodes %d , Duration %f\n; Throughput: %f mb / node / second",
			du,
			rd.sp.hardware.nodes,
			testDuration,
			throughput)
		exportToRoachperf(ctx, rd.t, rd.c, rd.sp.testName, int64(throughput))
	}
}

// maybeValidateFingerprint runs a fingerprint on the restored database and
// checks it against the fingerprint stored in the fixture metadata. If the test
// is not configured to validate fingerprints, then it will skip the check.
// If the fixture metadata does not contain a fingerprint, then it will fail the
// test.
func (rd *restoreDriver) maybeValidateFingerprint(ctx context.Context) {
	if rd.sp.skipFingerprint {
		rd.t.L().Printf("restore spec is not configured to validate fingerprints; skipping fingerprint check.")
		return
	} else if rd.fixtureMetadata.Fingerprint == nil {
		rd.t.Fatalf("fixture metadata does not contain a fingerprint; cannot validate fingerprint")
	}

	conn, err := rd.c.ConnE(ctx, rd.t.L(), rd.c.Node(1)[0])
	require.NoError(rd.t, err)
	defer conn.Close()
	fingerprint := fingerprintDatabase(
		ctx, rd.t, conn, rd.sp.backup.fixture.DatabaseName(), "", /* aost */
	)
	require.Equal(rd.t, rd.fixtureMetadata.Fingerprint, fingerprint, "fingerprint mismatch after restore")
}

// exportToRoachperf exports a single perf metric for the given test to roachperf.
func exportToRoachperf(
	ctx context.Context, t test.Test, c cluster.Cluster, testName string, metric int64,
) {

	exporter := roachtestutil.CreateWorkloadHistogramExporter(t, c)

	// The easiest way to record a precise metric for roachperf is to caste it as a duration,
	// in seconds in the histogram's upper bound.
	reg := histogram.NewRegistryWithExporter(time.Duration(metric)*time.Second, histogram.MockWorkloadName, exporter)

	bytesBuf := bytes.NewBuffer([]byte{})
	writer := io.Writer(bytesBuf)

	exporter.Init(&writer)
	defer roachtestutil.CloseExporter(ctx, exporter, t, c, bytesBuf, c.Node(1), "")
	var err error
	// Ensure the histogram contains the name of the roachtest
	reg.GetHandle().Get(testName)

	// Serialize the histogram into the buffer
	reg.Tick(func(tick histogram.Tick) {
		err = tick.Exporter.SnapshotAndWrite(tick.Hist, tick.Now, tick.Elapsed, &tick.Name)
	})

	if err != nil {
		return
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
	url := "https://" + adminUIAddrs[0] + "/ts/query"

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
