// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

const (
	singleNodeIndexBackfillCustomers       = 25_000
	singleNodeIndexBackfillActiveCustomers = 5_000
)

// registerSingleNodeIndexBackfill registers tests that run an index backfill on
// a single node cluster with disk bandwidth limited via cgroups. The test
// imports TPC-E data (without disk limiting for speed), then applies disk
// bandwidth limits before running the workload and index backfill concurrently.
//
// The test uses disk snapshots to speed up repeated runs - on first run it
// creates a snapshot after TPC-E init using a published CRDB release (so the
// snapshot has a well-defined internal version), and subsequent runs reuse that
// snapshot.
//
// Two variants are registered:
// - One without AC provisioned bandwidth setting (just cgroup limiting)
// - One with AC provisioned bandwidth set to match the cgroup limit
func registerSingleNodeIndexBackfill(r registry.Registry) {
	for _, withProvisionedBandwidth := range []bool{false, true} {
		withProvisionedBandwidth := withProvisionedBandwidth // capture loop variable

		nameSuffix := fmt.Sprintf("/provisioned-bandwidth=%t", withProvisionedBandwidth)
		// Keep prefix short to fit GCE's 63 character limit for snapshot names.
		snapshotPrefix := "snibf-tpce"
		if withProvisionedBandwidth {
			snapshotPrefix += "-bw"
		}

		// Snapshot prefix includes customer count so we can scale up later
		// without conflicting with existing snapshots.
		snapshotPrefix = fmt.Sprintf("%s-%d", snapshotPrefix, singleNodeIndexBackfillCustomers)

		r.Add(registry.TestSpec{
			Name:             "admission-control/single-node-index-backfill" + nameSuffix,
			Owner:            registry.OwnerAdmissionControl,
			Timeout:          12 * time.Hour,
			Benchmark:        true,
			CompatibleClouds: registry.OnlyGCE,
			Suites:           registry.Suites(registry.Weekly),
			Cluster: r.MakeClusterSpec(
				2, // 1 CRDB node + 1 workload node
				// 16vCPUs so that CPU is not the bottleneck.
				spec.CPU(16),
				spec.WorkloadNode(),
				// The use of snapshots requires workload nodes to also have an attached disk.
				spec.WorkloadRequiresDisk(),
				spec.WorkloadNodeCPU(16),
				spec.VolumeSize(500),
				spec.ReuseNone(),
				spec.Arch(spec.AllExceptFIPS),
			),
			SnapshotPrefix: snapshotPrefix,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runSingleNodeIndexBackfill(ctx, t, c, withProvisionedBandwidth)
			},
		})
	}
}

const customers = singleNodeIndexBackfillCustomers

func runSingleNodeIndexBackfill(
	ctx context.Context, t test.Test, c cluster.Cluster, withProvisionedBandwidth bool,
) {
	const (
		activeCustomers      = singleNodeIndexBackfillActiveCustomers
		provisionedBandwidth = 128 << 20 // 128 MiB/s
	)

	if c.Spec().NodeCount != 2 {
		t.Fatalf("expected 2 nodes, found %d", c.Spec().NodeCount)
	}

	if !t.SkipInit() {
		doInitSingleNodeIndexBackfill(ctx, t, c)
	}

	// Set up Prometheus for metrics collection.
	promCfg := &prometheus.Config{}
	promCfg.WithPrometheusNode(c.WorkloadNode().InstallNodes()[0]).
		WithNodeExporter(c.CRDBNodes().InstallNodes()).
		WithCluster(c.CRDBNodes().InstallNodes()).
		WithGrafanaDashboardJSON(grafana.SnapshotAdmissionControlGrafanaJSON)
	if err := c.StartGrafana(ctx, t.L(), promCfg); err != nil {
		t.Fatal(err)
	}

	// Start the cluster (or restart after snapshot creation) with the current
	// version. This will upgrade the data if the snapshot was created with an
	// older version.
	t.Status("starting cluster for workload")
	startOpts := option.NewStartOpts(option.NoBackupSchedule)
	startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
		"--vmodule=io_load_listener=2")
	roachtestutil.SetDefaultSQLPort(c, &startOpts.RoachprodOpts)
	roachtestutil.SetDefaultAdminUIPort(c, &startOpts.RoachprodOpts)
	settings := install.MakeClusterSettings(install.NumRacksOption(1))
	if err := c.StartE(ctx, t.L(), startOpts, settings, c.CRDBNodes()); err != nil {
		t.Fatal(err)
	}

	// Set up Prometheus client for metrics collection during index backfill.
	promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), promCfg)
	require.NoError(t, err)
	statCollector := clusterstats.NewStatsCollector(ctx, promClient)

	// Initialize TPC-E spec for running workload commands.
	tpceSpec, err := initTPCESpec(ctx, t.L(), c)
	require.NoError(t, err)

	// Configure cluster settings for workload phase.
	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	// Enable admission control first since we may override some settings below.
	roachtestutil.SetAdmissionControl(ctx, t, c, true)

	// Disable elastic CPU control for index backfill so it is not throttled.
	if _, err := db.ExecContext(
		ctx, "SET CLUSTER SETTING bulkio.index_backfill.elastic_control.enabled = false",
	); err != nil {
		t.Fatal(err)
	}

	// Increase index backfill parallelism to utilize more vCPUs.
	if _, err := db.ExecContext(
		ctx, "SET CLUSTER SETTING bulkio.index_backfill.ingest_concurrency = 8",
	); err != nil {
		t.Fatal(err)
	}

	// Disable elastic CPU control completely, so that this test can focus
	// on the disk bandwidth bottleneck. NB: this makes the other elastic
	// CPU settings irrelevant, but we leave them there for future reference,
	// in case we revisit this decision.
	//
	// TODO(sumeer): revisit.
	if _, err := db.ExecContext(
		ctx, "SET CLUSTER SETTING admission.elastic_cpu.enabled = false",
	); err != nil {
		t.Fatal(err)
	}

	// Configure elastic CPU settings so that all unknown elastic work can also
	// proceed fast in this context.
	if _, err := db.ExecContext(
		ctx, "SET CLUSTER SETTING admission.elastic_cpu.adjustment_delta_per_second = 0.01",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx, "SET CLUSTER SETTING admission.elastic_cpu.inactive_point = 0.2",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx, "SET CLUSTER SETTING admission.elastic_cpu.multiplicative_factor_on_inactive_decrease = 0.1",
	); err != nil {
		t.Fatal(err)
	}

	// Set up cgroup disk staller to limit disk bandwidth to 128 MiB/s.
	t.Status(fmt.Sprintf("limiting disk bandwidth to %d bytes/s via cgroups", provisionedBandwidth))
	staller := roachtestutil.MakeCgroupDiskStaller(t, c,
		true /* readsToo */, false /* logsToo */, false /* disableStateValidation */)
	staller.Setup(ctx)
	staller.Slow(ctx, c.CRDBNodes(), provisionedBandwidth /* bytesPerSecond */)

	// Optionally set the AC provisioned bandwidth cluster setting.
	if withProvisionedBandwidth {
		t.Status("setting kvadmission.store.provisioned_bandwidth = '128MiB'")
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING kvadmission.store.provisioned_bandwidth = '128MiB'",
		); err != nil {
			t.Fatal(err)
		}
		t.Status("setting kvadmission.store.elastic_disk_bandwidth_max_util = 1.0, to attempt to fully utilize the disk bandwidth")
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING kvadmission.store.elastic_disk_bandwidth_max_util = 1.0",
		); err != nil {
			t.Fatal(err)
		}
	}

	// Increase compaction concurrency to better utilize 16 vCPUs. This
	// also ensures compaction concurrency does not become a bottleneck for
	// writes to the store.
	if _, err := db.ExecContext(
		ctx, "SET CLUSTER SETTING storage.compaction_concurrency = 4",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx, "SET CLUSTER SETTING storage.max_compaction_concurrency = 6",
	); err != nil {
		t.Fatal(err)
	}

	// Run TPC-E workload, KV0 workload, and index backfill concurrently.
	workloadDuration := 120 * time.Minute
	m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())
	var backfillDuration time.Duration
	var totalBWSamples []float64

	// Goroutine 1: Run TPC-E workload.
	m.Go(func(ctx context.Context) error {
		t.Status(fmt.Sprintf("starting TPC-E workload with %d active customers (<%s)",
			activeCustomers, workloadDuration))
		runOptions := tpceCmdOptions{
			customers:       customers,
			activeCustomers: activeCustomers,
			racks:           1,
			duration:        workloadDuration,
			threads:         256,
			skipCleanup:     true,
			connectionOpts:  defaultTPCEConnectionOpts(),
		}
		result, err := tpceSpec.run(ctx, t, c, runOptions)
		if err != nil {
			t.L().Printf("TPC-E workload error: %v", err)
			return err
		}
		t.L().Printf("TPC-E workload output:\n%s\n", result.Stdout)
		return nil
	})

	// Goroutine 2: Run KV0 (all writes) workload to add additional write load.
	// This generates ~5 MiB/s of writes (5000 ops/s × 1024 bytes). This adds
	// some background write load to the cluster, which TPC-E lacks.
	m.Go(func(ctx context.Context) error {
		t.Status("starting KV0 workload (all writes, ~5 MiB/s)")
		const (
			kvBlockSize   = 1024
			kvConcurrency = 20
			kvSplits      = 100
			kvMaxRate     = 5000
		)
		cmd := fmt.Sprintf(
			"./cockroach workload run kv --init --read-percent=0 "+
				"--min-block-bytes=%d --max-block-bytes=%d "+
				"--concurrency=%d --splits=%d --max-rate=%d "+
				"--duration=%s "+
				"--tolerate-errors "+
				"{pgurl:1}",
			kvBlockSize, kvBlockSize,
			kvConcurrency, kvSplits, kvMaxRate,
			workloadDuration,
		)
		err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
		if err != nil {
			t.L().Printf("KV0 workload error: %v", err)
			return err
		}
		t.L().Printf("KV0 workload completed")
		return nil
	})

	// Goroutine 3: Run index backfill after a short baseline period, with
	// metrics collection.
	m.Go(func(ctx context.Context) error {
		// Wait for workload to stabilize before starting index backfill.
		t.Status(fmt.Sprintf("recording baseline performance (<%s)", 5*time.Minute))
		time.Sleep(5 * time.Minute)

		t.Status("starting index creation on tpce.cash_transaction")
		indexName := fmt.Sprintf("index_%s", timeutil.Now().Format("20060102_T150405"))

		// Start metrics collection goroutine that runs during index backfill.
		stopMetrics := make(chan struct{})
		metricsDone := make(chan struct{})
		t.Go(func(context.Context, *logger.Logger) error {
			defer close(metricsDone)
			writeBWQuery := divQuery("rate(sys_host_disk_write_bytes[1m])", 1<<20)
			readBWQuery := divQuery("rate(sys_host_disk_read_bytes[1m])", 1<<20)

			getMetricVal := func(query string) (float64, error) {
				point, err := statCollector.CollectPoint(ctx, t.L(), timeutil.Now(), query)
				if err != nil {
					return 0, err
				}
				for _, v := range point["node"] {
					return v.Value, nil
				}
				return 0, fmt.Errorf("no data for query %s", query)
			}

			t.L().Printf("=== INDEX BACKFILL METRICS COLLECTION STARTED (1m interval) ===")
			ticker := time.NewTicker(1 * time.Minute)
			defer ticker.Stop()

			iteration := 0
			for {
				select {
				case <-ticker.C:
					iteration++
					writeBW, writeErr := getMetricVal(writeBWQuery)
					readBW, readErr := getMetricVal(readBWQuery)
					if writeErr != nil || readErr != nil {
						t.L().Printf("[metrics %d] error collecting: write=%v, read=%v",
							iteration, writeErr, readErr)
						continue
					}
					totalBW := writeBW + readBW
					totalBWSamples = append(totalBWSamples, totalBW)
					t.L().Printf("[metrics %d] disk bandwidth: read=%.2f MiB/s, write=%.2f MiB/s, total=%.2f MiB/s",
						iteration, readBW, writeBW, totalBW)
				case <-stopMetrics:
					t.L().Printf("=== INDEX BACKFILL METRICS COLLECTION STOPPED ===")
					return nil
				case <-ctx.Done():
					return nil
				}
			}
		}, task.Name("metrics-collector"))

		// Run the actual index creation.
		backfillStart := timeutil.Now()
		_, err := db.ExecContext(ctx,
			fmt.Sprintf("CREATE INDEX %s ON tpce.cash_transaction (ct_dts)", indexName),
		)
		backfillDuration = timeutil.Since(backfillStart)

		// Stop metrics collection.
		close(stopMetrics)
		<-metricsDone

		if err != nil {
			t.L().Printf("index creation error: %v", err)
			return err
		}
		t.L().Printf("index backfill completed in %s", backfillDuration)
		t.Status("finished index creation")
		return nil
	})

	m.Wait()

	// Export index backfill duration to roachperf.
	c.Run(ctx, option.WithNodes(c.CRDBNodes()), "mkdir", "-p", t.PerfArtifactsDir())
	perfResult := fmt.Sprintf(`{"index_backfill_duration_secs": %.1f}`, backfillDuration.Seconds())
	c.Run(ctx, option.WithNodes(c.CRDBNodes()),
		fmt.Sprintf(`echo '%s' > %s/stats.json`, perfResult, t.PerfArtifactsDir()))
	t.L().Printf("roachperf stats: %s", perfResult)

	// Validate pass/fail criteria. Thresholds are intentionally loose to
	// tolerate run-to-run variance while catching severe regressions (e.g.,
	// AC over-throttling the backfill or starving the disk).
	const (
		maxBackfillDuration = 90 * time.Minute
		minAvgTotalBW       = 50.0 // MiB/s
	)
	if backfillDuration > maxBackfillDuration {
		t.Fatalf("index backfill took %s, exceeding maximum of %s",
			backfillDuration, maxBackfillDuration)
	}
	if len(totalBWSamples) > 0 {
		var sum float64
		for _, s := range totalBWSamples {
			sum += s
		}
		avgTotalBW := sum / float64(len(totalBWSamples))
		t.L().Printf("average total disk bandwidth during backfill: %.2f MiB/s (%d samples)",
			avgTotalBW, len(totalBWSamples))
		if avgTotalBW < minAvgTotalBW {
			t.Fatalf("average total disk bandwidth %.2f MiB/s is below minimum of %.0f MiB/s",
				avgTotalBW, minAvgTotalBW)
		}
	}

	t.Status("test completed successfully")
}

func doInitSingleNodeIndexBackfill(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Check for existing snapshots.
	snapshots, err := c.ListSnapshots(ctx, vm.VolumeSnapshotListOpts{
		NamePrefix: t.SnapshotPrefix(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(snapshots) == 0 {
		// No existing snapshots - need to initialize TPC-E and create snapshots.
		// Use a published CRDB release so the snapshot has a well-defined internal
		// version that can be upgraded to any newer version.
		t.L().Printf("=== NO EXISTING SNAPSHOTS FOUND for prefix %q ===", t.SnapshotPrefix())
		t.L().Printf("=== WILL CREATE NEW SNAPSHOTS after TPC-E init ===")

		// Get the latest predecessor release version.
		pred, err := release.LatestPredecessor(t.BuildVersion())
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Using predecessor release %s for snapshot creation", pred)

		// Upload the predecessor binary to all nodes.
		path, err := clusterupgrade.UploadCockroach(
			ctx, t, t.L(), c, c.All(), clusterupgrade.MustParseVersion(pred),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Copy over the binary to ./cockroach and run it from there. This test
		// captures disk snapshots, which are fingerprinted using the binary
		// version found in this path.
		c.Run(ctx, option.WithNodes(c.All()), fmt.Sprintf("cp %s ./cockroach", path))

		// Start the cluster with rack locality configured (required by TPC-E init).
		t.Status(fmt.Sprintf("starting cluster with %s for snapshot creation", pred))
		startOpts := option.NewStartOpts(option.NoBackupSchedule)
		roachtestutil.SetDefaultSQLPort(c, &startOpts.RoachprodOpts)
		settings := install.MakeClusterSettings(install.NumRacksOption(1))
		if err := c.StartE(ctx, t.L(), startOpts, settings, c.CRDBNodes()); err != nil {
			t.Fatal(err)
		}

		// Initialize TPC-E spec for running workload commands.
		tpceSpec, err := initTPCESpec(ctx, t.L(), c)
		require.NoError(t, err)

		// Configure cluster settings before import.
		db := c.Conn(ctx, t.L(), 1)

		// Disable automatic stats collection to speed up import.
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false",
		); err != nil {
			db.Close()
			t.Fatal(err)
		}

		// Disable elastic CPU control for import so it is not throttled.
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING bulkio.import.elastic_control.enabled = false",
		); err != nil {
			db.Close()
			t.Fatal(err)
		}
		db.Close()

		// Import TPC-E data.
		t.Status(fmt.Sprintf("initializing TPC-E with %d customers for snapshot creation", customers))
		tpceSpec.init(ctx, t, c, tpceCmdOptions{
			customers:      customers,
			racks:          1, // single node
			connectionOpts: defaultTPCEConnectionOpts(),
		})

		// Stop all nodes before capturing cluster snapshots.
		t.Status("stopping cluster to create snapshots")
		c.Stop(ctx, t.L(), option.DefaultStopOpts())

		// Create snapshots.
		t.Status(fmt.Sprintf("creating snapshots with prefix %q", t.SnapshotPrefix()))
		snapshots, err = c.CreateSnapshot(ctx, t.SnapshotPrefix())
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("=== CREATED %d NEW SNAPSHOT(S) with prefix %q ===", len(snapshots), t.SnapshotPrefix())
	} else {
		// Existing snapshots found - copy data from snapshot to local disk.
		// We use a different approach than ApplySnapshots to avoid the GCE
		// snapshot hydration problem where reads from a snapshot-based disk
		// are very slow (~50 MB/s with 250ms latency) until fully hydrated.
		//
		// Instead, we:
		// 1. Create a temporary disk from the snapshot
		// 2. Attach it as a secondary disk
		// 3. Copy files to the primary (non-snapshot) disk
		// 4. Detach and delete the temporary disk
		//
		// This way, reads and writes to the primary disk are fast, and we
		// only pay the slow snapshot read cost once during the copy.
		t.L().Printf("=== FOUND %d EXISTING SNAPSHOT(S) with prefix %q ===", len(snapshots), t.SnapshotPrefix())
		t.L().Printf("=== WILL COPY DATA FROM SNAPSHOT (avoiding hydration problem) ===")

		// Find the snapshot for the CRDB node (node 1).
		var crdbSnapshot vm.VolumeSnapshot
		for _, snap := range snapshots {
			// Snapshot names end with node number, e.g., "...-n2-0001"
			if strings.HasSuffix(snap.Name, "-0001") {
				crdbSnapshot = snap
				break
			}
		}
		if crdbSnapshot.ID == "" {
			t.Fatal("could not find snapshot for CRDB node (node 1)")
		}
		t.L().Printf("Using snapshot %s (ID: %s)", crdbSnapshot.Name, crdbSnapshot.ID)

		// Get the zone once from GCE metadata and validate it.
		// This is more robust than repeating the metadata call for each gcloud command.
		t.Status("getting GCE zone from instance metadata")
		getZoneCmd := `curl -sf -H "Metadata-Flavor: Google" ` +
			`http://metadata.google.internal/computeMetadata/v1/instance/zone`
		zoneOutput, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.CRDBNodes()), getZoneCmd)
		if err != nil {
			t.Fatalf("failed to get zone from GCE metadata: %v", err)
		}
		// Zone format: projects/PROJECT_NUMBER/zones/ZONE_NAME
		zoneParts := strings.Split(strings.TrimSpace(zoneOutput.Stdout), "/")
		if len(zoneParts) < 4 || zoneParts[len(zoneParts)-2] != "zones" {
			t.Fatalf("unexpected zone format from metadata: %q", zoneOutput.Stdout)
		}
		zone := zoneParts[len(zoneParts)-1]
		t.L().Printf("GCE zone: %s", zone)

		// Create a temporary disk from the snapshot.
		tempDiskName := fmt.Sprintf("%s-temp-snapshot", c.Name())
		t.Status("creating temporary disk from snapshot")
		t.L().Printf("=== CREATING TEMP DISK %s FROM SNAPSHOT ===", tempDiskName)

		createDiskCmd := fmt.Sprintf(
			`gcloud compute disks create %s --source-snapshot=%s --zone=%s --type=pd-ssd --quiet`,
			tempDiskName, crdbSnapshot.ID, zone)
		if err := c.RunE(ctx, option.WithNodes(c.CRDBNodes()), createDiskCmd); err != nil {
			t.Fatalf("failed to create disk from snapshot: %v", err)
		}

		// Attach the temporary disk to the CRDB node.
		t.Status("attaching temporary snapshot disk")
		t.L().Printf("=== ATTACHING TEMP DISK ===")
		attachCmd := fmt.Sprintf(
			`gcloud compute instances attach-disk $(hostname) --disk=%s --zone=%s --device-name=snapshot-disk --quiet`,
			tempDiskName, zone)
		if err := c.RunE(ctx, option.WithNodes(c.CRDBNodes()), attachCmd); err != nil {
			t.Fatalf("failed to attach snapshot disk: %v", err)
		}

		// Mount the temporary disk.
		t.Status("mounting snapshot disk")
		t.L().Printf("=== MOUNTING SNAPSHOT DISK ===")
		mountCmd := `sudo mkdir -p /mnt/snapshot && ` +
			`sudo mount -o ro /dev/disk/by-id/google-snapshot-disk /mnt/snapshot`
		if err := c.RunE(ctx, option.WithNodes(c.CRDBNodes()), mountCmd); err != nil {
			t.Fatalf("failed to mount snapshot disk: %v", err)
		}

		// Wipe existing data and copy from snapshot with progress indicator.
		// Using rsync -ah --progress to show progress during copy.
		t.Status("copying data from snapshot to local disk")
		t.L().Printf("=== COPYING DATA FROM SNAPSHOT (this may take a few minutes) ===")
		copyCmd := `echo "Source data size:" && du -sh /mnt/snapshot/cockroach && ` +
			`echo "Wiping /mnt/data1/cockroach..." && ` +
			`sudo rm -rf /mnt/data1/cockroach && ` +
			`echo "Starting copy with rsync..." && ` +
			`sudo rsync -ah --info=progress2 /mnt/snapshot/cockroach /mnt/data1/ && ` +
			`echo "Copy complete. Final size:" && ` +
			`du -sh /mnt/data1/cockroach`
		if err := c.RunE(ctx, option.WithNodes(c.CRDBNodes()), copyCmd); err != nil {
			t.Fatalf("failed to copy data from snapshot: %v", err)
		}
		t.L().Printf("=== DATA COPY COMPLETE ===")

		// Unmount and detach the temporary disk.
		t.Status("cleaning up temporary snapshot disk")
		t.L().Printf("=== UNMOUNTING AND DETACHING TEMP DISK ===")
		unmountCmd := `sudo umount /mnt/snapshot && sudo rmdir /mnt/snapshot`
		if err := c.RunE(ctx, option.WithNodes(c.CRDBNodes()), unmountCmd); err != nil {
			t.L().Printf("Warning: failed to unmount snapshot disk: %v", err)
		}

		detachCmd := fmt.Sprintf(
			`gcloud compute instances detach-disk $(hostname) --disk=%s --zone=%s --quiet`,
			tempDiskName, zone)
		if err := c.RunE(ctx, option.WithNodes(c.CRDBNodes()), detachCmd); err != nil {
			t.L().Printf("Warning: failed to detach snapshot disk: %v", err)
		}

		// Delete the temporary disk.
		t.L().Printf("=== DELETING TEMP DISK ===")
		deleteCmd := fmt.Sprintf(
			`gcloud compute disks delete %s --zone=%s --quiet`,
			tempDiskName, zone)
		if err := c.RunE(ctx, option.WithNodes(c.CRDBNodes()), deleteCmd); err != nil {
			t.L().Printf("Warning: failed to delete temporary disk: %v", err)
		}

		t.L().Printf("=== SNAPSHOT DATA SUCCESSFULLY COPIED TO LOCAL DISK ===")
	}
}

// Index backfill results on master as of Feb 1, 2026:

// Without the provisioned bandwidth set, the backfill
// took ~38min.
//
// The cgroup limiting to 128MiB/s is not very effective, since we
// see the 1min rates exceed 128MiB/s (without the provisioned bandwidth set):
// [metrics 11] disk bandwidth: read=27.26 MiB/s, write=111.12 MiB/s, total=138.38 MiB/s
// [metrics 12] disk bandwidth: read=34.36 MiB/s, write=108.92 MiB/s, total=143.28 MiB/s
// [metrics 13] disk bandwidth: read=44.72 MiB/s, write=130.52 MiB/s, total=175.24 MiB/s
// [metrics 14] disk bandwidth: read=40.98 MiB/s, write=130.78 MiB/s, total=171.77 MiB/s
// [metrics 15] disk bandwidth: read=47.38 MiB/s, write=129.66 MiB/s, total=177.04 MiB/s
// [metrics 16] disk bandwidth: read=36.05 MiB/s, write=88.03 MiB/s, total=124.08 MiB/s
// [metrics 17] disk bandwidth: read=33.54 MiB/s, write=109.46 MiB/s, total=143.00 MiB/s
// [metrics 18] disk bandwidth: read=26.79 MiB/s, write=58.79 MiB/s, total=85.58 MiB/s
// [metrics 19] disk bandwidth: read=48.01 MiB/s, write=118.73 MiB/s, total=166.75 MiB/s
// [metrics 20] disk bandwidth: read=45.58 MiB/s, write=128.43 MiB/s, total=174.01 MiB/s
// [metrics 21] disk bandwidth: read=39.16 MiB/s, write=89.80 MiB/s, total=128.95 MiB/s
// [metrics 22] disk bandwidth: read=26.91 MiB/s, write=124.97 MiB/s, total=151.89 MiB/s
// [metrics 23] disk bandwidth: read=74.38 MiB/s, write=129.95 MiB/s, total=204.33 MiB/s
// [metrics 24] disk bandwidth: read=56.46 MiB/s, write=56.99 MiB/s, total=113.46 MiB/s
// [metrics 25] disk bandwidth: read=58.86 MiB/s, write=89.39 MiB/s, total=148.25 MiB/s
// [metrics 26] disk bandwidth: read=53.40 MiB/s, write=58.13 MiB/s, total=111.53 MiB/s
// [metrics 27] disk bandwidth: read=56.57 MiB/s, write=89.15 MiB/s, total=145.72 MiB/s
//
// With the provisioned bandwidth set, the backfill took 57min, even though
// it was set to fully utilize the disk bandwidth. We see disk bandwidth
// sequences like:
// [metrics 15] disk bandwidth: read=19.19 MiB/s, write=79.17 MiB/s, total=98.36 MiB/s
// [metrics 16] disk bandwidth: read=29.32 MiB/s, write=112.51 MiB/s, total=141.83 MiB/s
// [metrics 17] disk bandwidth: read=15.61 MiB/s, write=83.52 MiB/s, total=99.13 MiB/s
// [metrics 18] disk bandwidth: read=26.57 MiB/s, write=109.58 MiB/s, total=136.15 MiB/s
// [metrics 19] disk bandwidth: read=15.96 MiB/s, write=84.80 MiB/s, total=100.76 MiB/s
// [metrics 20] disk bandwidth: read=33.39 MiB/s, write=123.29 MiB/s, total=156.68 MiB/s
// [metrics 21] disk bandwidth: read=12.52 MiB/s, write=93.12 MiB/s, total=105.65 MiB/s
// [metrics 22] disk bandwidth: read=35.68 MiB/s, write=114.89 MiB/s, total=150.57 MiB/s
// [metrics 23] disk bandwidth: read=19.50 MiB/s, write=93.59 MiB/s, total=113.09 MiB/s
// [metrics 24] disk bandwidth: read=21.38 MiB/s, write=71.85 MiB/s, total=93.23 MiB/s
//
// One of the causes of underutilization of bandwidth is write-amp model
// fluctuation, noted in
// https://github.com/cockroachdb/cockroach/issues/146223#issuecomment-3786601809.
