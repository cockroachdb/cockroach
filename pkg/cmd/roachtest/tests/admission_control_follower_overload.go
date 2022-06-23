// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package tests

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/prometheus"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/stretchr/testify/require"
)

func registerAdmissionControlFollowerOverload(r registry.Registry) {
	spec := func(subtest string, cfg admissionControlFollowerOverloadOpts) registry.TestSpec {
		return registry.TestSpec{
			Name:    "admission/follower-overload/" + subtest,
			Owner:   registry.OwnerKV,
			Timeout: 3 * time.Hour,
			// Don't re-use the cluster, since we don't have any conventions about
			// `wipe` removing any custom systemd units.
			//
			// NB: use 16vcpu machines to avoid getting anywhere close to EBS bandwidth limits
			// on AWS, see:
			// https://github.com/cockroachdb/cockroach/issues/82109#issuecomment-1154049976
			Cluster: r.MakeClusterSpec(4, spec.CPU(16), spec.ReuseNone(), spec.AvoidSSD(), spec.RAID0(false)),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runAdmissionControlFollowerOverload(ctx, t, c, cfg)
			},
		}
	}

	// The control group - just the vanilla cluster workloads, no nemesis. Running
	// this and looking at performance blips can give us an idea of what "normal"
	// looks like. This is most directly contrasted with presplit-with-leases but
	// since the workload on N3 barely needs any resources, it should also compare
	// well with presplit-no-leases.
	r.Add(spec("presplit-control", admissionControlFollowerOverloadOpts{
		kv0N12:         true,
		kvN12ExtraArgs: "--splits 100",
		kv50N3:         true,
	}))
	// n3 has no leases (but has disk overload), so n1 and n2 field all of the
	// active work but replicate to n3. The workload should be steady with good
	// p99s since there is no backpressure from n3 (at the time of writing) and
	// we're not sending it any foreground traffic. The quota pools shouldn't
	// deplete since writes are spread out evenly across 100 ranges.
	r.Add(spec("presplit-no-leases", admissionControlFollowerOverloadOpts{
		ioNemesis:      true,
		kv0N12:         true,
		kvN12ExtraArgs: "--splits 100",
	}))
	// Everything as before, but now the writes aren't spread out but all hit the
	// same range. This could lead to the quota pool on that range running
	// significantly emptier, possibly to the point of stalling foreground writes.
	r.Add(spec("hotspot-no-leases", admissionControlFollowerOverloadOpts{
		ioNemesis:      true,
		kv0N12:         true,
		kvN12ExtraArgs: "--sequential",
		kv50N3:         true,
	}))
	// This is identical to presplit-no-leases, but this time we are also running a
	// (small) workload against n3. Looking at the performance of this workload gives
	// us an idea of the impact of follower writes overload on a foreground workload.
	r.Add(spec("presplit-with-leases", admissionControlFollowerOverloadOpts{
		ioNemesis:      true,
		kv0N12:         true,
		kvN12ExtraArgs: "--splits=100",
		kv50N3:         true,
	}))

}

type admissionControlFollowerOverloadOpts struct {
	ioNemesis      bool
	kv0N12         bool
	kvN12ExtraArgs string
	kv50N3         bool
}

func runAdmissionControlFollowerOverload(
	ctx context.Context, t test.Test, c cluster.Cluster, cfg admissionControlFollowerOverloadOpts,
) {
	require.False(t, c.IsLocal())

	resetSystemdUnits := func() {
		for _, cmd := range []string{"stop", "reset-failed"} {
			_ = c.RunE(ctx, c.Node(4), "sudo", "systemctl", cmd, "kv-n12")
			_ = c.RunE(ctx, c.Node(4), "sudo", "systemctl", cmd, "kv-n3")
		}
	}

	// Make cluster re-use possible to iterate on this test without making a new
	// cluster every time.
	const dev = true
	if dev {
		resetSystemdUnits()
	}

	// Set up prometheus.
	{
		clusNodes := c.Range(1, c.Spec().NodeCount-1)
		workloadNode := c.Node(c.Spec().NodeCount)
		promNode := workloadNode
		cfg := (&prometheus.Config{}).
			WithPrometheusNode(promNode).
			WithGrafanaDashboard("https://gist.githubusercontent.com/tbg/f238d578269143187e71a1046562225f/raw").
			WithCluster(clusNodes).
			WithNodeExporter(clusNodes).
			WithWorkload(workloadNode, 2112). // kv-n12
			WithWorkload(workloadNode, 2113)  // kv-n3 (if present)

		p, saveSnap, err := prometheus.Init(
			ctx,
			*cfg,
			c,
			t.L(),
			repeatRunner{C: c, T: t}.repeatRunE,
		)
		require.NoError(t, err)
		defer saveSnap(t.ArtifactsDir())
		_ = p
	}

	phaseDuration := 3 * time.Minute // TODO time.Hour

	nodes := c.Range(1, 3)
	c.Put(ctx, t.Cockroach(), "cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), nodes)
	db := c.Conn(ctx, t.L(), 1)
	require.NoError(t, WaitFor3XReplication(ctx, t, db))

	if cfg.kv0N12 {
		args := strings.Fields("./cockroach workload init kv {pgurl:1}")
		args = append(args, strings.Fields(cfg.kvN12ExtraArgs)...)
		c.Run(ctx, c.Node(1), args...)
	}
	if cfg.kv50N3 {
		args := strings.Fields("./cockroach workload init kv --db kvn3 {pgurl:1}")
		c.Run(ctx, c.Node(1), args...)
	}

	// Node 3 should not have any leases (excepting kvn3, if present).
	runner := sqlutils.MakeSQLRunner(db)
	for _, row := range runner.QueryStr(
		t, `SELECT target FROM [ SHOW ZONE CONFIGURATIONS ]`,
	) {
		q := `ALTER ` + row[0] + ` CONFIGURE ZONE USING lease_preferences = '[[-node3]]'`
		t.L().Printf("%s", q)
		_, err := db.Exec(q)
		require.NoError(t, err)
	}
	if cfg.kv50N3 {
		q := `ALTER DATABASE kvn3 CONFIGURE ZONE USING lease_preferences = '[[+node3]]', constraints = COPY FROM PARENT`
		t.L().Printf("%s", q)
		runner.Exec(t, q)
	}

	{
		var attempts int
		for ctx.Err() == nil {
			attempts++
			m1 := runner.QueryStr(t, `SELECT range_id FROM crdb_internal.ranges WHERE lease_holder=3 AND database_name != 'kvn3'`)
			m2 := runner.QueryStr(t, `SELECT range_id FROM crdb_internal.ranges WHERE lease_holder!=3 AND database_name = 'kvn3'`)
			if len(m1)+len(m2) == 0 {
				t.L().Printf("done waiting for lease movement")
				break
			}
			if len(m1) > 0 {
				t.L().Printf("waiting for %d range leases to move off n3: %v", len(m1), m1)
			}
			if len(m2) > 0 {
				t.L().Printf("waiting for %d range leases to move to n3: %v", len(m2), m2)
			}

			time.Sleep(10 * time.Second)
			require.Less(t, attempts, 100)
		}
	}

	if cfg.kv0N12 {
		// Deploy workload against the default kv database (which has no leases on
		// n3) and let it run for a phase duration. This does not block and keeps
		// running even after the test tears down. Initially, the below workload was
		// configured for 400 requests per second with 10k blocks, amounting to
		// 4mb/s of goodput. Experimentally this was observed to cause (after ~8h) a
		// per-store read throughput of ~60mb/s and write throughput of ~140mb/s for
		// a total of close to 200mb/s (per store). This was too much for default
		// EBS disks (see below) and there was unpredictable performance when
		// reprovisioning such volumes with higher throughput, so we run at 1mb/s
		// which should translate to ~50mb/s of max sustained combined throughput.
		//
		// NB: on GCE pd-ssd, we get 30 IOPS/GB of (combined) throughput and
		// 0.45MB/(GB*s) for each GB provisioned, so for the 500GB volumes in this
		// test 15k IOPS and 225MB/s.
		//
		// See: https://cloud.google.com/compute/docs/disks/performance#footnote-1
		//
		// On AWS, the default EBS volumes have 3000 IOPS and 125MB/s combined
		// throughput. Additionally, instances have a throughput limit for talking
		// to EBS, see:
		//
		// https://github.com/cockroachdb/cockroach/issues/82109#issuecomment-1154049976
		deployWorkload := `
mkdir -p logs &&
sudo systemd-run --property=Type=exec
--property=StandardOutput=file:/home/ubuntu/logs/kv-n12.stdout.log
--property=StandardError=file:/home/ubuntu/logs/kv-n12.stderr.log
--remain-after-exit --unit kv-n12 -- ./cockroach workload run kv --read-percent 0
--max-rate 400 --concurrency 400 --min-block-bytes 2500 --max-block-bytes 2500 --tolerate-errors {pgurl:1-2}`
		c.Run(ctx, c.Node(4), strings.Fields(deployWorkload)...)
	}
	if cfg.kv50N3 {
		// On n3, we run a "trickle" workload that does not add much work to the
		// system but which we can use to establish to monitor the impact of the
		// overload on the follower to its foreground traffic. All leases for this
		// workload are held by n3.
		const deployWorkload = `
sudo systemd-run --property=Type=exec
--property=StandardOutput=file:/home/ubuntu/logs/kv-n3.stdout.log
--property=StandardError=file:/home/ubuntu/logs/kv-n3.stderr.log
--remain-after-exit --unit kv-n3 -- ./cockroach workload run kv --db kvn3
--read-percent 50 --max-rate 100 --concurrency 1000 --min-block-bytes 100 --max-block-bytes 100
--prometheus-port 2113 --tolerate-errors {pgurl:3}`
		c.Run(ctx, c.Node(4), strings.Fields(deployWorkload)...)
	}
	t.L().Printf("deployed workload")

	wait(c.NewMonitor(ctx, nodes), phaseDuration)

	if cfg.ioNemesis {
		// Limit write throughput on s3 to 20mb/s. This is not enough to keep up
		// with the workload, at least not in the long run, due to write amp.
		c.Run(ctx, c.Node(3), "sudo", "systemctl", "set-property", "cockroach", "IOWriteBandwidthMax={store-dir} 20971520")
		t.L().Printf("installed write throughput limit on n3")
	}

	wait(c.NewMonitor(ctx, nodes), phaseDuration)

	// TODO collect, assert on, and export metrics, using:
	// https://github.com/cockroachdb/cockroach/pull/80724.
	// Things to check:
	// - LSM health of follower (and, to be sure, on other replicas)
	// -Latency of a benign read-only workload on the follower
	// - Comparison of baseline perf of kv0 workload before disk nemesis (i.e.
	//   run first without nemesis, then with nemesis, maybe again without, make
	//   sure they're all sort of comparable, or report all three, or something
	//   like that. At first probably just export the overall coefficient of
	//   variation or something like that and leave detailed interpretation to
	//   human eyes on roachperf.
	t.Fatal("failing on purpose")
}

func wait(m cluster.Monitor, duration time.Duration) {
	m.Go(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(duration):
			return nil
		}
	})
	m.Wait()
}
