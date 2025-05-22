// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// We want a roachtest which performs an index-backfill on a multi-store
// cluster while a write only workload is running.
func registerMultiStoreIndexBackfill(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "admission-control/multi-store-index-backfill",
		Owner:            registry.OwnerAdmissionControl,
		Timeout:          6 * time.Hour,
		Benchmark:        true,
		CompatibleClouds: registry.OnlyGCE,
		// TODO(kvoli): Enable this as part of asserting on the performance. Also
		// check other admission control tests which don't currently assert on
		// performance. See #111614.
		Suites: registry.ManualOnly,
		Cluster: r.MakeClusterSpec(
			10, /* nodeCount */
			spec.CPU(16),
			spec.WorkloadNode(),
			spec.WorkloadNodeCPU(16),
			spec.GCEVolumeType("pd-ssd"),
			spec.VolumeSize(200),
			spec.GCEVolumeCount(4),
		),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule),
				install.MakeClusterSettings(), c.CRDBNodes())
			// Approx. 1.5 TiB of (3x) replicated data, 4 billion rows.
			c.Run(ctx, option.WithNodes(c.WorkloadNode()),
				"./cockroach workload fixtures import bulkingest --a 2000 "+
					"--b 2000 --c 1000 --index-b-c-a=false --files-per-node=10 "+
					"--batches-by-b=false {pgurl:1}")

			m := c.NewMonitor(ctx, c.CRDBNodes())
			cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
				// This should use approx. 20% of the cluster's resources (disk/cpu).
				if err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(
					"./cockroach workload run kv --init --splits=100 --read-percent=0 "+
						"--max-rate=5000 --concurrency=1024 --min-block-bytes=4096 "+
						"--max-block-bytes=4096 --tolerate-errors {pgurl%s}",
					c.CRDBNodes())); err != nil && !errors.Is(err, context.Canceled) {
					// Expect the context be be canceled in the happy case.
					return err
				}
				return nil
			})
			m.Go(func(ctx context.Context) error {
				db := c.Conn(ctx, t.L(), 1)
				defer db.Close()
				defer cancelWorkload()

				start := timeutil.Now()
				t.Status("creating index b_c_a on bulkingest.bulkingest")
				_, err := db.Exec(`CREATE INDEX b_c_a ON bulkingest.bulkingest (b,c,a)`)
				t.Status("index b_c_a created in", timeutil.Since(start))
				return err
			})
			m.Wait()
		},
	})
}
