// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func registerHotSpotSplits(r registry.Registry) {
	// This test sets up a cluster and runs kv on it with high concurrency and a large block size
	// to force a large range. We then make sure that the largest range isn't larger than a threshold and
	// that backpressure is working correctly.
	runHotSpot := func(ctx context.Context, t test.Test, c cluster.Cluster, duration time.Duration, concurrency int) {
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())

		c.Run(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload init kv --drop {pgurl:1}`)

		m := t.NewGroup(task.WithContext(ctx))

		m.Go(func(ctx context.Context, l *logger.Logger) error {
			l.Printf("starting load generator\n")

			const blockSize = 1 << 18 // 256 KB
			return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(
				"./cockroach workload run kv --read-percent=0 --tolerate-errors --concurrency=%d "+
					"--min-block-bytes=%d --max-block-bytes=%d --duration=%s {pgurl%s}",
				concurrency, blockSize, blockSize, duration.String(), c.CRDBNodes()))
		}, task.Name("load-generator"))

		m.Go(func(ctx context.Context, l *logger.Logger) error {
			t.Status("starting checks for range sizes")
			const sizeLimit = 3 * (1 << 29) // 3*512 MB (512 mb is default size)

			db := c.Conn(ctx, l, 1)
			defer db.Close()

			var size = float64(0)
			for tBegin := timeutil.Now(); timeutil.Since(tBegin) <= duration; {
				if err := db.QueryRow(
					`select max(bytes_per_replica->'PMax') from crdb_internal.kv_store_status;`,
				).Scan(&size); err != nil {
					return err
				}
				if size > sizeLimit {
					return errors.Errorf("range size %s exceeded %s", humanizeutil.IBytes(int64(size)),
						humanizeutil.IBytes(int64(sizeLimit)))
				}

				t.Status(fmt.Sprintf("max range size %s", humanizeutil.IBytes(int64(size))))

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
				}
			}

			return nil
		}, task.Name("range-size-checks"))
		m.Wait()
	}

	minutes := 10 * time.Minute
	numNodes := 4
	concurrency := 128

	r.Add(registry.TestSpec{
		Name:  fmt.Sprintf("hotspotsplits/nodes=%d", numNodes),
		Owner: registry.OwnerKV,
		// Test OOMs below this version because of scans over the large rows.
		// No problem in 20.1 thanks to:
		// https://github.com/cockroachdb/cockroach/pull/45323.
		Cluster:          r.MakeClusterSpec(numNodes, spec.WorkloadNode()),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		// This test may timeout waiting for replica divergence post-test
		// validation due to high write volume, see #141007.
		SkipPostValidations: registry.PostValidationReplicaDivergence,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.IsLocal() {
				concurrency = 32
				fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
			}
			runHotSpot(ctx, t, c, minutes, concurrency)
		},
	})
}
