// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
)

func registerDisaggRebalance(r registry.Registry) {
	disaggRebalanceSpec := r.MakeClusterSpec(4)
	r.Add(registry.TestSpec{
		Name:              fmt.Sprintf("disagg-rebalance/aws/%s", disaggRebalanceSpec),
		CompatibleClouds:  registry.OnlyAWS,
		Suites:            registry.Suites(registry.Nightly),
		Owner:             registry.OwnerStorage,
		Cluster:           disaggRebalanceSpec,
		RequiresLicense:   true,
		EncryptionSupport: registry.EncryptionAlwaysDisabled,
		Timeout:           1 * time.Hour,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			s3dir := fmt.Sprintf("s3://%s/disagg-rebalance/%s?AUTH=implicit", testutils.BackupTestingBucketLongTTL(), c.Name())
			startOpts := option.NewStartOpts(option.NoBackupSchedule)
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--experimental-shared-storage=%s", s3dir))
			c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Range(1, 3))

			initialWaitDuration := 2 * time.Minute
			warehouses := 20

			t.Status("workload initialization")
			cmd := fmt.Sprintf(
				"./cockroach workload fixtures import tpcc --warehouses=%d {pgurl:1}",
				warehouses,
			)
			m := c.NewMonitor(ctx, c.Range(1, 3))
			m.Go(func(ctx context.Context) error {
				return c.RunE(ctx, c.Node(1), cmd)
			})
			m.Wait()

			m2 := c.NewMonitor(ctx, c.Range(1, 3))

			m2.Go(func(ctx context.Context) error {
				t.Status("run tpcc")

				cmd := fmt.Sprintf(
					"./cockroach workload run tpcc --warehouses=%d --duration=10m {pgurl:1-3}",
					warehouses,
				)

				return c.RunE(ctx, c.Node(1), cmd)
			})

			select {
			case <-time.After(initialWaitDuration):
			case <-ctx.Done():
				return
			}

			t.Status("starting fourth node")
			// Start the fourth node.
			c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(4))

			t.Status("verify rebalance")

			db := c.Conn(ctx, t.L(), 4)

			// Wait for the new node to get at least one replica before calling
			// waitForRebalance.
			testutils.SucceedsSoon(t, func() error {
				var count int
				if err := db.QueryRow(
					"SELECT count(*) FROM crdb_internal.ranges WHERE array_position(replicas, $1) IS NOT NULL",
					4,
				).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count <= 0 {
					return errors.New("newly added node n4 has zero replicas")
				}
				return nil
			})
			defer func() {
				_ = db.Close()
			}()

			if err := waitForRebalance(ctx, t.L(), db, 10 /* maxStdDev */, 20 /* stableSeconds */); err != nil {
				t.Fatal(err)
			}

			var count int
			if err := db.QueryRow(
				// Check if the new node has any replicas.
				"SELECT count(*) FROM crdb_internal.ranges WHERE array_position(replicas, $1) IS NOT NULL",
				4,
			).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count < 10 {
				t.Fatalf("did not replicate to n4 quickly enough, only found %d replicas", count)
			}

			var bytesInRanges int64
			if err := db.QueryRow(
				"SELECT sum(used) "+
					"FROM crdb_internal.kv_store_status WHERE node_id = $1 GROUP BY node_id LIMIT 1",
				4,
			).Scan(&bytesInRanges); err != nil {
				t.Fatal(err)
			}
			var bytesSnapshotted int64
			if err := db.QueryRow(
				"SELECT metrics['range.snapshots.rcvd-bytes']::INT FROM crdb_internal.kv_store_status WHERE node_id = $1 LIMIT 1",
				4,
			).Scan(&bytesSnapshotted); err != nil {
				t.Fatal(err)
			}

			t.L().PrintfCtx(ctx, "got snapshot received bytes = %s, logical bytes in ranges = %s", humanize.IBytes(uint64(bytesSnapshotted)), humanize.IBytes(uint64(bytesInRanges)))
			if bytesSnapshotted > bytesInRanges {
				t.Fatalf("unexpected snapshot received bytes %d > bytes in all replicas on n4 %d, did not do a disaggregated rebalance?", bytesSnapshotted, bytesInRanges)
			}

			t.Status("continue tpcc")

			if err := m2.WaitE(); err != nil {
				t.Fatal(err)
			}
		},
	})
}
