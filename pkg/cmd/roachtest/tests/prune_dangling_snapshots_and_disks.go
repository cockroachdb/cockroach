// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// This test exists only to prune expired snapshots. Not all cloud providers
// (GCE) let you store volume snapshots in buckets with a pre-configured TTL. So
// we use this nightly roachtest as a poor man's cron job.
func registerPruneDanglingSnapshotsAndDisks(r registry.Registry) {
	clusterSpec := r.MakeClusterSpec(
		1, /* nodeCount */
		spec.Cloud(spec.GCE),
	)

	r.Add(registry.TestSpec{
		Name:            "prune-dangling",
		Owner:           registry.OwnerTestEng,
		Cluster:         clusterSpec,
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			snapshots, err := c.ListSnapshots(ctx, vm.VolumeSnapshotListOpts{
				CreatedBefore: timeutil.Now().Add(-1 * roachprod.SnapshotTTL),
				Labels: map[string]string{
					vm.TagUsage: "roachtest", // only prune out snapshots created in tests
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			for _, snapshot := range snapshots {
				if err := c.DeleteSnapshots(ctx, snapshot); err != nil {
					t.Fatal(err)
				}
				t.L().Printf("pruned old snapshot %s (id=%s)", snapshot.Name, snapshot.ID)
			}

			// TODO(irfansharif): Also prune out unattached disks. Use something
			// like:
			//
			//	gcloud compute --project $project disks list --filter="-users:*"
		},
	})
}
