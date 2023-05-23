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
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

func registerIndexBackfill(r registry.Registry) {
	clusterSpec := r.MakeClusterSpec(
		1, /* nodeCount */
		spec.CPU(8),
		spec.Zones("us-east1-b"),
		spec.VolumeSize(500),
		spec.Cloud(spec.GCE),
	)
	clusterSpec.InstanceType = "n2-standard-8"
	clusterSpec.GCEMinCPUPlatform = "Intel Ice Lake"
	clusterSpec.GCEVolumeType = "pd-ssd"

	r.Add(registry.TestSpec{
		Name:  "admission-control/index-backfill",
		Owner: registry.OwnerAdmissionControl,
		// TODO(irfansharif): Reduce to weekly cadence once stabilized.
		// Tags:            registry.Tags(`weekly`),
		Cluster:         clusterSpec,
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// TODO(irfansharif): Make a registry of these prefix strings. It's
			// important no registered name is a prefix of another.
			const snapshotPrefix = "ac-index-backfill"

			var snapshots []vm.VolumeSnapshot
			snapshots, err := c.ListSnapshots(ctx, vm.VolumeSnapshotListOpts{
				// TODO(irfansharif): Search by taking in the other parts of the
				// snapshot fingerprint, i.e. the node count, the version, etc.
				Name: snapshotPrefix,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshots) == 0 {
				t.L().Printf("no existing snapshots found for %s (%s), doing pre-work", t.Name(), snapshotPrefix)
				// TODO(irfansharif): Add validation that we're some released
				// version, probably the predecessor one. Also ensure that any
				// running CRDB processes have been stopped since we're taking
				// raw disk snapshots. Also later we'll be unmounting/mounting
				// attached volumes.
				if err := c.CreateSnapshot(ctx, snapshotPrefix); err != nil {
					t.Fatal(err)
				}
				snapshots, err = c.ListSnapshots(ctx, vm.VolumeSnapshotListOpts{Name: snapshotPrefix})
				if err != nil {
					t.Fatal(err)
				}
				t.L().Printf("using %d newly created snapshot(s) with prefix %q", len(snapshots), snapshotPrefix)
			} else {
				t.L().Printf("using %d pre-existing snapshot(s) with prefix %q", len(snapshots), snapshotPrefix)
			}

			if err := c.ApplySnapshots(ctx, snapshots); err != nil {
				t.Fatal(err)
			}

			// TODO(irfansharif): Actually do something using TPC-E, index
			// backfills and replication admission control.
		},
	})
}
