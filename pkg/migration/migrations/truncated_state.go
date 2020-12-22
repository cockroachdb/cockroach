// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// defaultPageSize controls how many ranges are paged in by default when
// iterating through all ranges in a cluster during any given migration. We
// pulled this number out of thin air(-ish). Let's consider a cluster with 50k
// ranges, with each range taking ~200ms. We're being somewhat conservative with
// the duration, but in a wide-area cluster with large hops between the manager
// and the replicas, it could be true. Here's how long it'll take for various
// block sizes:
//
//   page size of 1   ~ 2h 46m
//   page size of 50  ~ 3m 20s
//   page size of 200 ~ 50s
const defaultPageSize = 200

func truncatedStateMigration(
	ctx context.Context, cv clusterversion.ClusterVersion, h migration.Cluster,
) error {
	var batchIdx, numMigratedRanges int
	init := func() { batchIdx, numMigratedRanges = 1, 0 }
	if err := h.IterateRangeDescriptors(ctx, defaultPageSize, init, func(descriptors ...roachpb.RangeDescriptor) error {
		for _, desc := range descriptors {
			// NB: This is a bit of a wart. We want to reach the first range,
			// but we can't address the (local) StartKey. However, keys.LocalMax
			// is on r1, so we'll just use that instead to target r1.
			start, end := desc.StartKey, desc.EndKey
			if bytes.Compare(desc.StartKey, keys.LocalMax) < 0 {
				start, _ = keys.Addr(keys.LocalMax)
			}
			if err := h.DB().Migrate(ctx, start, end, cv.Version); err != nil {
				return err
			}
		}

		// TODO(irfansharif): Instead of logging this to the debug log, we
		// should insert these into a `system.migrations` table for external
		// observability.
		numMigratedRanges += len(descriptors)
		log.Infof(ctx, "[batch %d/??] migrated %d ranges", batchIdx, numMigratedRanges)
		batchIdx++

		return nil
	}); err != nil {
		return err
	}

	log.Infof(ctx, "[batch %d/%d] migrated %d ranges", batchIdx, batchIdx, numMigratedRanges)

	// Make sure that all stores have synced. Given we're a below-raft
	// migrations, this ensures that the applied state is flushed to disk.
	req := &serverpb.SyncAllEnginesRequest{}
	op := "flush-stores"
	return h.ForEveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
		_, err := client.SyncAllEngines(ctx, req)
		return err
	})
}

func postTruncatedStateMigration(
	ctx context.Context, cv clusterversion.ClusterVersion, h migration.Cluster,
) error {
	// Purge all replicas that haven't been migrated to use the unreplicated
	// truncated state and the range applied state.
	truncStateVersion := clusterversion.ByKey(clusterversion.TruncatedAndRangeAppliedStateMigration)
	req := &serverpb.PurgeOutdatedReplicasRequest{Version: &truncStateVersion}
	op := fmt.Sprintf("purge-outdated-replicas=%s", req.Version)
	return h.ForEveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
		_, err := client.PurgeOutdatedReplicas(ctx, req)
		return err
	})
}
