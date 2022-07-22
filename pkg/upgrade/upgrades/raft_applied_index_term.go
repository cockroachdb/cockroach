// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// defaultPageSize controls how many ranges are paged in by default when
// iterating through all ranges in a cluster during any given upgrade. We
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

// persistWatermarkBatchInterval specifies how often to persist the progress
// watermark (in batches). 5 batches means we'll checkpoint every 1000 ranges.
const persistWatermarkBatchInterval = 5

func raftAppliedIndexTermMigration(
	ctx context.Context, cv clusterversion.ClusterVersion, deps upgrade.SystemDeps, job *jobs.Job,
) error {
	// Fetch the migration's watermark (latest migrated range's end key), in case
	// we're resuming a previous migration.
	var resumeWatermark roachpb.RKey
	if progress, ok := job.Progress().Details.(*jobspb.Progress_Migration); ok {
		if len(progress.Migration.Watermark) > 0 {
			resumeWatermark = append(resumeWatermark, progress.Migration.Watermark...)
			log.Infof(ctx, "loaded migration watermark %s, resuming", resumeWatermark)
		}
	}

	retryOpts := retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		MaxRetries:     5,
	}

	var batchIdx, numMigratedRanges int
	init := func() { batchIdx, numMigratedRanges = 1, 0 }
	if err := deps.Cluster.IterateRangeDescriptors(ctx, defaultPageSize, init, func(descriptors ...roachpb.RangeDescriptor) error {
		var progress jobspb.MigrationProgress
		for _, desc := range descriptors {
			// NB: This is a bit of a wart. We want to reach the first range,
			// but we can't address the (local) StartKey. However, keys.LocalMax
			// is on r1, so we'll just use that instead to target r1.
			start, end := desc.StartKey, desc.EndKey
			if bytes.Compare(desc.StartKey, keys.LocalMax) < 0 {
				start, _ = keys.Addr(keys.LocalMax)
			}

			// Skip any ranges below the resume watermark.
			if bytes.Compare(end, resumeWatermark) <= 0 {
				continue
			}

			// Migrate the range, with a few retries.
			if err := retryOpts.Do(ctx, func(ctx context.Context) error {
				err := deps.DB.Migrate(ctx, start, end, cv.Version)
				if err != nil {
					log.Errorf(ctx, "range %d migration failed, retrying: %s", desc.RangeID, err)
				}
				return err
			}); err != nil {
				return err
			}

			progress.Watermark = end
		}

		// Persist the migration's progress.
		if batchIdx%persistWatermarkBatchInterval == 0 && len(progress.Watermark) > 0 {
			if err := job.SetProgress(ctx, nil, progress); err != nil {
				return errors.Wrap(err, "failed to record migration progress")
			}
		}

		// TODO(irfansharif): Instead of logging this to the debug log, we
		// should insert these into a `system.upgrades` table for external
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
	// upgrades, this ensures that the applied state is flushed to disk.
	req := &serverpb.SyncAllEnginesRequest{}
	op := "flush-stores"
	return deps.Cluster.ForEveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
		_, err := client.SyncAllEngines(ctx, req)
		return err
	})
}

func postRaftAppliedIndexTermMigration(
	ctx context.Context, cv clusterversion.ClusterVersion, deps upgrade.SystemDeps, _ *jobs.Job,
) error {
	// TODO(sumeer): this is copied from postTruncatedStateMigration. In
	// comparison, postSeparatedIntentsMigration iterated over ranges and issues
	// a noop below-raft upgrade. I am not clear on why there is a difference.
	// Get this clarified.

	// Purge all replicas that haven't been migrated to use the unreplicated
	// truncated state and the range applied state.
	truncStateVersion := clusterversion.ByKey(clusterversion.AddRaftAppliedIndexTermMigration)
	req := &serverpb.PurgeOutdatedReplicasRequest{Version: &truncStateVersion}
	op := fmt.Sprintf("purge-outdated-replicas=%s", req.Version)
	return deps.Cluster.ForEveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
		_, err := client.PurgeOutdatedReplicas(ctx, req)
		return err
	})
}
