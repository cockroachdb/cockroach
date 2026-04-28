// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// progressCheckpointInterval is how often the progress-updater
// goroutine wakes to publish the most-recently-closed tick's end
// time to the job's HighWater. Long enough to keep system.jobs
// write traffic minimal, short enough that operators see the log's
// position move within a tick or two of when it actually advances.
const progressCheckpointInterval = 30 * time.Second

// runProgressUpdater periodically copies the manager's lastClosed
// value into the job's Progress.HighWater field. It returns when
// ctx is cancelled (job pause / cancel / fail / completion).
func runProgressUpdater(ctx context.Context, job *jobs.Job, manager *TickManager) error {
	ticker := time.NewTicker(progressCheckpointInterval)
	defer ticker.Stop()

	// lastWritten remembers what we most recently persisted so we can
	// skip the system.jobs write when nothing has advanced.
	var lastWritten hlc.Timestamp
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// TODO(dt): also persist the in-flight aggregate frontier and
			// the per-open-tick file lists so resume doesn't have to
			// re-derive and re-flush events between the HighWater and
			// the in-flight frontier into new files.
			closed := manager.LastClosed()
			if !lastWritten.Less(closed) {
				continue
			}
			if err := writeHighWater(ctx, job, closed); err != nil {
				// Don't fail the job on a transient progress write
				// failure; log and try again on the next tick. The
				// HighWater is informational — losing one update
				// just means operators see a slightly stale value
				// until the next successful write.
				log.Dev.Warningf(ctx, "revlogjob: persisting progress high-water %s: %v", closed, err)
				continue
			}
			lastWritten = closed
		}
	}
}

// writeHighWater persists hw into the job's Progress.HighWater
// field via the standard Update path.
func writeHighWater(ctx context.Context, job *jobs.Job, hw hlc.Timestamp) error {
	return job.NoTxn().Update(ctx, func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}
		md.Progress.Progress = &jobspb.Progress_HighWater{HighWater: &hw}
		ju.UpdateProgress(md.Progress)
		return nil
	})
}
