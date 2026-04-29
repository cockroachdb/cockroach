// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// checkpointInterval is how often the checkpoint loop wakes to
// snapshot TickManager state and persist it. Long enough to keep
// txn churn against system.job_progress / system.job_info minimal;
// short enough that operators see the log's resolved time move
// within a tick or two of when it actually advances and that
// resume after a crash loses at most one interval of work.
const checkpointInterval = 30 * time.Second

// runCheckpointer periodically snapshots TickManager state and
// hands it to the Persister. It returns when ctx is cancelled (job
// pause / cancel / fail / completion).
//
// Failures during a single checkpoint are logged and skipped
// rather than fatal: persistence is best-effort progress
// reporting, not a correctness gate. The next interval retries
// from the then-current snapshot.
func runCheckpointer(ctx context.Context, persister Persister, manager *TickManager) error {
	ticker := time.NewTicker(checkpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := checkpointOnce(ctx, persister, manager); err != nil {
				log.Dev.Warningf(ctx, "revlogjob: checkpoint persist failed: %v", err)
			}
		}
	}
}

// checkpointOnce snapshots and persists once. Extracted for tests.
func checkpointOnce(ctx context.Context, persister Persister, manager *TickManager) error {
	state, err := manager.Snapshot()
	if err != nil {
		return err
	}
	return persister.Store(ctx, state)
}
