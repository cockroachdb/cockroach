// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfiller

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// jobSubdirectoryCleaner defines the interface for cleaning up SST subdirectories.
// It is satisfied by *bulkutil.BulkJobCleaner.
type jobSubdirectoryCleaner interface {
	CleanupJobSubdirectory(
		ctx context.Context, jobID jobspb.JobID, storagePrefixes []string, subdirectory string,
	) error
}

// phaseTransition represents a detected phase change in the distributed merge
// pipeline after checkpoint persistence.
type phaseTransition struct {
	TableID            descpb.ID
	OldPhase           int32
	NewPhase           int32
	SSTStoragePrefixes []string
}

// phaseTransitionCleaner orchestrates SST cleanup for phase transitions.
type phaseTransitionCleaner struct {
	jobID   jobspb.JobID
	cleaner jobSubdirectoryCleaner
}

// cleanupTransition performs SST cleanup for a single phase transition.
func (c *phaseTransitionCleaner) cleanupTransition(
	ctx context.Context, transition phaseTransition,
) error {
	log.Dev.Infof(ctx, "triggering SST cleanup for job %d, table %d after phase %d→%d transition",
		c.jobID, transition.TableID, transition.OldPhase, transition.NewPhase)

	// Determine which subdirectory to clean up based on completed phase:
	// - newPhase=1: cleanup map/ (map phase output, input to iteration 1)
	// - newPhase=2: cleanup merge/iter-1/ (iteration 1 output, input to iteration 2)
	subdirectory := bulkutil.NewDistMergePaths(c.jobID).InputSubdir(int(transition.NewPhase))

	if len(transition.SSTStoragePrefixes) == 0 {
		log.Dev.Infof(ctx, "no storage prefixes found, skipping cleanup")
		return nil
	}

	if err := c.cleaner.CleanupJobSubdirectory(ctx, c.jobID, transition.SSTStoragePrefixes, subdirectory); err != nil {
		return errors.Wrapf(err, "cleaning up subdirectory %s", subdirectory)
	}

	log.Dev.Infof(ctx, "successfully cleaned up SSTs in %s", subdirectory)
	return nil
}
