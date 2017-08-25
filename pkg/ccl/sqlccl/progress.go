// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sqlccl

import (
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// For both backups and restores, we compute progress as the number of completed
// export or import requests, respectively, divided by the total number of
// requests. To avoid hammering the system.jobs table, when a response comes
// back, we issue a progress update only if a) it's been a duration of
// progressTimeThreshold since the last update, or b) the difference between the
// last logged fractionCompleted and the current fractionCompleted is more than
// progressFractionThreshold.
const (
	progressTimeThreshold     = time.Second
	progressFractionThreshold = 0.05
)

type jobProgressLogger struct {
	// These fields must be externally initialized.
	job           *jobs.Job
	startFraction float32
	totalChunks   int
	progressedFn  jobs.ProgressedFn

	// The remaining fields are for internal use only.
	mu struct {
		syncutil.Mutex
		completedChunks      int
		lastReportedAt       time.Time
		lastReportedFraction float32
	}
}

func (jpl *jobProgressLogger) chunkFinished(ctx context.Context) error {
	jpl.mu.Lock()
	jpl.mu.completedChunks++
	fraction := float32(jpl.mu.completedChunks) / float32(jpl.totalChunks)
	fraction = fraction*(1-jpl.startFraction) + jpl.startFraction
	shouldLogProgress := fraction-jpl.mu.lastReportedFraction > progressFractionThreshold ||
		jpl.mu.lastReportedAt.Add(progressTimeThreshold).Before(timeutil.Now())
	if shouldLogProgress {
		jpl.mu.lastReportedAt = timeutil.Now()
		jpl.mu.lastReportedFraction = fraction
	}
	jpl.mu.Unlock()

	if !shouldLogProgress {
		return nil
	}

	// NB: This timeout is very important. If a progress update takes longer than
	// progressTimeThreshold, another goroutine might come along and attempt to
	// update the progress. Without a timeout, our update will contend with the
	// new goroutine's update, which slows down both progress updates, leading to
	// a self-reinforcing cycle where dozens of goroutines contend to update the
	// progress of this job. A 15-node cluster with 60 such contending goroutines
	// observed a progress update that took several minutes, for example. This
	// contention inadvertently applies backpressure on the backup/restore
	// coordinator and brings the job to a near halt.
	//
	// TODO(benesch): see if there's a cleaner way to handle this by refactoring
	// the way backup and restore manage their progress-logging goroutines.
	ctx, cancel := context.WithTimeout(ctx, progressTimeThreshold)
	defer cancel()
	err := jpl.job.Progressed(ctx, fraction, jpl.progressedFn)
	// The context deadline error might come from a remote node, in which case
	// err != context.DeadlineExceeded, so we check the error string instead.
	if err != nil && err.Error() == context.DeadlineExceeded.Error() {
		log.Warningf(ctx, "job %d: context deadline exceeded while updating progress", *jpl.job.ID())
		return nil
	}
	return err
}
