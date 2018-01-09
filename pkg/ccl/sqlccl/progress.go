// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
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
	progressedFn  func(context.Context, jobs.Details)

	// The remaining fields are for internal use only.
	completedChunks      int
	lastReportedAt       time.Time
	lastReportedFraction float32
}

// chunkFinished marks one chunk of the job as completed. If either the time or
// fraction threshold has been reached, the progress update will be persisted to
// system.jobs.
//
// NB: chunkFinished is not threadsafe. A previous implementation that was
// threadsafe occasionally led to massive contention. One 2TB restore on a 15
// node cluster, for example, had 60 goroutines attempting to update the
// progress at once, causing massive contention on the row in system.jobs. This
// inadvertently applied backpressure on the restore's import requests and
// slowed the job to a crawl. If multiple threads need to update progress, use a
// channel and a dedicated goroutine that calls loop.
func (jpl *jobProgressLogger) chunkFinished(ctx context.Context) error {
	jpl.completedChunks++
	fraction := float32(jpl.completedChunks) / float32(jpl.totalChunks)
	fraction = fraction*(1-jpl.startFraction) + jpl.startFraction
	shouldLogProgress := fraction-jpl.lastReportedFraction > progressFractionThreshold ||
		jpl.lastReportedAt.Add(progressTimeThreshold).Before(timeutil.Now())
	if !shouldLogProgress {
		return nil
	}
	jpl.lastReportedAt = timeutil.Now()
	jpl.lastReportedFraction = fraction
	return jpl.job.Progressed(ctx, func(ctx context.Context, details jobs.Details) float32 {
		if jpl.progressedFn != nil {
			jpl.progressedFn(ctx, details)
		}
		return fraction
	})
}

// loop calls chunkFinished for every message received over chunkCh. It exits
// when chunkCh is closed, when totalChunks messages have been received, or when
// the context is canceled.
func (jpl *jobProgressLogger) loop(ctx context.Context, chunkCh <-chan struct{}) error {
	for {
		select {
		case _, ok := <-chunkCh:
			if !ok {
				return nil
			}
			if err := jpl.chunkFinished(ctx); err != nil {
				return err
			}
			if jpl.completedChunks == jpl.totalChunks {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
