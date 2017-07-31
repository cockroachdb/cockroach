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

	if shouldLogProgress {
		return jpl.job.Progressed(ctx, fraction, jpl.progressedFn)
	}
	return nil
}
