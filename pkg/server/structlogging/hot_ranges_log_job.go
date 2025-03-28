// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package structlogging

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func (s *hotRangesLoggingScheduler) Resume(ctx context.Context, execCtxI interface{}) error {
	// This job is a forever running background job, and it is always safe to
	// terminate the SQL pod whenever the job is running, so mark it as idle.
	s.job.MarkIdle(true)

	return s.start(ctx, s.stopper)
}

func (s *hotRangesLoggingScheduler) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	if jobs.HasErrJobCanceled(jobErr) {
		err := errors.NewAssertionErrorWithWrappedErrf(
			jobErr, "hot range logging job is not cancelable",
		)
		log.Errorf(ctx, "%v", err)
	}
	return nil
}

func (s *hotRangesLoggingScheduler) CollectProfile(ctx context.Context, execCtx interface{}) error {
	return nil
}
