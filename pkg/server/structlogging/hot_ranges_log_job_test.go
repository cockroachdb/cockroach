// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package structlogging_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// This test verifies that the hot ranges logging job starts for both
// the system and app layers, and exits quickly for the system layer.
func TestHotRangesLoggingJobExitProcedure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStress(t)
	skip.UnderRace(t)

	ctx := context.Background()
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer ts.Stopper().Stop(ctx)

	syslayer := ts.SystemLayer().SQLConn(t)
	applayer := ts.ApplicationLayer().SQLConn(t)

	testutils.SucceedsSoon(t, func() error {
		row := syslayer.QueryRow("SELECT status FROM system.public.jobs WHERE id = $1", jobs.HotRangesLoggerJobID)
		var status string
		require.NoError(t, row.Scan(&status))
		if status != "succeeded" {
			return errors.Newf("system job status is %s, not succeeded", status)
		}

		row = applayer.QueryRow("SELECT status FROM system.public.jobs WHERE id = $1", jobs.HotRangesLoggerJobID)
		require.NoError(t, row.Scan(&status))
		if status != "running" {
			return errors.Newf("app job status is %s, not running", status)
		}
		return nil
	})
}
