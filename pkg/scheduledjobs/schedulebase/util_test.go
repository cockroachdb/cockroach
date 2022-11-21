// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schedulebase

import (
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestComputeScheduleRecurrence(t *testing.T) {
	testCases := []struct {
		name     string
		cronExpr string
		expected *ScheduleRecurrence
	}{
		{
			name:     "Every hour",
			cronExpr: "0 * * * *",
			expected: &ScheduleRecurrence{"0 * * * *", time.Hour},
		},
		{
			name:     "Daily function",
			cronExpr: "@daily",
			expected: &ScheduleRecurrence{"@daily", time.Hour * 24},
		},
		{
			name:     "5th minute every 2 hours",
			cronExpr: "5 */2 * * *",
			expected: &ScheduleRecurrence{"5 */2 * * *", time.Hour * 2},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			recurrence, err := ComputeScheduleRecurrence(timeutil.Now(), &test.cronExpr)
			require.NoError(t, err)
			require.Equal(t, test.expected, recurrence)
		})
	}
}

func TestParseErrorBehaviour(t *testing.T) {
	testCases := []struct {
		input    string
		expected jobspb.ScheduleDetails_ErrorHandlingBehavior
	}{
		{
			input:    "retry",
			expected: jobspb.ScheduleDetails_RETRY_SOON,
		},
		{
			input:    "reschedule",
			expected: jobspb.ScheduleDetails_RETRY_SCHED,
		},
		{
			input:    "pause",
			expected: jobspb.ScheduleDetails_PAUSE_SCHED,
		},
	}

	t.Run("string value to parsed object", func(t *testing.T) {
		for _, test := range testCases {
			t.Run(test.input, func(t *testing.T) {
				scheduleDetails := &jobspb.ScheduleDetails{}
				err := ParseOnError(test.input, scheduleDetails)
				require.NoError(t, err)
				require.Equal(t, test.expected, scheduleDetails.OnError)
			})
		}
	})

	t.Run("object to string value", func(t *testing.T) {
		for _, test := range testCases {
			t.Run(test.input, func(t *testing.T) {
				errorOption, err := ParseOnErrorOption(test.expected)
				require.NoError(t, err)
				require.Equal(t, test.input, strings.ToLower(errorOption))
			})
		}
	})
}

func TestParseWaitBehavior(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		input    string
		expected jobspb.ScheduleDetails_WaitBehavior
	}{
		{
			input:    "start",
			expected: jobspb.ScheduleDetails_NO_WAIT,
		},
		{
			input:    "skip",
			expected: jobspb.ScheduleDetails_SKIP,
		},
		{
			input:    "wait",
			expected: jobspb.ScheduleDetails_WAIT,
		},
	}

	t.Run("string value to parsed object", func(t *testing.T) {
		for _, test := range testCases {
			t.Run(test.input, func(t *testing.T) {
				scheduleDetails := &jobspb.ScheduleDetails{}
				err := ParseWaitBehavior(test.input, scheduleDetails)
				require.NoError(t, err)
				require.Equal(t, test.expected, scheduleDetails.Wait)
			})
		}
	})

	t.Run("object to string value", func(t *testing.T) {
		for _, test := range testCases {
			t.Run(test.input, func(t *testing.T) {
				waitOption, err := ParseOnPreviousRunningOption(test.expected)
				require.NoError(t, err)
				require.Equal(t, test.input, strings.ToLower(waitOption))
			})
		}
	})

}

// CheckScheduleAlreadyExists is tested in scheduled_changefeed_test.go
