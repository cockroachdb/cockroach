// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/stretchr/testify/assert"
)

func TestJobsStructuredLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	jobSpy := logtestutils.NewStructuredLogSpy(
		t,
		[]logpb.Channel{logpb.Channel_OPS},
		[]string{"status_change"},
		func(entry logpb.Entry) (eventpb.StatusChange, error) {
			var structuredPayload eventpb.StatusChange
			err := json.Unmarshal([]byte(entry.Message[entry.StructuredStart:entry.StructuredEnd]), &structuredPayload)
			if err != nil {
				return structuredPayload, err
			}
			return structuredPayload, nil
		},
	)

	cleanup := log.InterceptWith(context.Background(), jobSpy)
	defer cleanup()

	rts := registryTestSuite{}
	defer rts.setUp(t)()
	defer rts.tearDown()

	j, err := jobs.TestingCreateAndStartJob(rts.ctx, rts.registry, rts.idb(), rts.mockJob)
	if err != nil {
		t.Fatal(err)
	}
	rts.job = j

	rts.mu.Lock()
	rts.mu.e.ResumeStart = true
	rts.mu.Unlock()
	rts.resumeCheckCh <- struct{}{}
	assertJobStateChangeEvent(t, jobSpy, &rts, jobs.StatusRunning, jobs.StatusRunning)

	rts.resumeCh <- nil
	rts.mu.Lock()
	rts.mu.e.ResumeExit++
	rts.mu.e.Success = true
	rts.mu.Unlock()
	assertJobStateChangeEvent(t, jobSpy, &rts, jobs.StatusSucceeded, jobs.StatusRunning)
}

func assertJobStateChangeEvent(
	t *testing.T,
	stmtSpy *logtestutils.StructuredLogSpy[eventpb.StatusChange],
	rts *registryTestSuite,
	expectedNewStatus jobs.Status,
	expectedPrevStatus jobs.Status,
) {
	rts.check(t, expectedNewStatus)
	l := stmtSpy.GetUnreadLogs(logpb.Channel_OPS)
	containsStateChange := containsStateChangeLog(l, rts.job, expectedNewStatus, expectedPrevStatus)
	assert.True(t, containsStateChange)
}

func containsStateChangeLog(
	jobEventsLogs []eventpb.StatusChange,
	job *jobs.StartableJob,
	expectedNewStatus jobs.Status,
	expectedPrevStatus jobs.Status,
) bool {
	for _, jobEventsLog := range jobEventsLogs {
		if jobEventsLog.JobID == int64(job.ID()) &&
			jobEventsLog.PreviousStatus == string(expectedPrevStatus) &&
			jobEventsLog.NewStatus == string(expectedNewStatus) {
			return true
		}
	}
	return false
}
