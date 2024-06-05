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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/stretchr/testify/assert"
)

// log.StructuredPayload has `any` as the payload type. We are defining this new struct to marshal the payload
// directly into a `jobs.JobEvent[jobs.JobStateChangeDetails]` struct
type JobStateChangeStructuredPayload struct {
	Metadata log.StructuredLogMeta `json:"metadata"`
	Payload  jobs.JobEvent         `json:"payload"`
}

func TestJobsStructuredLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	jobSpy := logtestutils.NewStructuredLogSpy(
		t,
		[]logpb.Channel{logpb.Channel_STRUCTURED_EVENTS},
		[]string{"job_event"},
		func(entry logpb.Entry) (jobs.JobEvent, error) {
			var structuredPayload JobStateChangeStructuredPayload
			err := json.Unmarshal([]byte(entry.Message[entry.StructuredStart:entry.StructuredEnd]), &structuredPayload)
			if err != nil {
				return structuredPayload.Payload, err
			}
			return structuredPayload.Payload, nil
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
	stmtSpy *logtestutils.StructuredLogSpy[jobs.JobEvent],
	rts *registryTestSuite,
	expectedNewStatus jobs.Status,
	expectedPrevStatus jobs.Status,
) {
	rts.check(t, expectedNewStatus)
	l := stmtSpy.GetUnreadLogs(logpb.Channel_STRUCTURED_EVENTS)
	containsStateChange, err := containsStateChangeLog(l, rts.job, expectedNewStatus, expectedPrevStatus)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, containsStateChange)
}

func containsStateChangeLog(
	jobEventsLogs []jobs.JobEvent,
	job *jobs.StartableJob,
	expectedNewStatus jobs.Status,
	expectedPrevStatus jobs.Status,
) (bool, error) {
	for _, jobEventsLog := range jobEventsLogs {
		if jobEventsLog.JobID == job.ID() && jobEventsLog.EventType == jobs.StateChange {
			jsonBytes, err := json.Marshal(jobEventsLog.EventDetails)
			if err != nil {
				return false, err
			}
			var stateChangeDetails jobs.JobStateChangeDetails
			if err = json.Unmarshal(jsonBytes, &stateChangeDetails); err != nil {
				return false, err
			}

			fmt.Printf("stateChangeDetails: %+v\n", stateChangeDetails)
			if stateChangeDetails.NewStatus == expectedNewStatus &&
				stateChangeDetails.PreviousStatus == expectedPrevStatus {
				return true, nil
			}
		}
	}
	return false, nil
}
