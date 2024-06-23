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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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

	stmtSpy := logtestutils.NewStructuredLogSpy(
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

	cleanup := log.InterceptWith(context.Background(), stmtSpy)
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
	assertJobStateChangeEvent(t, stmtSpy, &rts, jobs.StatusRunning)

	rts.resumeCh <- nil
	rts.mu.Lock()
	rts.mu.e.ResumeExit++
	rts.mu.e.Success = true
	rts.mu.Unlock()
	assertJobStateChangeEvent(t, stmtSpy, &rts, jobs.StatusSucceeded)
}

func assertJobStateChangeEvent(
	t *testing.T,
	stmtSpy *logtestutils.StructuredLogSpy[jobs.JobEvent],
	rts *registryTestSuite,
	expectedStatus jobs.Status,
) {
	rts.check(t, expectedStatus)
	l := stmtSpy.GetUnreadLogs(logpb.Channel_STRUCTURED_EVENTS)
	assert.True(t, containsStateChangeLog(l, rts.job.ID(), expectedStatus))
}

func containsStateChangeLog(
	jobEventsLogs []jobs.JobEvent, id jobspb.JobID, expectedStatus jobs.Status,
) bool {
	for _, jobEventsLog := range jobEventsLogs {
		if jobEventsLog.JobID == id && jobEventsLog.EventType == jobs.StateChange {
			b, _ := json.Marshal(jobEventsLog.EventDetails)
			var stateChangeDetails jobs.JobStateChangeDetails
			_ = json.Unmarshal(b, &stateChangeDetails)
			if stateChangeDetails.NewStatus == expectedStatus {
				return true
			}
		}
	}
	return false
}
