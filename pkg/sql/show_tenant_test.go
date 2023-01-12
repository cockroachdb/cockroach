// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestGetTenantStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name            string
		jobStatus       jobs.Status
		replicationInfo *streampb.StreamIngestionStats
		expectedStatus  tenantStatus
	}{
		{
			name:            "paused",
			jobStatus:       jobs.StatusPaused,
			replicationInfo: nil,
			expectedStatus:  replicationPaused,
		},
		{
			name:            "pending",
			jobStatus:       jobs.StatusPending,
			replicationInfo: nil,
			expectedStatus:  initReplication,
		},
		{
			name:            "running no info",
			jobStatus:       jobs.StatusRunning,
			replicationInfo: nil,
			expectedStatus:  initReplication,
		},
		{
			name:            "pause requested no info",
			jobStatus:       jobs.StatusPauseRequested,
			replicationInfo: nil,
			expectedStatus:  initReplication,
		},
		{
			name:      "running no lag",
			jobStatus: jobs.StatusRunning,
			replicationInfo: &streampb.StreamIngestionStats{
				ReplicationLagInfo: nil,
			},
			expectedStatus: initReplication,
		},
		{
			name:      "running with lag",
			jobStatus: jobs.StatusRunning,
			replicationInfo: &streampb.StreamIngestionStats{
				ReplicationLagInfo: &streampb.StreamIngestionStats_ReplicationLagInfo{},
			},
			expectedStatus: replicating,
		},
		{
			name:      "running with progress",
			jobStatus: jobs.StatusRunning,
			replicationInfo: &streampb.StreamIngestionStats{
				ReplicationLagInfo: &streampb.StreamIngestionStats_ReplicationLagInfo{},
				IngestionProgress:  &jobspb.StreamIngestionProgress{},
			},
			expectedStatus: replicating,
		},
		{
			name:      "running with cutover",
			jobStatus: jobs.StatusRunning,
			replicationInfo: &streampb.StreamIngestionStats{
				ReplicationLagInfo: &streampb.StreamIngestionStats_ReplicationLagInfo{},
				IngestionProgress: &jobspb.StreamIngestionProgress{
					CutoverTime: hlc.Timestamp{WallTime: 5},
				},
			},
			expectedStatus: cuttingOver,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status := getTenantStatus(tc.jobStatus, tc.replicationInfo)
			require.Equal(t, tc.expectedStatus, status)
		})
	}
}
