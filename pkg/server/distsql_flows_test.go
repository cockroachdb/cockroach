// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestMergeDistSQLRemoteFlows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	flowIDs := make([]execinfrapb.FlowID, 4)
	for i := range flowIDs {
		flowIDs[i].UUID = uuid.MakeV4()
	}
	sort.Slice(flowIDs, func(i, j int) bool {
		return bytes.Compare(flowIDs[i].GetBytes(), flowIDs[j].GetBytes()) < 0
	})
	ts := make([]time.Time, 4)
	for i := range ts {
		ts[i] = timeutil.Now()
	}

	for _, tc := range []struct {
		a        []serverpb.DistSQLRemoteFlows
		b        []serverpb.DistSQLRemoteFlows
		expected []serverpb.DistSQLRemoteFlows
	}{
		// a is empty
		{
			a: []serverpb.DistSQLRemoteFlows{},
			b: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1]},
						{NodeID: 2, Timestamp: ts[2]},
						{NodeID: 3, Timestamp: ts[3]},
					},
				},
				{
					FlowID: flowIDs[1],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1]},
					},
				},
			},
			expected: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1]},
						{NodeID: 2, Timestamp: ts[2]},
						{NodeID: 3, Timestamp: ts[3]},
					},
				},
				{
					FlowID: flowIDs[1],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1]},
					},
				},
			},
		},
		// b is empty
		{
			a: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1]},
						{NodeID: 2, Timestamp: ts[2]},
						{NodeID: 3, Timestamp: ts[3]},
					},
				},
				{
					FlowID: flowIDs[1],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1]},
					},
				},
			},
			b: []serverpb.DistSQLRemoteFlows{},
			expected: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1]},
						{NodeID: 2, Timestamp: ts[2]},
						{NodeID: 3, Timestamp: ts[3]},
					},
				},
				{
					FlowID: flowIDs[1],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1]},
					},
				},
			},
		},
		// both non-empty with some intersections
		{
			a: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1]},
						{NodeID: 2, Timestamp: ts[2]},
						{NodeID: 3, Timestamp: ts[3]},
					},
				},
				{
					FlowID: flowIDs[2],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 3, Timestamp: ts[3]},
					},
				},
				{
					FlowID: flowIDs[3],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 0, Timestamp: ts[0]},
					},
				},
			},
			b: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 0, Timestamp: ts[0]},
					},
				},
				{
					FlowID: flowIDs[1],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 0, Timestamp: ts[0]},
						{NodeID: 1, Timestamp: ts[1]},
						{NodeID: 2, Timestamp: ts[2]},
					},
				},
				{
					FlowID: flowIDs[3],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1]},
						{NodeID: 2, Timestamp: ts[2]},
					},
				},
			},
			expected: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 0, Timestamp: ts[0]},
						{NodeID: 1, Timestamp: ts[1]},
						{NodeID: 2, Timestamp: ts[2]},
						{NodeID: 3, Timestamp: ts[3]},
					},
				},
				{
					FlowID: flowIDs[1],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 0, Timestamp: ts[0]},
						{NodeID: 1, Timestamp: ts[1]},
						{NodeID: 2, Timestamp: ts[2]},
					},
				},
				{
					FlowID: flowIDs[2],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 3, Timestamp: ts[3]},
					},
				},
				{
					FlowID: flowIDs[3],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 0, Timestamp: ts[0]},
						{NodeID: 1, Timestamp: ts[1]},
						{NodeID: 2, Timestamp: ts[2]},
					},
				},
			},
		},
	} {
		require.Equal(t, tc.expected, mergeDistSQLRemoteFlows(tc.a, tc.b))
	}
}
