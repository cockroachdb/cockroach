// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package serverpb

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestLatencyGetter_GetLatency(t *testing.T) {
	type fields struct {
		lastUpdatedTime time.Time
		latencyMap      map[roachpb.NodeID]map[roachpb.NodeID]int64
	}
	type args struct {
		ctx          context.Context
		originNodeID roachpb.NodeID
		targetNodeID roachpb.NodeID
	}

	latencyMap := map[roachpb.NodeID]map[roachpb.NodeID]int64{
		1: {2: 5},
		2: {1: 3},
	}

	tests := []struct {
		name            string
		fields          fields
		args            args
		expectedLatency int64
	}{
		{
			name: "GetLatencyWithoutUpdate",
			fields: fields{
				lastUpdatedTime: timeutil.Now().Add(time.Hour),
				latencyMap:      latencyMap,
			},
			args:            args{ctx: context.Background(), originNodeID: 1, targetNodeID: 2},
			expectedLatency: 5,
		},
		{
			name: "UpdateLatencies",
			fields: fields{
				lastUpdatedTime: timeutil.Now().Add(time.Hour * -1),
				latencyMap:      latencyMap,
			},
			args: args{ctx: context.Background(), originNodeID: 1, targetNodeID: 2},
			// expectedLatency is 0 for this test because when the NodesStatusServer can't be accessed,
			// a latency of 0 is returned so that it isn't displayed on EXPLAIN ANALYZE diagrams.
			expectedLatency: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lg := &LatencyGetter{}
			lg.mu.lastUpdatedTime = tt.fields.lastUpdatedTime
			lg.mu.latencyMap = tt.fields.latencyMap
			if got := lg.GetLatency(tt.args.ctx, tt.args.originNodeID, tt.args.targetNodeID); got != tt.expectedLatency {
				t.Errorf("GetLatency() = %v, want %v", got, tt.expectedLatency)
			}
		})
	}
}
