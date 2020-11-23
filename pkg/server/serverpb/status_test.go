package serverpb

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func TestLatencyGetter_GetLatency(t *testing.T) {
	type fields struct {
		NodesStatusServer *OptionalNodesStatusServer
		syncutil.Mutex
		lastUpdatedTime time.Time
		latencyMap      map[roachpb.NodeID]map[roachpb.NodeID]int64
	}
	type args struct {
		ctx          context.Context
		originNodeID roachpb.NodeID
		targetNodeID roachpb.NodeID
	}

	latencyMap := map[roachpb.NodeID]map[roachpb.NodeID]int64{
		1: map[roachpb.NodeID]int64{2: 5},
		2: map[roachpb.NodeID]int64{1: 3},
	}

	tests := []struct {
		name            string
		fields          fields
		args            args
		expectedLatency int64
	}{
		{
			name:            "GetLatencyWithoutUpdate",
			fields:          fields{NodesStatusServer: &OptionalNodesStatusServer{}, lastUpdatedTime: time.Now().Add(time.Hour), latencyMap: latencyMap},
			args:            args{ctx: context.Background(), originNodeID: 1, targetNodeID: 2},
			expectedLatency: 5,
		},
		{
			name:            "UpdateLatencies",
			fields:          fields{NodesStatusServer: &OptionalNodesStatusServer{}, lastUpdatedTime: time.Now().Add(time.Hour * -1), latencyMap: latencyMap},
			args:            args{ctx: context.Background(), originNodeID: 1, targetNodeID: 2},
			expectedLatency: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lg := &LatencyGetter{
				NodesStatusServer: tt.fields.NodesStatusServer,
			}
			lg.mu.lastUpdatedTime = tt.fields.lastUpdatedTime
			lg.mu.latencyMap = tt.fields.latencyMap
			if got := lg.GetLatency(tt.args.ctx, tt.args.originNodeID, tt.args.targetNodeID); got != tt.expectedLatency {
				t.Errorf("GetLatency() = %v, want %v", got, tt.expectedLatency)
			}
		})
	}
}
