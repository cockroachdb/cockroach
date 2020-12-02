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
	context "context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SQLStatusServer is a smaller version of the serverpb.StatusInterface which
// includes only the methods used by the SQL subsystem.
type SQLStatusServer interface {
	ListSessions(context.Context, *ListSessionsRequest) (*ListSessionsResponse, error)
	ListLocalSessions(context.Context, *ListSessionsRequest) (*ListSessionsResponse, error)
	CancelQuery(context.Context, *CancelQueryRequest) (*CancelQueryResponse, error)
	CancelSession(context.Context, *CancelSessionRequest) (*CancelSessionResponse, error)
}

// OptionalNodesStatusServer is a StatusServer that is only optionally present
// inside the SQL subsystem. In practice, it is present on the system tenant,
// and not present on "regular" tenants.
type OptionalNodesStatusServer struct {
	w errorutil.TenantSQLDeprecatedWrapper // stores serverpb.StatusServer
}

// MakeOptionalNodesStatusServer initializes and returns an
// OptionalNodesStatusServer. The provided server will be returned via
// OptionalNodesStatusServer() if and only if it is not nil.
func MakeOptionalNodesStatusServer(s NodesStatusServer) OptionalNodesStatusServer {
	return OptionalNodesStatusServer{
		// Return the status server from OptionalSQLStatusServer() only if one was provided.
		// We don't have any calls to .Deprecated().
		w: errorutil.MakeTenantSQLDeprecatedWrapper(s, s != nil /* exposed */),
	}
}

// NodesStatusServer is the subset of the serverpb.StatusInterface that is used
// by the SQL subsystem but is unavailable to tenants.
type NodesStatusServer interface {
	Nodes(context.Context, *NodesRequest) (*NodesResponse, error)
}

// OptionalNodesStatusServer returns the wrapped NodesStatusServer, if it is
// available. If it is not, an error referring to the optionally supplied issues
// is returned.
func (s *OptionalNodesStatusServer) OptionalNodesStatusServer(
	issue int,
) (NodesStatusServer, error) {
	v, err := s.w.OptionalErr(issue)
	if err != nil {
		return nil, err
	}
	return v.(NodesStatusServer), nil
}

// LatencyGetter stores the map of latencies obtained from the NodesStatusServer.
// These latencies are displayed on the streams of EXPLAIN ANALYZE diagrams.
// This struct is put here to avoid import cycles.
type LatencyGetter struct {
	NodesStatusServer *OptionalNodesStatusServer
	mu                struct {
		syncutil.Mutex
		lastUpdatedTime time.Time
		latencyMap      map[roachpb.NodeID]map[roachpb.NodeID]int64
	}
}

const updateThreshold = 5 * time.Second

// GetLatency is a helper function that updates the latencies between nodes
// if the time since the last update exceeds the updateThreshold. This function
// returns the latency between the origin node and the target node.
func (lg *LatencyGetter) GetLatency(
	ctx context.Context, originNodeID roachpb.NodeID, targetNodeID roachpb.NodeID,
) int64 {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if timeutil.Since(lg.mu.lastUpdatedTime) < updateThreshold {
		return lg.mu.latencyMap[originNodeID][targetNodeID]
	}
	// Update latencies in latencyMap.
	if lg.NodesStatusServer == nil {
		// When latency is 0, it is not shown on EXPLAIN ANALYZE diagrams.
		return 0
	}
	ss, err := lg.NodesStatusServer.OptionalNodesStatusServer(errorutil.FeatureNotAvailableToNonSystemTenantsIssue)
	if err != nil {
		// When latency is 0, it is not shown on EXPLAIN ANALYZE diagrams.
		return 0
	}
	if lg.mu.latencyMap == nil {
		lg.mu.latencyMap = make(map[roachpb.NodeID]map[roachpb.NodeID]int64)
	}
	response, err := ss.Nodes(ctx, &NodesRequest{})
	if err != nil {
		// When latency is 0, it is not shown on EXPLAIN ANALYZE diagrams.
		return 0
	}
	for i := 0; i < len(response.Nodes); i++ {
		sendingNodeID := response.Nodes[i].Desc.NodeID
		if lg.mu.latencyMap[sendingNodeID] == nil {
			lg.mu.latencyMap[sendingNodeID] = make(map[roachpb.NodeID]int64)
		}
		for j := 0; j < len(response.Nodes); j++ {
			receivingNodeID := response.Nodes[i].Desc.NodeID
			if sendingNodeID != receivingNodeID {
				lg.mu.latencyMap[sendingNodeID][receivingNodeID] = response.Nodes[i].Activity[receivingNodeID].Latency
			}
		}
	}
	lg.mu.lastUpdatedTime = timeutil.Now()
	return lg.mu.latencyMap[originNodeID][targetNodeID]
}
