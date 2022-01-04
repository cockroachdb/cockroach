// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"container/heap"
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stmtStream struct {
	stream  *serverpb.Status_StreamStatementStatsClient
	curStmt *serverpb.StatementsResponse_CollectedStatementStatistics
}

type StmtStreamHeap []*stmtStream

func (s StmtStreamHeap) Len() int { return len(s) }

func compareStmtStats(lhs, rhs *serverpb.StatementsResponse_CollectedStatementStatistics) int {
	// We sort the elements based on the stmtKey struct in sqlstats pkg,
	//as stats with the same stmtKey will later be aggregated.

	// Compare app names.
	if lhs.Key.KeyData.App < rhs.Key.KeyData.App {
		return -1
	}
	if lhs.Key.KeyData.App > rhs.Key.KeyData.App {
		return 1
	}

	// Compare fingerprint ids.
	if lhs.ID < rhs.ID {
		return -1
	}
	if lhs.ID > rhs.ID {
		return 1
	}

	// Compare transaction fingerprint ID.
	if lhs.Key.KeyData.TransactionFingerprintID < rhs.Key.KeyData.TransactionFingerprintID {
		return -1
	}
	if lhs.Key.KeyData.TransactionFingerprintID > rhs.Key.KeyData.TransactionFingerprintID {
		return 1
	}
	return 0
}

func (s StmtStreamHeap) Less(i, j int) bool {
	return compareStmtStats(s[i].curStmt, s[j].curStmt) <= 0
}

func (s StmtStreamHeap) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s *StmtStreamHeap) Push(x interface{}) {
	item := x.(*stmtStream)
	*s = append(*s, item)
}

func (s *StmtStreamHeap) Pop() interface{} {
	oldHeap := *s
	heapSize := len(oldHeap)
	item := oldHeap[heapSize-1]
	oldHeap[heapSize-1] = nil
	*s = oldHeap[0 : heapSize-1]
	return item
}

func streamLocalStats(
	ctx context.Context,
	nodeID roachpb.NodeID,
	sqlServer *SQLServer,
	stream *serverpb.Status_StreamStatementStatsServer,
) error {
	stmtStats, err := sqlServer.pgServer.SQLServer.GetUnscrubbedStmtStats(ctx)
	if err != nil {
		return err
	}

	sort.Slice(stmtStats, func(i, j int) bool {
		// We sort the elements based on the stmtKey struct in sqlstats pkg,
		//as stats with the same stmtKey will later be aggregated.
		lhs := stmtStats[i]
		rhs := stmtStats[j]

		// Compare app names.
		if lhs.Key.App < rhs.Key.App {
			return true
		}
		if lhs.Key.App > rhs.Key.App {
			return false
		}

		// Compare fingerprint ids.
		if lhs.ID < rhs.ID {
			return true
		}
		if lhs.ID > rhs.ID {
			return false
		}

		// Compare transaction fingerprint ID.
		if lhs.Key.TransactionFingerprintID < rhs.Key.TransactionFingerprintID {
			return true
		}
		if lhs.Key.TransactionFingerprintID > rhs.Key.TransactionFingerprintID {
			return false
		}

		// The two elements are equal.
		return true
	})

	for _, stmt := range stmtStats {
		resp := &serverpb.StatementsResponse_CollectedStatementStatistics{
			Key: serverpb.StatementsResponse_ExtendedStatementStatisticsKey{
				KeyData: stmt.Key,
				NodeID:  nodeID,
			},
			ID:    stmt.ID,
			Stats: stmt.Stats,
		}
		if err := (*stream).Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func (s *statusServer) StreamStatementStats(
	req *serverpb.StatementsRequest, stream serverpb.Status_StreamStatementStatsServer,
) error {
	ctx := stream.Context()
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireViewActivityPermission(ctx); err != nil {
		return err
	}

	if s.gossip.NodeID.Get() == 0 {
		return status.Errorf(codes.Unavailable, "nodeID not set")
	}

	localReq := &serverpb.StatementsRequest{
		NodeID: "local",
	}

	var nodeStreams []*serverpb.Status_StreamStatementStatsClient

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			return streamLocalStats(ctx, requestedNodeID, s.admin.server.sqlServer, &stream)
		}

		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return err
		}

		nodeStream, err := status.StreamStatementStats(ctx, localReq)

		if err != nil {
			return err
		}
		nodeStreams = append(nodeStreams, &nodeStream)
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	nodeStatement := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		return status.StreamStatementStats(ctx, localReq)
	}

	if err := s.iterateNodes(ctx, fmt.Sprintf("statement statistics for node %s", req.NodeID),
		dialFn,
		nodeStatement,
		func(nodeID roachpb.NodeID, resp interface{}) {
			nodeStream := resp.(*serverpb.Status_StreamStatementStatsClient)
			nodeStreams = append(nodeStreams, nodeStream)
		},
		func(nodeID roachpb.NodeID, err error) {
			// TODO(couchand): do something here...
		},
	); err != nil {
		return err
	}

	var stmtStreamsHeap StmtStreamHeap
	heap.Init(&stmtStreamsHeap)

	for _, nodeStream := range nodeStreams {
		stats, err := (*nodeStream).Recv()
		if err != nil {
			continue
		}
		heap.Push(&stmtStreamsHeap, stmtStream{
			stream:  nodeStream,
			curStmt: stats,
		})
	}

	nextStatInStreams := func() *serverpb.StatementsResponse_CollectedStatementStatistics {
		// Returns the stream containing the next stmt, and gets the next item
		// from that stream, adding the stream back to the heap if still valid.
		// Returns nil if there are no more valid streams.

		if stmtStreamsHeap.Len() == 0 {
			return nil
		}

		curStream := heap.Pop(&stmtStreamsHeap).(*stmtStream)
		nextStmt, err := (*curStream.stream).Recv()
		if err == nil {
			// If there were no errors, add stream back to heap.
			// TODO: handle non-EOF errors?
			curStream.curStmt = nextStmt
			heap.Push(&stmtStreamsHeap, curStream)
		}
		return curStream.curStmt
	}

	curStmt := nextStatInStreams()
	for curStmt != nil {
		nextStmt := nextStatInStreams()

		for nextStmt != nil {
			// We aggregate stmts with the same stmtKey,
			// until we encounter a stmt with a different key.
			if compareStmtStats(curStmt, nextStmt) != 0 {
				break
			}
			curStmt.Stats.Add(&nextStmt.Stats)
			nextStmt = nextStatInStreams()
		}

		if err := stream.Send(curStmt); err != nil {
			return err
		}

		// Set the current statement to aggregate to the next
		// statement in the streams.
		curStmt = nextStmt
	}

	return nil
}
