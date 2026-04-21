// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *statusServer) ResetSQLStats(
	ctx context.Context, req *serverpb.ResetSQLStatsRequest,
) (*serverpb.ResetSQLStatsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireRepairClusterPermission(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.ResetSQLStatsResponse{}
	localSQLStats := s.sqlServer.pgServer.SQLServer.GetLocalSQLStatsProvider()
	persistedSQLStats := s.sqlServer.pgServer.SQLServer.GetSQLStatsProvider()

	// If we need to reset persisted stats, we delegate to persisted sql stats,
	// which will trigger a system table truncation and RPC fanout under the hood.
	if req.ResetPersistedStats {
		if err := persistedSQLStats.ResetClusterSQLStats(ctx); err != nil {
			return nil, err
		}

		return response, nil
	}

	localReq := &serverpb.ResetSQLStatsRequest{
		NodeID: "local",
		// Only the top level RPC handler handles the reset persisted stats.
		ResetPersistedStats: false,
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			err := localSQLStats.Reset(ctx)
			return response, err
		}
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return status.ResetSQLStats(ctx, localReq)
	}

	resetSQLStats := func(ctx context.Context, status serverpb.RPCStatusClient, _ roachpb.NodeID) (interface{}, error) {
		return status.ResetSQLStats(ctx, localReq)
	}

	var fanoutError error
	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "reset SQL statistics",
		noTimeout,
		s.dialNode,
		resetSQLStats,
		func(nodeID roachpb.NodeID, resp interface{}) {
			// Nothing to do here.
		},
		func(nodeID roachpb.NodeID, nodeFnError error) {
			if nodeFnError != nil {
				fanoutError = errors.CombineErrors(fanoutError, nodeFnError)
			}
		},
	); err != nil {
		return nil, err
	}

	return response, fanoutError
}

// DrainSqlStats satisfies the wire-generated serverpb.StatusServer interface.
// It delegates to DrainSqlStatsStream, which takes the narrower
// RPCStatus_DrainSqlStatsStream interface that both gRPC and DRPC server
// streams (and in-process sinks) implement.
func (s *statusServer) DrainSqlStats(
	req *serverpb.DrainSqlStatsRequest, stream serverpb.Status_DrainSqlStatsServer,
) error {
	return s.DrainSqlStatsStream(req, stream)
}

// DrainSqlStatsStream streams in-memory SQL stats to stream.Send. It is
// the shared entry point for the gRPC and DRPC handlers and for in-process
// callers (which pass a tiny sink type that satisfies the unified
// RPCStatus_DrainSqlStatsStream interface).
//
// The request's NodeID field selects between three modes:
//
//   - "local" (or any parsed NodeID matching this instance): drain this
//     node's in-memory stats. The handler emits chunks of size
//     sqlstats.DrainSqlStatsChunkSize. Stats are removed from memory at
//     drain time; chunks that fail to send are lost — same behavior as
//     the legacy unary form, but the loss window per failure shrinks
//     from "whole node's stats" to "one chunk's stats".
//
//   - any other parsed NodeID: forward to that instance's local handler
//     and proxy chunks back to the caller unchanged.
//
//   - empty string: cluster-fanout. Open a stream to every live SQL
//     instance and forward chunks to the caller's outbound stream as
//     they arrive. The proxy does not merge: the caller receives
//     per-source chunks (possibly with the same fingerprint appearing
//     in chunks from different sources) and is responsible for any
//     deduplication. This bounds proxy memory at
//     O(maxConcurrency × chunk_size) regardless of cluster size or
//     fingerprint count.
func (s *statusServer) DrainSqlStatsStream(
	req *serverpb.DrainSqlStatsRequest, stream serverpb.RPCStatus_DrainSqlStatsStream,
) error {
	ctx := authserver.ForwardSQLIdentityThroughRPCCalls(stream.Context())
	ctx = s.AnnotateCtx(ctx)

	_, isAdmin, err := s.privilegeChecker.GetUserAndRole(ctx)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if !isAdmin {
		return status.Error(codes.PermissionDenied, "user does not have admin role")
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			return s.drainSqlStatsLocal(ctx, stream.Send)
		}
		return s.drainSqlStatsRemote(ctx, requestedNodeID, stream.Send)
	}
	return s.drainSqlStatsClusterFanout(ctx, stream.Send)
}

// drainSqlStatsLocal drains this node's in-memory stats and chunks them
// into the outbound stream. Stats are wiped from memory at drain time;
// chunks that fail to send are lost (same semantics as the legacy unary
// drain). The next coordinated flush cycle picks up newly accumulated
// stats. Every emitted chunk is tagged with the local instance's
// NodeID so a fan-in caller can attribute chunks to their source.
func (s *statusServer) drainSqlStatsLocal(
	ctx context.Context, send func(*serverpb.DrainStatsChunk) error,
) error {
	provider := s.sqlServer.pgServer.SQLServer.GetLocalSQLStatsProvider()
	stmtStats, txnStats, fingerprintCount := provider.DrainStats(ctx)

	chunkSize := int(sqlstats.DrainSqlStatsChunkSize.Get(&s.st.SV))
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	localNodeID := roachpb.NodeID(s.serverIterator.getID())
	chunk := newDrainStatsChunk(chunkSize)
	chunk.proto.NodeID = localNodeID
	flush := func(last bool) error {
		if last {
			chunk.proto.FingerprintCount = fingerprintCount
		}
		if err := send(chunk.proto); err != nil {
			return err
		}
		chunk = newDrainStatsChunk(chunkSize)
		chunk.proto.NodeID = localNodeID
		return nil
	}

	for _, stmt := range stmtStats {
		chunk.proto.Statements = append(chunk.proto.Statements, stmt)
		if chunk.entries() >= chunkSize {
			if err := flush(false); err != nil {
				return err
			}
		}
	}
	for _, txn := range txnStats {
		chunk.proto.Transactions = append(chunk.proto.Transactions, txn)
		if chunk.entries() >= chunkSize {
			if err := flush(false); err != nil {
				return err
			}
		}
	}

	// Always emit a terminal chunk so the consumer sees fingerprintCount
	// even when the drain produced zero entries.
	if err := flush(true); err != nil {
		return err
	}
	log.Dev.VInfof(ctx, 1,
		"drainSqlStatsLocal: streamed %d stmts, %d txns, fingerprint_count=%d",
		len(stmtStats), len(txnStats), fingerprintCount)
	return nil
}

// drainSqlStatsRemote forwards a targeted drain to another instance and
// passes its chunks through unchanged.
func (s *statusServer) drainSqlStatsRemote(
	ctx context.Context, nodeID roachpb.NodeID, send func(*serverpb.DrainStatsChunk) error,
) error {
	statusCli, err := s.dialNode(ctx, nodeID)
	if err != nil {
		return err
	}
	stream, err := statusCli.DrainSqlStats(ctx, &serverpb.DrainSqlStatsRequest{NodeID: "local"})
	if err != nil {
		return err
	}
	return forwardDrainStream(stream, send)
}

// drainSqlStatsClusterFanout opens targeted DrainSqlStats streams to every
// live SQL instance and forwards their chunks to the outbound stream as
// they arrive. The proxy is a pure goroutine fan-in — it does not buffer
// or merge — so peak proxy memory is bounded by
// O(maxConcurrency × chunk_size) regardless of cluster size or fingerprint
// count.
//
// Chunks from different sources are tagged with the source's NodeID
// (set by drainSqlStatsLocal) and may carry the same fingerprint across
// sources; consumers that need deduped data are responsible for the
// merge. After all per-instance drains complete, the fanout sends one
// final summary chunk (Summary populated, no statements/transactions)
// listing every instance's outcome so consumers can correlate received
// chunks with the instances that produced them and react to partial
// failures.
//
// One unreachable instance does not abort the fanout; its error is
// logged and reported in the summary. The fanout returns an error only
// when every instance failed, which signals something cluster-wide is
// wrong.
func (s *statusServer) drainSqlStatsClusterFanout(
	ctx context.Context, send func(*serverpb.DrainStatsChunk) error,
) error {
	// sendMu serializes outbound stream.Send calls. gRPC streams are not
	// safe for concurrent Send.
	var sendMu syncutil.Mutex
	safeSend := func(chunk *serverpb.DrainStatsChunk) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return send(chunk)
	}

	localReq := &serverpb.DrainSqlStatsRequest{NodeID: "local"}
	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (serverpb.RPCStatusClient, error) {
		return s.dialNode(ctx, nodeID)
	}
	// nodeFn opens a stream against the targeted node and forwards every
	// chunk to safeSend until EOF or error. Each per-node forward runs in
	// its own goroutine inside iterateNodesExt; safeSend's mutex ensures
	// chunks from different sources interleave safely on the outbound
	// stream.
	nodeFn := func(ctx context.Context, client serverpb.RPCStatusClient, _ roachpb.NodeID) (struct{}, error) {
		stream, err := client.DrainSqlStats(ctx, localReq)
		if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, forwardDrainStream(stream, safeSend)
	}

	// Track per-node outcomes for the trailing summary chunk. The
	// callbacks run on the iterateNodesExt result-collector goroutine
	// (one at a time), so no synchronization is needed.
	outcomes := make(map[roachpb.NodeID]error)
	if err := iterateNodesExt(ctx, s.serverIterator, s.stopper, "drain SQL statistics",
		dialFn,
		nodeFn,
		func(nodeID roachpb.NodeID, _ struct{}) {
			outcomes[nodeID] = nil
		},
		func(nodeID roachpb.NodeID, nodeFnError error) {
			outcomes[nodeID] = nodeFnError
			log.Dev.Warningf(ctx, "drain SQL statistics: node %d: %v", nodeID, nodeFnError)
		},
		// Cap inflight inbound streams; matches the previous unary fanout's
		// concurrency limit.
		iterateNodesOpts{maxConcurrency: 4},
	); err != nil {
		return err
	}

	summary := &serverpb.DrainStatsSummary{
		Nodes: make([]serverpb.DrainStatsSummary_NodeOutcome, 0, len(outcomes)),
	}
	var successCount int
	var failures error
	for nodeID, nodeErr := range outcomes {
		outcome := serverpb.DrainStatsSummary_NodeOutcome{NodeID: nodeID}
		if nodeErr == nil {
			successCount++
		} else {
			outcome.Error = nodeErr.Error()
			failures = errors.CombineErrors(failures, errors.Wrapf(nodeErr, "node %d", nodeID))
		}
		summary.Nodes = append(summary.Nodes, outcome)
	}

	// If every instance failed there is nothing useful to summarize and the
	// caller should see this as a hard error.
	if successCount == 0 && len(outcomes) > 0 {
		return errors.Wrap(failures, "drain SQL statistics: all instances failed")
	}
	return safeSend(&serverpb.DrainStatsChunk{Summary: summary})
}

// forwardDrainStream pulls every chunk from an inbound DrainSqlStats client
// stream and forwards each one through send. EOF on the inbound stream is
// not an error; any other receive error propagates.
func forwardDrainStream(
	stream serverpb.RPCStatus_DrainSqlStatsClient, send func(*serverpb.DrainStatsChunk) error,
) error {
	for {
		chunk, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := send(chunk); err != nil {
			return err
		}
	}
}

// drainStatsChunk is a small builder around a DrainStatsChunk proto used by
// the local snapshot streamer.
type drainStatsChunk struct {
	proto *serverpb.DrainStatsChunk
}

func newDrainStatsChunk(capacity int) *drainStatsChunk {
	return &drainStatsChunk{
		proto: &serverpb.DrainStatsChunk{
			Statements:   make([]*appstatspb.CollectedStatementStatistics, 0, capacity),
			Transactions: make([]*appstatspb.CollectedTransactionStatistics, 0, capacity),
		},
	}
}

func (c *drainStatsChunk) entries() int {
	return len(c.proto.Statements) + len(c.proto.Transactions)
}
