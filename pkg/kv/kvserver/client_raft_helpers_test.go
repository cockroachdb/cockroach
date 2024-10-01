// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type unreliableRaftHandlerFuncs struct {
	// If non-nil, can return false to avoid dropping the msg to
	// unreliableRaftHandler.rangeID. If nil, all messages pertaining to the
	// respective range are dropped.
	dropReq  func(*kvserverpb.RaftMessageRequest) bool
	dropHB   func(*kvserverpb.RaftHeartbeat) bool
	dropResp func(*kvserverpb.RaftMessageResponse) bool
	// snapErr and delegateErr default to returning nil.
	snapErr     func(*kvserverpb.SnapshotRequest_Header) error
	delegateErr func(request *kvserverpb.DelegateSendSnapshotRequest) error
}

func noopRaftHandlerFuncs() unreliableRaftHandlerFuncs {
	return unreliableRaftHandlerFuncs{
		dropResp: func(*kvserverpb.RaftMessageResponse) bool {
			return false
		},
		dropReq: func(*kvserverpb.RaftMessageRequest) bool {
			return false
		},
		dropHB: func(*kvserverpb.RaftHeartbeat) bool {
			return false
		},
	}
}

// unreliableRaftHandler drops all Raft messages that are addressed to the
// specified rangeID, but lets all other messages through.
type unreliableRaftHandler struct {
	name    string
	rangeID roachpb.RangeID
	kvserver.IncomingRaftMessageHandler
	unreliableRaftHandlerFuncs
}

func (h *unreliableRaftHandler) HandleRaftRequest(
	ctx context.Context,
	req *kvserverpb.RaftMessageRequest,
	respStream kvserver.RaftMessageResponseStream,
) *kvpb.Error {
	if len(req.Heartbeats)+len(req.HeartbeatResps) > 0 {
		reqCpy := *req
		req = &reqCpy
		req.Heartbeats = h.filterHeartbeats(req.Heartbeats)
		req.HeartbeatResps = h.filterHeartbeats(req.HeartbeatResps)
		if len(req.Heartbeats)+len(req.HeartbeatResps) == 0 {
			// Entirely filtered.
			return nil
		}
	} else if req.RangeID == h.rangeID {
		if h.dropReq == nil || h.dropReq(req) {
			var prefix string
			if h.name != "" {
				prefix = fmt.Sprintf("[%s] ", h.name)
			}
			log.Infof(
				ctx,
				"%sdropping r%d Raft message %s",
				prefix,
				req.RangeID,
				raft.DescribeMessage(req.Message, func([]byte) string {
					return "<omitted>"
				}),
			)

			return nil
		}
		if !h.dropReq(req) && log.V(1) {
			// Debug logging, even if requests aren't dropped. This is a
			// convenient way to observe all raft messages in unit tests when
			// run using --vmodule='client_raft_helpers_test=1'.
			var prefix string
			if h.name != "" {
				prefix = fmt.Sprintf("[%s] ", h.name)
			}
			log.Infof(
				ctx,
				"%s [raft] r%d Raft message %s",
				prefix,
				req.RangeID,
				raft.DescribeMessage(req.Message, func([]byte) string {
					return "<omitted>"
				}),
			)
		}
	}
	return h.IncomingRaftMessageHandler.HandleRaftRequest(ctx, req, respStream)
}

func (h *unreliableRaftHandler) filterHeartbeats(
	hbs []kvserverpb.RaftHeartbeat,
) []kvserverpb.RaftHeartbeat {
	if len(hbs) == 0 {
		return hbs
	}
	var cpy []kvserverpb.RaftHeartbeat
	for i := range hbs {
		hb := &hbs[i]
		if hb.RangeID != h.rangeID || (h.dropHB != nil && !h.dropHB(hb)) {
			cpy = append(cpy, *hb)
		}
	}
	return cpy
}

func (h *unreliableRaftHandler) HandleRaftResponse(
	ctx context.Context, resp *kvserverpb.RaftMessageResponse,
) error {
	if resp.RangeID == h.rangeID {
		if h.dropResp == nil || h.dropResp(resp) {
			return nil
		}
	}
	return h.IncomingRaftMessageHandler.HandleRaftResponse(ctx, resp)
}

func (h *unreliableRaftHandler) HandleSnapshot(
	ctx context.Context,
	header *kvserverpb.SnapshotRequest_Header,
	respStream kvserver.SnapshotResponseStream,
) error {
	if header.RaftMessageRequest.RangeID == h.rangeID && h.snapErr != nil {
		if err := h.snapErr(header); err != nil {
			return err
		}
	}
	return h.IncomingRaftMessageHandler.HandleSnapshot(ctx, header, respStream)
}

func (h *unreliableRaftHandler) HandleDelegatedSnapshot(
	ctx context.Context, req *kvserverpb.DelegateSendSnapshotRequest,
) *kvserverpb.DelegateSnapshotResponse {
	if req.RangeID == h.rangeID && h.delegateErr != nil {
		if err := h.delegateErr(req); err != nil {
			return &kvserverpb.DelegateSnapshotResponse{
				Status:       kvserverpb.DelegateSnapshotResponse_ERROR,
				EncodedError: errors.EncodeError(context.Background(), err),
			}
		}
	}
	return h.IncomingRaftMessageHandler.HandleDelegatedSnapshot(ctx, req)
}

// testClusterStoreRaftMessageHandler exists to allows a store to be stopped and
// restarted while maintaining a partition using an unreliableRaftHandler.
type testClusterStoreRaftMessageHandler struct {
	tc       *testcluster.TestCluster
	storeIdx int
}

func (h *testClusterStoreRaftMessageHandler) getStore() (*kvserver.Store, error) {
	ts := h.tc.Servers[h.storeIdx]
	return ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
}

func (h *testClusterStoreRaftMessageHandler) HandleRaftRequest(
	ctx context.Context,
	req *kvserverpb.RaftMessageRequest,
	respStream kvserver.RaftMessageResponseStream,
) *kvpb.Error {
	store, err := h.getStore()
	if err != nil {
		return kvpb.NewError(err)
	}
	return store.HandleRaftRequest(ctx, req, respStream)
}

func (h *testClusterStoreRaftMessageHandler) HandleRaftResponse(
	ctx context.Context, resp *kvserverpb.RaftMessageResponse,
) error {
	store, err := h.getStore()
	if err != nil {
		return err
	}
	return store.HandleRaftResponse(ctx, resp)
}

func (h *testClusterStoreRaftMessageHandler) HandleSnapshot(
	ctx context.Context,
	header *kvserverpb.SnapshotRequest_Header,
	respStream kvserver.SnapshotResponseStream,
) error {
	store, err := h.getStore()
	if err != nil {
		return err
	}
	return store.HandleSnapshot(ctx, header, respStream)
}

func (h *testClusterStoreRaftMessageHandler) HandleDelegatedSnapshot(
	ctx context.Context, req *kvserverpb.DelegateSendSnapshotRequest,
) *kvserverpb.DelegateSnapshotResponse {
	store, err := h.getStore()
	if err != nil {
		return &kvserverpb.DelegateSnapshotResponse{
			Status:       kvserverpb.DelegateSnapshotResponse_ERROR,
			EncodedError: errors.EncodeError(context.Background(), err),
		}
	}
	return store.HandleDelegatedSnapshot(ctx, req)
}

// testClusterPartitionedRange is a convenient abstraction to create a range on a node
// in a multiTestContext which can be partitioned and unpartitioned.
type testClusterPartitionedRange struct {
	rangeID roachpb.RangeID
	mu      struct {
		syncutil.RWMutex
		partitionedNodeIdx  int
		partitioned         bool
		partitionedReplicas map[roachpb.ReplicaID]bool
	}
	handlers []kvserver.IncomingRaftMessageHandler
}

// setupPartitionedRange sets up an testClusterPartitionedRange for the provided
// TestCluster, rangeID, and node index in the TestCluster. The range is
// initially not partitioned.
//
// We're going to set up the cluster with partitioning so that we can
// partition node p from the others. We do this by installing
// unreliableRaftHandler listeners on all three Stores which we can enable
// and disable with an atomic. The handler on the partitioned store filters
// out all messages while the handler on the other two stores only filters
// out messages from the partitioned store. When activated the configuration
// looks like:
//
//	     [p]
//	    x  x
//	   /    \
//	  x      x
//	[*]<---->[*]
//
// The activated argument controls whether the partition is activated when this
// function returns.
//
// If replicaID is zero then it is resolved by looking up the replica for the
// partitionedNode of from the current range descriptor of rangeID.
func setupPartitionedRange(
	tc *testcluster.TestCluster,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	partitionedNodeIdx int,
	activated bool,
	funcs unreliableRaftHandlerFuncs,
) (*testClusterPartitionedRange, error) {
	handlers := make([]kvserver.IncomingRaftMessageHandler, 0, len(tc.Servers))
	for i := range tc.Servers {
		handlers = append(handlers, &testClusterStoreRaftMessageHandler{
			tc:       tc,
			storeIdx: i,
		})
	}
	return setupPartitionedRangeWithHandlers(tc, rangeID, replicaID, partitionedNodeIdx, activated, handlers, funcs)
}

func setupPartitionedRangeWithHandlers(
	tc *testcluster.TestCluster,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	partitionedNodeIdx int,
	activated bool,
	handlers []kvserver.IncomingRaftMessageHandler,
	funcs unreliableRaftHandlerFuncs,
) (*testClusterPartitionedRange, error) {
	pr := &testClusterPartitionedRange{
		rangeID:  rangeID,
		handlers: make([]kvserver.IncomingRaftMessageHandler, 0, len(handlers)),
	}
	pr.mu.partitioned = activated
	pr.mu.partitionedNodeIdx = partitionedNodeIdx
	if replicaID == 0 {
		ts := tc.Servers[partitionedNodeIdx]
		store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
		if err != nil {
			return nil, err
		}
		partRepl, err := store.GetReplica(rangeID)
		if err != nil {
			return nil, err
		}
		partReplDesc, err := partRepl.GetReplicaDescriptor()
		if err != nil {
			return nil, err
		}
		replicaID = partReplDesc.ReplicaID
	}
	pr.mu.partitionedReplicas = map[roachpb.ReplicaID]bool{
		replicaID: true,
	}
	for i := range tc.Servers {
		s := i
		h := &unreliableRaftHandler{
			rangeID:                    rangeID,
			IncomingRaftMessageHandler: handlers[s],
			unreliableRaftHandlerFuncs: funcs,
		}
		// Only filter messages from the partitioned store on the other
		// two stores.
		if h.dropReq == nil {
			h.dropReq = func(req *kvserverpb.RaftMessageRequest) bool {
				pr.mu.RLock()
				defer pr.mu.RUnlock()
				return pr.mu.partitioned &&
					(s == pr.mu.partitionedNodeIdx ||
						req.FromReplica.StoreID == roachpb.StoreID(pr.mu.partitionedNodeIdx)+1)
			}
		}
		if h.dropHB == nil {
			h.dropHB = func(hb *kvserverpb.RaftHeartbeat) bool {
				pr.mu.RLock()
				defer pr.mu.RUnlock()
				if !pr.mu.partitioned {
					return false
				}
				if s == partitionedNodeIdx {
					return true
				}
				return pr.mu.partitionedReplicas[hb.FromReplicaID]
			}
		}
		if h.dropResp == nil {
			h.dropResp = func(resp *kvserverpb.RaftMessageResponse) bool {
				pr.mu.RLock()
				defer pr.mu.RUnlock()
				return pr.mu.partitioned &&
					(s == pr.mu.partitionedNodeIdx ||
						resp.FromReplica.StoreID == roachpb.StoreID(pr.mu.partitionedNodeIdx)+1)
			}
		}
		if h.snapErr == nil {
			h.snapErr = func(header *kvserverpb.SnapshotRequest_Header) error {
				pr.mu.RLock()
				defer pr.mu.RUnlock()
				if !pr.mu.partitioned {
					return nil
				}
				if pr.mu.partitionedReplicas[header.RaftMessageRequest.ToReplica.ReplicaID] {
					return errors.New("partitioned")
				}
				return nil
			}
		}
		if h.delegateErr == nil {
			h.delegateErr = func(resp *kvserverpb.DelegateSendSnapshotRequest) error {
				pr.mu.RLock()
				defer pr.mu.RUnlock()
				if pr.mu.partitionedReplicas[resp.DelegatedSender.ReplicaID] {
					return errors.New("partitioned")
				}
				return nil
			}
		}
		pr.handlers = append(pr.handlers, h)
		tc.Servers[s].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(tc.Target(s).StoreID, h)
	}
	return pr, nil
}

func (pr *testClusterPartitionedRange) deactivate() { pr.set(false) }
func (pr *testClusterPartitionedRange) activate()   { pr.set(true) }
func (pr *testClusterPartitionedRange) set(active bool) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.mu.partitioned = active
}

func (pr *testClusterPartitionedRange) addReplica(replicaID roachpb.ReplicaID) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.mu.partitionedReplicas[replicaID] = true
}

func (pr *testClusterPartitionedRange) extend(
	tc *testcluster.TestCluster,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	partitionedNode int,
	activated bool,
	funcs unreliableRaftHandlerFuncs,
) (*testClusterPartitionedRange, error) {
	return setupPartitionedRangeWithHandlers(tc, rangeID, replicaID, partitionedNode, activated, pr.handlers, funcs)
}

// dropRaftMessagesFrom sets up a Raft message handler on the given server that
// drops inbound Raft messages from the given range and replica IDs. Outbound
// messages are not affected, and must be dropped on the receiver.
//
// If cond is given, messages are only dropped when the atomic bool is true.
// Otherwise, messages are always dropped.
//
// This will replace the previous message handler, if any.
func dropRaftMessagesFrom(
	t *testing.T,
	srv serverutils.TestServerInterface,
	rangeID roachpb.RangeID,
	fromReplicaIDs []roachpb.ReplicaID,
	cond *atomic.Bool,
) {
	dropFrom := map[roachpb.ReplicaID]bool{}
	for _, id := range fromReplicaIDs {
		dropFrom[id] = true
	}
	shouldDrop := func(rID roachpb.RangeID, from roachpb.ReplicaID) bool {
		return rID == rangeID && (cond == nil || cond.Load()) && dropFrom[from]
	}

	store, err := srv.GetStores().(*kvserver.Stores).GetStore(srv.GetFirstStoreID())
	require.NoError(t, err)
	srv.RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(store.StoreID(), &unreliableRaftHandler{
		rangeID:                    rangeID,
		IncomingRaftMessageHandler: store,
		unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
			dropHB: func(hb *kvserverpb.RaftHeartbeat) bool {
				return shouldDrop(hb.RangeID, hb.FromReplicaID)
			},
			dropReq: func(req *kvserverpb.RaftMessageRequest) bool {
				return shouldDrop(req.RangeID, req.FromReplica.ReplicaID)
			},
			dropResp: func(resp *kvserverpb.RaftMessageResponse) bool {
				return shouldDrop(resp.RangeID, resp.FromReplica.ReplicaID)
			},
		},
	})
}

// getMapsDiff returns the difference between the values of corresponding
// metrics in two maps. Assumption: beforeMap and afterMap contain the same set
// of keys.
func getMapsDiff(beforeMap map[string]int64, afterMap map[string]int64) map[string]int64 {
	diffMap := make(map[string]int64)
	for metricName, beforeValue := range beforeMap {
		if v, ok := afterMap[metricName]; ok {
			diffMap[metricName] = v - beforeValue
		}
	}
	return diffMap
}
