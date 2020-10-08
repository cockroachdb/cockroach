// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft"
)

type unreliableRaftHandlerFuncs struct {
	// If non-nil, can return false to avoid dropping a msg to rangeID.
	dropReq  func(*kvserver.RaftMessageRequest) bool
	dropHB   func(*kvserver.RaftHeartbeat) bool
	dropResp func(*kvserver.RaftMessageResponse) bool
	// snapErr defaults to returning nil.
	snapErr func(*kvserver.SnapshotRequest_Header) error
}

func noopRaftHandlerFuncs() unreliableRaftHandlerFuncs {
	return unreliableRaftHandlerFuncs{
		dropResp: func(*kvserver.RaftMessageResponse) bool {
			return false
		},
		dropReq: func(*kvserver.RaftMessageRequest) bool {
			return false
		},
		dropHB: func(*kvserver.RaftHeartbeat) bool {
			return false
		},
	}
}

// unreliableRaftHandler drops all Raft messages that are addressed to the
// specified rangeID, but lets all other messages through.
type unreliableRaftHandler struct {
	rangeID roachpb.RangeID
	kvserver.RaftMessageHandler
	unreliableRaftHandlerFuncs
}

func (h *unreliableRaftHandler) HandleRaftRequest(
	ctx context.Context,
	req *kvserver.RaftMessageRequest,
	respStream kvserver.RaftMessageResponseStream,
) *roachpb.Error {
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
			log.Infof(
				ctx,
				"dropping r%d Raft message %s",
				req.RangeID,
				raft.DescribeMessage(req.Message, func([]byte) string {
					return "<omitted>"
				}),
			)

			return nil
		}
	}
	return h.RaftMessageHandler.HandleRaftRequest(ctx, req, respStream)
}

func (h *unreliableRaftHandler) filterHeartbeats(
	hbs []kvserver.RaftHeartbeat,
) []kvserver.RaftHeartbeat {
	if len(hbs) == 0 {
		return hbs
	}
	var cpy []kvserver.RaftHeartbeat
	for i := range hbs {
		hb := &hbs[i]
		if hb.RangeID != h.rangeID || (h.dropHB != nil && !h.dropHB(hb)) {
			cpy = append(cpy, *hb)
		}
	}
	return cpy
}

func (h *unreliableRaftHandler) HandleRaftResponse(
	ctx context.Context, resp *kvserver.RaftMessageResponse,
) error {
	if resp.RangeID == h.rangeID {
		if h.dropResp == nil || h.dropResp(resp) {
			return nil
		}
	}
	return h.RaftMessageHandler.HandleRaftResponse(ctx, resp)
}

func (h *unreliableRaftHandler) HandleSnapshot(
	header *kvserver.SnapshotRequest_Header, respStream kvserver.SnapshotResponseStream,
) error {
	if header.RaftMessageRequest.RangeID == h.rangeID && h.snapErr != nil {
		if err := h.snapErr(header); err != nil {
			return err
		}
	}
	return h.RaftMessageHandler.HandleSnapshot(header, respStream)
}

// mtcStoreRaftMessageHandler exists to allows a store to be stopped and
// restarted while maintaining a partition using an unreliableRaftHandler.
type mtcStoreRaftMessageHandler struct {
	mtc      *multiTestContext
	storeIdx int
}

func (h *mtcStoreRaftMessageHandler) HandleRaftRequest(
	ctx context.Context,
	req *kvserver.RaftMessageRequest,
	respStream kvserver.RaftMessageResponseStream,
) *roachpb.Error {
	store := h.mtc.Store(h.storeIdx)
	if store == nil {
		return roachpb.NewErrorf("store not found")
	}
	return store.HandleRaftRequest(ctx, req, respStream)
}

func (h *mtcStoreRaftMessageHandler) HandleRaftResponse(
	ctx context.Context, resp *kvserver.RaftMessageResponse,
) error {
	store := h.mtc.Store(h.storeIdx)
	if store == nil {
		return errors.New("store not found")
	}
	return store.HandleRaftResponse(ctx, resp)
}

func (h *mtcStoreRaftMessageHandler) HandleSnapshot(
	header *kvserver.SnapshotRequest_Header, respStream kvserver.SnapshotResponseStream,
) error {
	store := h.mtc.Store(h.storeIdx)
	if store == nil {
		return errors.New("store not found")
	}
	return store.HandleSnapshot(header, respStream)
}

// mtcPartitionedRange is a convenient abstraction to create a range on a node
// in a multiTestContext which can be partitioned and unpartitioned.
type mtcPartitionedRange struct {
	rangeID roachpb.RangeID
	mu      struct {
		syncutil.RWMutex
		partitionedNode     int
		partitioned         bool
		partitionedReplicas map[roachpb.ReplicaID]bool
	}
	handlers []kvserver.RaftMessageHandler
}

// setupPartitionedRange sets up an mtcPartitionedRange for the provided mtc,
// rangeID, and node index in the mtc. The range is initially not partitioned.
//
// We're going to set up the cluster with partitioning so that we can
// partition node p from the others. We do this by installing
// unreliableRaftHandler listeners on all three Stores which we can enable
// and disable with an atomic. The handler on the partitioned store filters
// out all messages while the handler on the other two stores only filters
// out messages from the partitioned store. When activated the configuration
// looks like:
//
//           [p]
//          x  x
//         /    \
//        x      x
//      [*]<---->[*]
//
// The activated argument controls whether the partition is activated when this
// function returns.
//
// If replicaID is zero then it is resolved by looking up the replica for the
// partitionedNode of from the current range descriptor of rangeID.
func setupPartitionedRange(
	mtc *multiTestContext,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	partitionedNode int,
	activated bool,
	funcs unreliableRaftHandlerFuncs,
) (*mtcPartitionedRange, error) {
	handlers := make([]kvserver.RaftMessageHandler, 0, len(mtc.stores))
	for i := range mtc.stores {
		handlers = append(handlers, &mtcStoreRaftMessageHandler{
			mtc:      mtc,
			storeIdx: i,
		})
	}
	return setupPartitionedRangeWithHandlers(mtc, rangeID, replicaID, partitionedNode, activated, handlers, funcs)
}

func setupPartitionedRangeWithHandlers(
	mtc *multiTestContext,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	partitionedNode int,
	activated bool,
	handlers []kvserver.RaftMessageHandler,
	funcs unreliableRaftHandlerFuncs,
) (*mtcPartitionedRange, error) {
	pr := &mtcPartitionedRange{
		rangeID:  rangeID,
		handlers: make([]kvserver.RaftMessageHandler, 0, len(handlers)),
	}
	pr.mu.partitioned = activated
	pr.mu.partitionedNode = partitionedNode
	if replicaID == 0 {
		partRepl, err := mtc.Store(partitionedNode).GetReplica(rangeID)
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
	for i := range mtc.stores {
		s := i
		h := &unreliableRaftHandler{
			rangeID:                    rangeID,
			RaftMessageHandler:         handlers[s],
			unreliableRaftHandlerFuncs: funcs,
		}
		// Only filter messages from the partitioned store on the other
		// two stores.
		if h.dropReq == nil {
			h.dropReq = func(req *kvserver.RaftMessageRequest) bool {
				pr.mu.RLock()
				defer pr.mu.RUnlock()
				return pr.mu.partitioned &&
					(s == pr.mu.partitionedNode ||
						req.FromReplica.StoreID == roachpb.StoreID(pr.mu.partitionedNode)+1)
			}
		}
		if h.dropHB == nil {
			h.dropHB = func(hb *kvserver.RaftHeartbeat) bool {
				pr.mu.RLock()
				defer pr.mu.RUnlock()
				if !pr.mu.partitioned {
					return false
				}
				if s == partitionedNode {
					return true
				}
				return pr.mu.partitionedReplicas[hb.FromReplicaID]
			}
		}
		if h.snapErr == nil {
			h.snapErr = func(header *kvserver.SnapshotRequest_Header) error {
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
		pr.handlers = append(pr.handlers, h)
		mtc.transport.Listen(mtc.stores[s].Ident.StoreID, h)
	}
	return pr, nil
}

func (pr *mtcPartitionedRange) deactivate() { pr.set(false) }
func (pr *mtcPartitionedRange) activate()   { pr.set(true) }
func (pr *mtcPartitionedRange) set(active bool) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.mu.partitioned = active
}

func (pr *mtcPartitionedRange) addReplica(replicaID roachpb.ReplicaID) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.mu.partitionedReplicas[replicaID] = true
}

func (pr *mtcPartitionedRange) extend(
	mtc *multiTestContext,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	partitionedNode int,
	activated bool,
	funcs unreliableRaftHandlerFuncs,
) (*mtcPartitionedRange, error) {
	return setupPartitionedRangeWithHandlers(mtc, rangeID, replicaID, partitionedNode, activated, pr.handlers, funcs)
}
