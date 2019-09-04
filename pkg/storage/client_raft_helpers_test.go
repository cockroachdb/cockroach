// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_test

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"go.etcd.io/etcd/raft"
)

// unreliableRaftHandler drops all Raft messages that are addressed to the
// specified rangeID, but lets all other messages through.
type unreliableRaftHandler struct {
	rangeID roachpb.RangeID
	storage.RaftMessageHandler
	// If non-nil, can return false to avoid dropping a msg to rangeID
	dropReq  func(*storage.RaftMessageRequest) bool
	dropHB   func(*storage.RaftHeartbeat) bool
	dropResp func(*storage.RaftMessageResponse) bool
}

func (h *unreliableRaftHandler) HandleRaftRequest(
	ctx context.Context,
	req *storage.RaftMessageRequest,
	respStream storage.RaftMessageResponseStream,
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
				"dropping Raft message %s",
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
	hbs []storage.RaftHeartbeat,
) []storage.RaftHeartbeat {
	if len(hbs) == 0 {
		return hbs
	}
	var cpy []storage.RaftHeartbeat
	for i := range hbs {
		hb := &hbs[i]
		if hb.RangeID != h.rangeID || (h.dropHB != nil && !h.dropHB(hb)) {
			cpy = append(cpy, *hb)
		}
	}
	return cpy
}

func (h *unreliableRaftHandler) HandleRaftResponse(
	ctx context.Context, resp *storage.RaftMessageResponse,
) error {
	if resp.RangeID == h.rangeID {
		if h.dropResp == nil || h.dropResp(resp) {
			return nil
		}
	}
	return h.RaftMessageHandler.HandleRaftResponse(ctx, resp)
}

// mtcStoreRaftMessageHandler exists to allows a store to be stopped and
// restarted while maintaining a partition using an unreliableRaftHandler.
type mtcStoreRaftMessageHandler struct {
	mtc      *multiTestContext
	storeIdx int
}

func (h *mtcStoreRaftMessageHandler) HandleRaftRequest(
	ctx context.Context,
	req *storage.RaftMessageRequest,
	respStream storage.RaftMessageResponseStream,
) *roachpb.Error {
	return h.mtc.Store(h.storeIdx).HandleRaftRequest(ctx, req, respStream)
}

func (h *mtcStoreRaftMessageHandler) HandleRaftResponse(
	ctx context.Context, resp *storage.RaftMessageResponse,
) error {
	return h.mtc.Store(h.storeIdx).HandleRaftResponse(ctx, resp)
}

func (h *mtcStoreRaftMessageHandler) HandleSnapshot(
	header *storage.SnapshotRequest_Header, respStream storage.SnapshotResponseStream,
) error {
	return h.mtc.Store(h.storeIdx).HandleSnapshot(header, respStream)
}

// mtcPartitionedRange is a convenient abstraction to create a range on a node
// in a multiTestContext which can be partitioned and unpartitioned.
type mtcPartitionedRange struct {
	rangeID         roachpb.RangeID
	partitionedNode int
	partitioned     atomic.Value
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
func setupPartitionedRange(
	mtc *multiTestContext, rangeID roachpb.RangeID, partitionedNode int, activated bool,
) (*mtcPartitionedRange, error) {
	partRange := &mtcPartitionedRange{
		rangeID:         rangeID,
		partitionedNode: partitionedNode,
	}
	partRange.partitioned.Store(activated)
	partRepl, err := mtc.stores[partitionedNode].GetReplica(rangeID)
	if err != nil {
		return nil, err
	}
	partReplDesc, err := partRepl.GetReplicaDescriptor()
	if err != nil {
		return nil, err
	}
	for i := range mtc.stores {
		s := i
		h := &unreliableRaftHandler{
			rangeID: rangeID,
			RaftMessageHandler: &mtcStoreRaftMessageHandler{
				mtc:      mtc,
				storeIdx: s,
			},
		}
		// Only filter messages from the partitioned store on the other
		// two stores.
		h.dropReq = func(req *storage.RaftMessageRequest) bool {
			return partRange.partitioned.Load().(bool) &&
				(s == partitionedNode || req.FromReplica.StoreID == partRepl.StoreID())
		}
		h.dropHB = func(hb *storage.RaftHeartbeat) bool {
			return partRange.partitioned.Load().(bool) &&
				(s == partitionedNode || hb.FromReplicaID == partReplDesc.ReplicaID)
		}
		mtc.transport.Listen(mtc.stores[s].Ident.StoreID, h)
	}
	return partRange, nil
}

func (pr *mtcPartitionedRange) activate() {
	pr.partitioned.Store(true)
}

func (pr *mtcPartitionedRange) deactivate() {
	pr.partitioned.Store(false)
}
