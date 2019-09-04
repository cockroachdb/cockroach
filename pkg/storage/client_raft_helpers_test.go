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
