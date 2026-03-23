// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtestutils

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// UnreliableRaftHandlerFuncs contains functions that can be used to control
// which messages are dropped by an UnreliableRaftHandler.
type UnreliableRaftHandlerFuncs struct {
	// If non-nil, can return false to avoid dropping the msg to
	// UnreliableRaftHandler.RangeID. If nil, all messages pertaining to the
	// respective range are dropped.
	DropReq  func(*kvserverpb.RaftMessageRequest) bool
	DropHB   func(*kvserverpb.RaftHeartbeat) bool
	DropResp func(*kvserverpb.RaftMessageResponse) bool
	// SnapErr and DelegateErr default to returning nil.
	SnapErr     func(*kvserverpb.SnapshotRequest_Header) error
	DelegateErr func(request *kvserverpb.DelegateSendSnapshotRequest) error
}

// NoopRaftHandlerFuncs returns UnreliableRaftHandlerFuncs that don't drop any
// messages.
func NoopRaftHandlerFuncs() UnreliableRaftHandlerFuncs {
	return UnreliableRaftHandlerFuncs{
		DropResp: func(*kvserverpb.RaftMessageResponse) bool {
			return false
		},
		DropReq: func(*kvserverpb.RaftMessageRequest) bool {
			return false
		},
		DropHB: func(*kvserverpb.RaftHeartbeat) bool {
			return false
		},
	}
}

// UnreliableRaftHandler drops all Raft messages that are addressed to the
// specified rangeID, but lets all other messages through.
type UnreliableRaftHandler struct {
	Name    string
	RangeID roachpb.RangeID
	kvserver.IncomingRaftMessageHandler
	UnreliableRaftHandlerFuncs
}

// HandleRaftRequest implements the kvserver.IncomingRaftMessageHandler interface.
func (h *UnreliableRaftHandler) HandleRaftRequest(
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
	} else if req.RangeID == h.RangeID {
		if h.DropReq == nil || h.DropReq(req) {
			var prefix string
			if h.Name != "" {
				prefix = fmt.Sprintf("[%s] ", h.Name)
			}
			log.KvExec.Infof(
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
		if !h.DropReq(req) && log.V(1) {
			// Debug logging, even if requests aren't dropped. This is a
			// convenient way to observe all raft messages in unit tests when
			// run using --vmodule='client_raft_helpers_test=1'.
			var prefix string
			if h.Name != "" {
				prefix = fmt.Sprintf("[%s] ", h.Name)
			}
			log.KvExec.Infof(
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

func (h *UnreliableRaftHandler) filterHeartbeats(
	hbs []kvserverpb.RaftHeartbeat,
) []kvserverpb.RaftHeartbeat {
	if len(hbs) == 0 {
		return hbs
	}
	var cpy []kvserverpb.RaftHeartbeat
	for i := range hbs {
		hb := &hbs[i]
		if hb.RangeID != h.RangeID || (h.DropHB != nil && !h.DropHB(hb)) {
			cpy = append(cpy, *hb)
		}
	}
	return cpy
}

// HandleRaftResponse implements the kvserver.IncomingRaftMessageHandler interface.
func (h *UnreliableRaftHandler) HandleRaftResponse(
	ctx context.Context, resp *kvserverpb.RaftMessageResponse,
) error {
	if resp.RangeID == h.RangeID {
		if h.DropResp == nil || h.DropResp(resp) {
			return nil
		}
	}
	return h.IncomingRaftMessageHandler.HandleRaftResponse(ctx, resp)
}

// HandleSnapshot implements the kvserver.IncomingRaftMessageHandler interface.
func (h *UnreliableRaftHandler) HandleSnapshot(
	ctx context.Context,
	header *kvserverpb.SnapshotRequest_Header,
	respStream kvserver.SnapshotResponseStream,
) error {
	if header.RaftMessageRequest.RangeID == h.RangeID && h.SnapErr != nil {
		if err := h.SnapErr(header); err != nil {
			return err
		}
	}
	return h.IncomingRaftMessageHandler.HandleSnapshot(ctx, header, respStream)
}

// HandleDelegatedSnapshot implements the kvserver.IncomingRaftMessageHandler interface.
func (h *UnreliableRaftHandler) HandleDelegatedSnapshot(
	ctx context.Context, req *kvserverpb.DelegateSendSnapshotRequest,
) *kvserverpb.DelegateSnapshotResponse {
	if req.RangeID == h.RangeID && h.DelegateErr != nil {
		if err := h.DelegateErr(req); err != nil {
			return &kvserverpb.DelegateSnapshotResponse{
				Status:       kvserverpb.DelegateSnapshotResponse_ERROR,
				EncodedError: errors.EncodeError(context.Background(), err),
			}
		}
	}
	return h.IncomingRaftMessageHandler.HandleDelegatedSnapshot(ctx, req)
}
