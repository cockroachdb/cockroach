// Copyright 2023 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangestats"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// spanStatsServer implements serverpb.SpanStatsServer.
// It services requests for serverpb.InternalSpanStatsRequest.
type spanStatsServer struct {
	fetcher      *rangestats.Fetcher
	distSender   *kvcoord.DistSender
	statusServer *systemStatusServer
	nodeDialer   *nodedialer.Dialer
	node         *Node
}

var _ serverpb.SpanStatsServer = &spanStatsServer{}

func (s *spanStatsServer) dialFn(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
	conn, err := s.nodeDialer.Dial(ctx, nodeID, rpc.DefaultClass)
	return serverpb.NewSpanStatsClient(conn), err
}

func (s *spanStatsServer) fanOut(
	ctx context.Context, req *serverpb.InternalSpanStatsRequest,
) (*serverpb.InternalSpanStatsResponse, error) {
	var res *serverpb.InternalSpanStatsResponse

	rSpan, err := keys.SpanAddr(req.Span)
	if err != nil {
		return nil, err
	}
	nodeIDs, _, err := nodeIDsAndRangeCountForSpan(ctx, s.distSender, rSpan)
	if err != nil {
		return nil, err
	}
	nodesWithReplica := make(map[roachpb.NodeID]bool)
	for _, nodeID := range nodeIDs {
		nodesWithReplica[nodeID] = true
	}

	// We should only fan out to a node if it has replicas for this span.
	// A blind fan out would be wasteful.
	smartDial := func(
		ctx context.Context,
		nodeID roachpb.NodeID,
	) (interface{}, error) {
		if _, ok := nodesWithReplica[nodeID]; ok {
			return s.dialFn(ctx, nodeID)
		}
		return nil, nil
	}

	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		// `smartDial` may skip this node, so check to see if the client is nil.
		if client == nil {
			return &serverpb.InternalSpanStatsResponse{}, nil
		}
		stats, err := client.(serverpb.SpanStatsClient).GetSpanStats(ctx,
			&serverpb.InternalSpanStatsRequest{
				Span:   req.Span,
				NodeID: nodeID,
			})
		if err != nil {
			return nil, err
		}
		return stats, err
	}

	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		nodeResponse := resp.(*serverpb.InternalSpanStatsResponse)
		res.ApproximateDiskBytes += nodeResponse.ApproximateDiskBytes
		res.TotalStats.Add(nodeResponse.TotalStats)
		res.RangeCount += nodeResponse.RangeCount
	}

	errorFn := func(nodeID roachpb.NodeID, err error) {
		log.Errorf(ctx, "could not get span stats sample for node %d: %v", nodeID, err)
	}

	if err := s.statusServer.iterateNodes(
		ctx,
		"iterating nodes for span stats",
		smartDial,
		nodeFn,
		responseFn,
		errorFn,
	); err != nil {
		return nil, err
	}

	return res, nil
}

func (s *spanStatsServer) getLocalStats(
	ctx context.Context, req *serverpb.InternalSpanStatsRequest,
) (*serverpb.InternalSpanStatsResponse, error) {
	res := &serverpb.InternalSpanStatsResponse{}

	sp, err := keys.SpanAddr(req.Span)
	if err != nil {
		return nil, err
	}
	ri := kvcoord.MakeRangeIterator(s.distSender)
	ri.Seek(ctx, sp.Key, kvcoord.Ascending)

	for {
		if !ri.Valid() {
			return nil, ri.Error()
		}

		desc := ri.Desc()
		descSpan := desc.RSpan()
		res.RangeCount += 1

		// Is the descriptor fully contained by the request span?
		if sp.ContainsKeyRange(descSpan.Key, desc.EndKey) {
			// If so, obtain stats for this range via RangeStats.
			rangeStats, err := s.fetcher.RangeStats(ctx,
				desc.StartKey.AsRawKey())
			if err != nil {
				return nil, err
			}
			for _, resp := range rangeStats {
				res.TotalStats.Add(resp.MVCCStats)
			}

		} else {
			// Otherwise, do an MVCC Scan.
			// We should only scan the part of the range that our request span
			// encompasses.
			scanStart := sp.Key
			scanEnd := sp.EndKey
			// If our request span began before the start of this range,
			// start scanning from this range's start key.
			if descSpan.Key.Compare(sp.Key) == 1 {
				scanStart = descSpan.Key
			}
			// If our request span ends after the end of this range,
			// stop scanning at this range's end key.
			if descSpan.EndKey.Compare(sp.EndKey) == -1 {
				scanEnd = descSpan.EndKey
			}
			err := s.node.stores.VisitStores(func(s *kvserver.Store) error {
				stats, err := storage.ComputeStats(
					s.Engine(),
					scanStart.AsRawKey(),
					scanEnd.AsRawKey(),
					timeutil.Now().UnixNano(),
				)

				if err != nil {
					return err
				}

				res.TotalStats.Add(stats)
				return nil
			})

			if err != nil {
				return nil, err
			}
		}

		if !ri.NeedAnother(sp) {
			break
		}
		ri.Next(ctx)
	}

	// Finally, get the approximate disk bytes from each store.
	err = s.node.stores.VisitStores(func(store *kvserver.Store) error {
		approxDiskBytes, err := store.Engine().ApproximateDiskBytes(
			req.Span.Key, req.Span.EndKey)
		if err != nil {
			return err
		}
		res.ApproximateDiskBytes += approxDiskBytes
		return nil
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

// GetSpanStats will return span stats according to the nodeID specified by req.
// If req.NodeID == 0, a fan out is done to collect and sum span stats from
// across the cluster. Otherwise, the node specified will be dialed,
// if the local node isn't already the node specified. If the node specified
// is the local node, span stats are computed and returned.
func (s *spanStatsServer) GetSpanStats(
	ctx context.Context, req *serverpb.InternalSpanStatsRequest,
) (*serverpb.InternalSpanStatsResponse, error) {
	// Perform a fan out when the requested NodeID is 0.
	if req.NodeID == 0 {
		return s.fanOut(ctx, req)
	}

	// See if the requested node is the local node.
	_, local, err := s.statusServer.parseNodeID(req.NodeID.String())
	if err != nil {
		return nil, err
	}

	// If the requested node is the local node, return stats.
	if local {
		return s.getLocalStats(ctx, req)
	}

	// Otherwise, dial the correct node, and ask for span stats.
	client, err := s.dialFn(ctx, req.NodeID)
	if err != nil {
		return nil, err
	}
	return client.(serverpb.SpanStatsClient).GetSpanStats(ctx, req)
}
