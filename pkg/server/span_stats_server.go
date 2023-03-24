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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangestats"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const UnexpectedLegacyRequest = "span stats request - unexpected populated legacy fields (StartKey, EndKey)"
const nodeErrorMsgPlaceholder = "could not get span stats sample for node %d: %v"
const exceedSpanLimitPlaceholder = "error getting span statistics - number of spans in request payload (%d) exceeds 'server.span_stats.span_batch_limit' cluster setting limit (%d)"

func (s *systemStatusServer) spanStatsFanOut(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	// Unexpected legacy request from 23.1 gateway node.
	if req.StartKey != nil || req.EndKey != nil {
		return nil, errors.New(UnexpectedLegacyRequest)
	}

	res := &roachpb.SpanStatsResponse{
		SpanToStats: make(map[string]*roachpb.SpanStats),
	}

	nodeHelperData, err := s.buildNodeSpanAndServerMapping(ctx, req, s.distSender)
	if err != nil {
		return nil, err
	}

	// We should only fan out to a node if it has replicas of any span.
	// A blind fan out would be wasteful.
	smartDial := func(
		ctx context.Context,
		nodeID roachpb.NodeID,
	) (interface{}, error) {
		if _, ok := nodeHelperData[nodeID]; ok {
			return s.dialNode(ctx, nodeID)
		}
		return nil, nil
	}

	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		// `smartDial` may skip this node, so check to see if the client is nil.
		// If it is, return nil response.
		if client == nil {
			return nil, nil
		}

		// We're on 23.1.
		if nodeHelperData[nodeID].version.IsActive(clusterversion.V23_1) {
			return currNodeFn(ctx, client, nodeID, nodeHelperData[nodeID].spans)
		}
		// We're on a version less than 23.1
		return legacyNodeFn(ctx, client, nodeID, nodeHelperData[nodeID].spans)
	}

	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		// Noop if nil response (returned from skipped node).
		if resp == nil {
			return
		}
		// We're on 23.1
		if nodeHelperData[nodeID].version.IsActive(clusterversion.V23_1) {
			currRespFn(res, resp)
			// We're on a version less than 23.1
		} else {
			legacyResponseFn(res, resp)
		}
	}

	errorFn := func(nodeID roachpb.NodeID, err error) {
		log.Errorf(ctx, nodeErrorMsgPlaceholder, nodeID, err)
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

func (s *systemStatusServer) getLocalStats(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	var res = &roachpb.SpanStatsResponse{}
	ri := kvcoord.MakeRangeIterator(s.distSender)

	//  Received legacy request as a result of a fanout from a 22.2 gateway node.
	if isLegacyRequest(req) {
		req.Spans = []roachpb.Span{
			{
				Key:    req.StartKey.AsRawKey(),
				EndKey: req.EndKey.AsRawKey(),
			},
		}
	} else {
		res.SpanToStats = make(map[string]*roachpb.SpanStats)
	}

	// For each span
	for _, span := range req.Spans {
		rSpan, err := keys.SpanAddr(span)
		if err != nil {
			return nil, err
		}
		// Seek to the span's start key.
		ri.Seek(ctx, rSpan.Key, kvcoord.Ascending)
		spanStats := &roachpb.SpanStats{}
		// Iterate through the span's ranges.
		var fullyContainedKeysBatch []roachpb.Key
		for {
			if !ri.Valid() {
				return nil, ri.Error()
			}

			// Get the descriptor for the current range of the span.
			desc := ri.Desc()
			descSpan := desc.RSpan()
			spanStats.RangeCount += 1

			// Is the descriptor fully contained by the request span?
			if rSpan.ContainsKeyRange(descSpan.Key, desc.EndKey) {
				// Collect into fullyContainedKeys batch.
				fullyContainedKeysBatch = append(fullyContainedKeysBatch, desc.StartKey.AsRawKey())
				// If we've exceeded the batch limit, request range stats for the current batch.
				if len(fullyContainedKeysBatch) > int(roachpb.RangeStatsBatchLimit.Get(&s.st.SV)) {
					// Obtain stats for fully contained ranges via RangeStats.
					fullyContainedKeysBatch, err = flushBatchedContainedKeys(ctx, s.rangeStatsFetcher, fullyContainedKeysBatch, spanStats)
					if err != nil {
						return nil, err
					}
				}
			} else {
				// Otherwise, do an MVCC Scan.
				// We should only scan the part of the range that our request span
				// encompasses.
				scanStart := rSpan.Key
				scanEnd := rSpan.EndKey
				// If our request span began before the start of this range,
				// start scanning from this range's start key.
				if descSpan.Key.Compare(rSpan.Key) == 1 {
					scanStart = descSpan.Key
				}
				// If our request span ends after the end of this range,
				// stop scanning at this range's end key.
				if descSpan.EndKey.Compare(rSpan.EndKey) == -1 {
					scanEnd = descSpan.EndKey
				}
				err = s.stores.VisitStores(func(s *kvserver.Store) error {
					stats, err := storage.ComputeStats(
						s.TODOEngine(),
						scanStart.AsRawKey(),
						scanEnd.AsRawKey(),
						timeutil.Now().UnixNano(),
					)

					if err != nil {
						log.Errorf(ctx, "span stats - error MVCC scan: %v", err)
						return err
					}

					spanStats.TotalStats.Add(stats)
					return nil
				})

				if err != nil {
					return nil, err
				}
			}

			if !ri.NeedAnother(rSpan) {
				break
			}
			ri.Next(ctx)
		}
		// If we still have some remaining ranges, request range stats for the current batch.
		if len(fullyContainedKeysBatch) > 0 {
			// Obtain stats for fully contained ranges via RangeStats.
			_, err = flushBatchedContainedKeys(ctx, s.rangeStatsFetcher, fullyContainedKeysBatch, spanStats)
			if err != nil {
				return nil, err
			}
			// Nil the batch.
			fullyContainedKeysBatch = nil
		}
		// Finally, get the approximate disk bytes from each store.
		err = s.stores.VisitStores(func(store *kvserver.Store) error {
			approxDiskBytes, err := store.TODOEngine().ApproximateDiskBytes(rSpan.Key.AsRawKey(), rSpan.EndKey.AsRawKey())
			if err != nil {
				log.Errorf(ctx, "error getting disk bytes: %v", err)
				return err
			}
			spanStats.ApproximateDiskBytes += approxDiskBytes
			return nil
		})

		if err != nil {
			return nil, err
		}

		if isLegacyRequest(req) {
			res.TotalStats = spanStats.TotalStats
			res.RangeCount = spanStats.RangeCount
			res.ApproximateDiskBytes = spanStats.ApproximateDiskBytes
		} else {
			res.SpanToStats[span.String()] = spanStats
		}
	}
	return res, nil
}

// getSpanStatsInternal will return span stats according to the nodeID specified
// by req. If req.NodeID == 0, a fan out is done to collect and sum span stats
// from across the cluster. Otherwise, the node specified will be dialed,
// if the local node isn't already the node specified. If the node specified
// is the local node, span stats are computed and returned.
func (s *systemStatusServer) getSpanStatsInternal(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	if len(req.Spans) > int(roachpb.SpanStatsBatchLimit.Get(&s.st.SV)) {
		return nil, errors.Newf(exceedSpanLimitPlaceholder, len(req.Spans), int(roachpb.SpanStatsBatchLimit.Get(&s.st.SV)))
	}

	// Perform a fan out when the requested NodeID is 0.
	if req.NodeID == "0" {
		return s.spanStatsFanOut(ctx, req)
	}

	// See if the requested node is the local node.
	_, local, err := s.statusServer.parseNodeID(req.NodeID)
	if err != nil {
		return nil, err
	}

	// If the requested node is the local node, return stats.
	if local {
		return s.getLocalStats(ctx, req)
	}

	// Otherwise, dial the correct node, and ask for span stats.
	nodeID, err := strconv.ParseInt(req.NodeID, 10, 32)
	if err != nil {
		return nil, err
	}

	client, err := s.dialNode(ctx, roachpb.NodeID(nodeID))
	if err != nil {
		return nil, err
	}
	return client.SpanStats(ctx, req)
}

type nodeServerVersionAndSpans struct {
	spans   []roachpb.Span
	version clusterversion.ClusterVersion
}

// buildNodeSpanAndServerMapping builds a mapping of:
//
//	node ID -> spans belonging to the node, node's server version
func (s *systemStatusServer) buildNodeSpanAndServerMapping(
	ctx context.Context, req *roachpb.SpanStatsRequest, ds *kvcoord.DistSender,
) (map[roachpb.NodeID]*nodeServerVersionAndSpans, error) {
	// Mapping of node ids to spans with a replica on the node.
	nodeHelperData := make(map[roachpb.NodeID]*nodeServerVersionAndSpans)

	// Iterate over the request spans.
	for _, span := range req.Spans {
		rSpan, err := keys.SpanAddr(span)
		if err != nil {
			return nil, err
		}
		// Get the node ids belonging to the span.
		nodeIDs, _, err := nodeIDsAndRangeCountForSpan(ctx, ds, rSpan)
		if err != nil {
			return nil, err
		}
		// Add the span to the map for each of the node IDs it belongs to.
		for _, nodeID := range nodeIDs {
			// Initialize struct pointer if it does not exist
			if _, exists := nodeHelperData[nodeID]; !exists {
				nodeHelperData[nodeID] = &nodeServerVersionAndSpans{}
			}
			nodeHelperData[nodeID].spans = append(nodeHelperData[nodeID].spans, span)
		}
	}

	for nodeID := range nodeHelperData {
		// KV call to get node server version.
		status, err := s.nodeStatus(ctx, &serverpb.NodeRequest{
			NodeId: nodeID.String(),
		})
		if err != nil {
			return nil, err
		}
		// We can access the version field here safely. Struct pointers will have
		// already been initialized when collecting node IDs.
		nodeHelperData[nodeID].version = clusterversion.ClusterVersion{
			Version: status.Desc.ServerVersion,
		}
	}
	return nodeHelperData, nil
}

func flushBatchedContainedKeys(
	ctx context.Context,
	fetcher *rangestats.Fetcher,
	fullyContainedKeysBatch []roachpb.Key,
	spanStats *roachpb.SpanStats,
) ([]roachpb.Key, error) {
	// Obtain stats for fully contained ranges via RangeStats.
	rangeStats, err := fetcher.RangeStats(ctx,
		fullyContainedKeysBatch...)
	if err != nil {
		return nil, err
	}
	for _, resp := range rangeStats {
		spanStats.TotalStats.Add(resp.MVCCStats)
	}
	// Reset the keys batch
	return fullyContainedKeysBatch[:0], nil
}

func isLegacyRequest(req *roachpb.SpanStatsRequest) bool {
	// If the start/end key fields are not nil, we have a request using the old request format.
	return req.StartKey != nil && req.EndKey != nil
}

type legacyNodeFnResponse = map[string]*roachpb.SpanStatsResponse

func legacyNodeFn(
	ctx context.Context, client interface{}, nodeID roachpb.NodeID, spans []roachpb.Span,
) (legacyNodeFnResponse, error) {
	// We're on a version less than 23.1, loop through each span. Call span stats for each span individually.
	resps := make(map[string]*roachpb.SpanStatsResponse, len(spans))
	// Send a SpanStats request for each span, requests are localized
	// to the old-version node.
	for _, span := range spans {
		rSpan, err := keys.SpanAddr(span)
		if err != nil {
			return nil, err
		}
		stats, err := client.(serverpb.StatusClient).SpanStats(ctx,
			&roachpb.SpanStatsRequest{
				NodeID:   nodeID.String(),
				StartKey: rSpan.Key,
				EndKey:   rSpan.EndKey,
			})
		if err != nil {
			return nil, err
		}
		spanStr := span.String()
		resps[spanStr] = stats
	}
	return resps, nil
}

func legacyResponseFn(res *roachpb.SpanStatsResponse, resp interface{}) {
	// We're on a version less than 23.1, expect the node response type to be legacyNodeFnResponse.
	nodeResponses := resp.(legacyNodeFnResponse)
	for spanStr, nodeResp := range nodeResponses {
		_, exists := res.SpanToStats[spanStr]
		if !exists {
			res.SpanToStats[spanStr] = &roachpb.SpanStats{
				RangeCount:           nodeResp.RangeCount,
				TotalStats:           nodeResp.TotalStats,
				ApproximateDiskBytes: nodeResp.ApproximateDiskBytes,
			}
		} else {
			res.SpanToStats[spanStr].ApproximateDiskBytes += nodeResp.ApproximateDiskBytes
			res.SpanToStats[spanStr].TotalStats.Add(nodeResp.TotalStats)
			res.SpanToStats[spanStr].RangeCount += nodeResp.RangeCount
		}
	}
}

func currNodeFn(
	ctx context.Context, client interface{}, nodeID roachpb.NodeID, spans []roachpb.Span,
) (*roachpb.SpanStatsResponse, error) {
	return client.(serverpb.StatusClient).SpanStats(ctx,
		&roachpb.SpanStatsRequest{
			NodeID: nodeID.String(),
			Spans:  spans,
		})
}

func currRespFn(res *roachpb.SpanStatsResponse, resp interface{}) {
	// We're on 23.1, expect the node response type to be *roachpb.SpanStatsResponse.
	nodeResponse := resp.(*roachpb.SpanStatsResponse)
	for spanStr, spanStats := range nodeResponse.SpanToStats {
		_, exists := res.SpanToStats[spanStr]
		if !exists {
			res.SpanToStats[spanStr] = spanStats
		} else {
			res.SpanToStats[spanStr].ApproximateDiskBytes += spanStats.ApproximateDiskBytes
			res.SpanToStats[spanStr].TotalStats.Add(spanStats.TotalStats)
			res.SpanToStats[spanStr].RangeCount += spanStats.RangeCount
		}
	}
}
