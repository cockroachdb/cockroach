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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangestats"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const MixedVersionErr = "span stats request - unable to service a mixed version request"
const UnexpectedLegacyRequest = "span stats request - unexpected populated legacy fields (StartKey, EndKey)"
const nodeErrorMsgPlaceholder = "could not get span stats sample for node %d: %v"
const exceedSpanLimitPlaceholder = "error getting span statistics - number of spans in request payload (%d) exceeds 'server.span_stats.span_batch_limit' cluster setting limit (%d)"

func (s *systemStatusServer) spanStatsFanOut(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	res := &roachpb.SpanStatsResponse{
		SpanToStats: make(map[string]*roachpb.SpanStats),
	}
	// Response level error
	var respErr error

	spansPerNode, err := s.getSpansPerNode(ctx, req)
	if err != nil {
		return nil, err
	}

	// We should only fan out to a node if it has replicas of any span.
	// A blind fan out would be wasteful.
	smartDial := func(
		ctx context.Context,
		nodeID roachpb.NodeID,
	) (interface{}, error) {
		if _, ok := spansPerNode[nodeID]; ok {
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

		resp, err := client.(serverpb.StatusClient).SpanStats(ctx,
			&roachpb.SpanStatsRequest{
				NodeID: nodeID.String(),
				Spans:  spansPerNode[nodeID],
			})
		return resp, err
	}

	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		// Noop if nil response (returned from skipped node).
		if resp == nil {
			return
		}

		nodeResponse := resp.(*roachpb.SpanStatsResponse)

		for spanStr, spanStats := range nodeResponse.SpanToStats {
			_, exists := res.SpanToStats[spanStr]
			if !exists {
				res.SpanToStats[spanStr] = spanStats
			} else {
				res.SpanToStats[spanStr].Add(spanStats)
			}
		}
	}

	errorFn := func(nodeID roachpb.NodeID, err error) {
		log.Errorf(ctx, nodeErrorMsgPlaceholder, nodeID, err)
		respErr = err
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

	return res, respErr
}

func (s *systemStatusServer) getLocalStats(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	var res = &roachpb.SpanStatsResponse{SpanToStats: make(map[string]*roachpb.SpanStats)}
	for _, span := range req.Spans {
		spanStats, err := s.statsForSpan(ctx, span)
		if err != nil {
			return nil, err
		}
		res.SpanToStats[span.String()] = spanStats
	}
	return res, nil
}

func (s *systemStatusServer) statsForSpan(
	ctx context.Context, span roachpb.Span,
) (*roachpb.SpanStats, error) {

	var descriptors []roachpb.RangeDescriptor
	scanner := rangedesc.NewScanner(s.db)
	pageSize := int(roachpb.RangeDescPageSize.Get(&s.st.SV))
	err := scanner.Scan(ctx, pageSize, func() {
		// If the underlying txn fails and needs to be retried,
		// clear the descriptors we've collected so far.
		descriptors = nil
	}, span, func(scanned ...roachpb.RangeDescriptor) error {
		descriptors = append(descriptors, scanned...)
		return nil
	})

	if err != nil {
		return nil, err
	}

	spanStats := &roachpb.SpanStats{}
	var fullyContainedKeysBatch []roachpb.Key
	rSpan, err := keys.SpanAddr(span)
	if err != nil {
		return nil, err
	}
	// Iterate through the span's ranges.
	for _, desc := range descriptors {

		// Get the descriptor for the current range of the span.
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
					return err
				}

				spanStats.TotalStats.Add(stats)
				return nil
			})

			if err != nil {
				return nil, err
			}
		}
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
		approxDiskBytes, remoteBytes, externalBytes, err := store.TODOEngine().ApproximateDiskBytes(rSpan.Key.AsRawKey(), rSpan.EndKey.AsRawKey())
		if err != nil {
			return err
		}
		spanStats.ApproximateDiskBytes += approxDiskBytes
		spanStats.RemoteFileBytes += remoteBytes
		spanStats.ExternalFileBytes += externalBytes
		return nil
	})

	if err != nil {
		return nil, err
	}
	return spanStats, nil
}

// getSpanStatsInternal will return span stats according to the nodeID specified
// by req. If req.NodeID == 0, a fan out is done to collect and sum span stats
// from across the cluster. Otherwise, the node specified will be dialed,
// if the local node isn't already the node specified. If the node specified
// is the local node, span stats are computed and returned.
func (s *systemStatusServer) getSpanStatsInternal(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
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

func (s *systemStatusServer) getSpansPerNode(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (map[roachpb.NodeID][]roachpb.Span, error) {
	// Mapping of node ids to spans with a replica on the node.
	spansPerNode := make(map[roachpb.NodeID][]roachpb.Span)
	pageSize := int(roachpb.RangeDescPageSize.Get(&s.st.SV))
	for _, span := range req.Spans {
		nodeIDs, err := nodeIDsForSpan(ctx, s.db, span, pageSize)
		if err != nil {
			return nil, err
		}
		// Add the span to the map for each of the node IDs it belongs to.
		for _, nodeID := range nodeIDs {
			spansPerNode[nodeID] = append(spansPerNode[nodeID], span)
		}
	}
	return spansPerNode, nil
}

// nodeIDsForSpan returns a list of nodeIDs that contain at least one replica
// for the argument span. Descriptors are found via ScanMetaKVs.
func nodeIDsForSpan(
	ctx context.Context, db *kv.DB, span roachpb.Span, pageSize int,
) ([]roachpb.NodeID, error) {
	nodeIDs := make(map[roachpb.NodeID]struct{})
	scanner := rangedesc.NewScanner(db)
	err := scanner.Scan(ctx, pageSize, func() {
		// If the underlying txn fails and needs to be retried,
		// clear the nodeIDs we've collected so far.
		nodeIDs = map[roachpb.NodeID]struct{}{}
	}, span, func(scanned ...roachpb.RangeDescriptor) error {
		for _, desc := range scanned {
			for _, repl := range desc.Replicas().Descriptors() {
				nodeIDs[repl.NodeID] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	nodeIDList := make([]roachpb.NodeID, 0, len(nodeIDs))
	for id := range nodeIDs {
		nodeIDList = append(nodeIDList, id)
	}

	return nodeIDList, nil
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
	return req.StartKey != nil || req.EndKey != nil
}
