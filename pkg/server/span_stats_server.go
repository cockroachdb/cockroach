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
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func (s *systemStatusServer) spanStatsFanOut(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	defer timeutil.TimeTrack(time.Now(), "systemStatusServer - spanStatsFanOut")
	res := &roachpb.SpanStatsResponse{
		SpanToStats: make(map[string]*roachpb.SpanStats),
	}

	nodeSpans, err := buildNodeSpanMapping(ctx, req, s.distSender)
	if err != nil {
		return nil, err
	}

	// We should only fan out to a node if it has replicas of any span.
	// A blind fan out would be wasteful.
	smartDial := func(
		ctx context.Context,
		nodeID roachpb.NodeID,
	) (interface{}, error) {
		if _, ok := nodeSpans[nodeID]; ok {
			return s.dialNode(ctx, nodeID)
		}
		return nil, nil
	}

	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		// `smartDial` may skip this node, so check to see if the client is nil.
		if client == nil {
			return &roachpb.SpanStatsResponse{}, nil
		}
		stats, err := client.(serverpb.StatusClient).SpanStats(ctx,
			&roachpb.SpanStatsRequest{
				NodeID: nodeID.String(),
				Spans:  nodeSpans[nodeID],
			})
		if err != nil {
			fmt.Printf("ENCOUNTERED AN ERROR WITH NODE ID: %s. ERROR: %s\n", nodeID.String(), err.Error())
			return nil, err
		}
		return stats, err
	}

	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		fmt.Printf("response fn node id: %s\n", nodeID.String())
		nodeResponse := resp.(*roachpb.SpanStatsResponse)
		for spanStr, spanStats := range nodeResponse.SpanToStats {
			_, exists := res.SpanToStats[spanStr]
			if !exists {
				spStats := spanStats
				res.SpanToStats[spanStr] = spStats

			} else {
				res.SpanToStats[spanStr].ApproximateDiskBytes += spanStats.ApproximateDiskBytes
				res.SpanToStats[spanStr].TotalStats.Add(spanStats.TotalStats)
				res.SpanToStats[spanStr].RangeCount += spanStats.RangeCount
			}
			fmt.Printf("response fn, iterating through node response. Span: %s. Stats: %s\n", spanStr, res.SpanToStats[spanStr])
		}
	}

	// Missed span on nodes: 6, 2, 7
	// ERROR occurred on nodes: 6, 2, 7

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

func (s *systemStatusServer) getLocalStats(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	defer timeutil.TimeTrack(time.Now(), "systemStatusServer - getLocalStats")
	// Arbitrary batch size limit for fetching range stats.
	const rangeStatsBatchLimit = 100
	res := &roachpb.SpanStatsResponse{
		SpanToStats: make(map[string]*roachpb.SpanStats),
	}
	ri := kvcoord.MakeRangeIterator(s.distSender)

	// For each span
	for _, span := range req.Spans {
		spanStart := time.Now()

		rSpan, err := ToRSpan(span)
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
				fmt.Println("error !ri.valid()")
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
				// If we've reached the batch limit, request range stats for the current batch.
				if len(fullyContainedKeysBatch) >= rangeStatsBatchLimit {
					// Obtain stats for fully contained ranges via RangeStats.
					rangeStats, err := s.rangeStatsFetcher.RangeStats(ctx,
						fullyContainedKeysBatch...)
					if err != nil {
						fmt.Println("error fetching range stats", err)
						return nil, err
					}
					for _, resp := range rangeStats {
						spanStats.TotalStats.Add(resp.MVCCStats)
					}
					// Reset the batch.
					fullyContainedKeysBatch = fullyContainedKeysBatch[:0]
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
				err := s.stores.VisitStores(func(s *kvserver.Store) error {
					stats, err := storage.ComputeStats(
						s.TODOEngine(),
						scanStart.AsRawKey(),
						scanEnd.AsRawKey(),
						timeutil.Now().UnixNano(),
					)

					if err != nil {
						fmt.Println("error MVCC scan", err)
						return err
					}

					spanStats.TotalStats.Add(stats)
					return nil
				})

				if err != nil {
					fmt.Println("error visiting stores", err)
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
			rangeStats, err := s.rangeStatsFetcher.RangeStats(ctx,
				fullyContainedKeysBatch...)
			if err != nil {
				fmt.Println("error fetching range stats (last)", err)
				return nil, err
			}
			for _, resp := range rangeStats {
				spanStats.TotalStats.Add(resp.MVCCStats)
			}
			// Nil the batch.
			fullyContainedKeysBatch = nil
		}
		// Finally, get the approximate disk bytes from each store.
		err = s.stores.VisitStores(func(store *kvserver.Store) error {
			approxDiskBytes, err := store.TODOEngine().ApproximateDiskBytes(rSpan.Key.AsRawKey(), rSpan.EndKey.AsRawKey())
			if err != nil {
				fmt.Println("error getting disk bytes", err, rSpan, rSpan.Key.AsRawKey(), rSpan.EndKey.AsRawKey())
				return err
			}
			spanStats.ApproximateDiskBytes += approxDiskBytes
			return nil
		})

		if err != nil {
			fmt.Println("error visiting stores (getting disk bytes)", err)
			return nil, err
		}
		res.SpanToStats[span.String()] = spanStats
		fmt.Printf("Time for span %s: %s. Node ID: %s\n",
			span.String(), time.Since(spanStart), req.NodeID)
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

func ToRSpan(s roachpb.Span) (roachpb.RSpan, error) {
	rKey, err := keys.Addr(s.Key)
	if err != nil {
		return roachpb.RSpan{}, err
	}
	return roachpb.RSpan{
		Key:    rKey,
		EndKey: rKey.PrefixEnd(),
	}, nil
}

func buildNodeSpanMapping(
	ctx context.Context, req *roachpb.SpanStatsRequest, ds *kvcoord.DistSender,
) (map[roachpb.NodeID][]roachpb.Span, error) {
	// Mapping of node ids to spans who have a range replica on the node.
	nodeSpans := make(map[roachpb.NodeID][]roachpb.Span)

	for _, span := range req.Spans {
		rSpan, err := ToRSpan(span)
		if err != nil {
			return nil, err
		}
		spanNodeIDs, _, err := nodeIDsAndRangeCountForSpan(ctx, ds, rSpan)
		if err != nil {
			return nil, err
		}
		for _, nodeID := range spanNodeIDs {
			nodeSpans[nodeID] = append(nodeSpans[nodeID], span)
		}
	}
	return nodeSpans, nil
}
