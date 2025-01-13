// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangestats"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var SpanStatsNodeTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.span_stats.node.timeout",
	"the duration allowed for a single node to return span stats data before"+
		" the request is cancelled; if set to 0, there is no timeout",
	time.Minute,
	settings.NonNegativeDuration,
)

const defaultRangeStatsBatchLimit = 100

// RangeStatsBatchLimit registers the maximum number of ranges to be batched
// when fetching range stats for a span.
var RangeStatsBatchLimit = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.span_stats.range_batch_limit",
	"the maximum batch size when fetching ranges statistics for a span",
	defaultRangeStatsBatchLimit,
	settings.PositiveInt,
)

// RangeDescPageSize controls the page size when iterating through range
// descriptors.
var RangeDescPageSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.span_stats.range_desc_page_size",
	"the page size when iterating through range descriptors",
	100,
	settings.IntInRange(5, 25000),
)

const MixedVersionErr = "span stats request - unable to service a mixed version request"
const UnexpectedLegacyRequest = "span stats request - unexpected populated legacy fields (StartKey, EndKey)"
const nodeErrorMsgPlaceholder = "could not get span stats sample for node %d: %v"

func (s *systemStatusServer) spanStatsFanOut(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	res := &roachpb.SpanStatsResponse{
		SpanToStats: make(map[string]*roachpb.SpanStats),
	}
	// Populate SpanToStats with empty values for each span,
	// so that clients may still access stats for a specific span
	// in the extreme case of an error encountered on every node.
	for _, sp := range req.Spans {
		res.SpanToStats[sp.String()] = &roachpb.SpanStats{}
	}

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
		if s.knobs != nil {
			if s.knobs.IterateNodesDialCallback != nil {
				if err := s.knobs.IterateNodesDialCallback(nodeID); err != nil {
					return nil, err
				}
			}
		}

		if _, ok := spansPerNode[nodeID]; ok {
			return s.dialNode(ctx, nodeID)
		}
		return nil, nil
	}

	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		if s.knobs != nil {
			if s.knobs.IterateNodesNodeCallback != nil {
				if err := s.knobs.IterateNodesNodeCallback(ctx, nodeID); err != nil {
					return nil, err
				}
			}
		}

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

	errorFn := func(nodeID roachpb.NodeID, err error) {
		log.Errorf(ctx, nodeErrorMsgPlaceholder, nodeID, err)
		errorMessage := fmt.Sprintf("%v", err)
		res.Errors = append(res.Errors, errorMessage)
	}

	timeout := SpanStatsNodeTimeout.Get(&s.st.SV)
	if err := iterateNodes(
		ctx,
		s.serverIterator,
		s.stopper,
		"iterating nodes for span stats",
		timeout,
		smartDial,
		nodeFn,
		collectSpanStatsResponses(ctx, res),
		errorFn,
	); err != nil {
		return nil, err
	}

	return res, nil
}

// collectSpanStatsResponses takes a *roachpb.SpanStatsResponse and creates a closure around it
// to provide as a callback to fanned out SpanStats requests.
func collectSpanStatsResponses(
	ctx context.Context, res *roachpb.SpanStatsResponse,
) func(nodeID roachpb.NodeID, resp interface{}) {
	responses := make(map[string]struct{})
	return func(nodeID roachpb.NodeID, resp interface{}) {
		// Noop if nil response (returned from skipped node).
		if resp == nil {
			return
		}

		nodeResponse := resp.(*roachpb.SpanStatsResponse)

		// Values of ApproximateTotalStats, ApproximateDiskBytes,
		// RemoteFileBytes, and ExternalFileBytes should be physical values, but
		// TotalStats should be the logical, pre-replicated value.
		//
		// Note: This implementation can return arbitrarily stale values, because instead of getting
		// MVCC stats from the leaseholder, MVCC stats are taken from the node that responded first.
		// See #108779.
		for spanStr, spanStats := range nodeResponse.SpanToStats {
			if spanStats == nil {
				log.Errorf(ctx, "Span stats for %s from node response is nil", spanStr)
				continue
			}

			_, ok := res.SpanToStats[spanStr]
			if !ok {
				log.Warningf(ctx, "Received Span not in original request: %s", spanStr)
				res.SpanToStats[spanStr] = &roachpb.SpanStats{}
			}

			// Accumulate physical values across all replicas:
			res.SpanToStats[spanStr].ApproximateTotalStats.Add(spanStats.TotalStats)
			res.SpanToStats[spanStr].ApproximateDiskBytes += spanStats.ApproximateDiskBytes
			res.SpanToStats[spanStr].RemoteFileBytes += spanStats.RemoteFileBytes
			res.SpanToStats[spanStr].ExternalFileBytes += spanStats.ExternalFileBytes
			res.SpanToStats[spanStr].StoreIDs = util.CombineUnique(res.SpanToStats[spanStr].StoreIDs, spanStats.StoreIDs)

			// Logical values: take the values from the node that responded first.
			// TODO: This should really be read from the leaseholder.
			// https://github.com/cockroachdb/cockroach/issues/138792
			if _, ok := responses[spanStr]; !ok {
				res.SpanToStats[spanStr].TotalStats = spanStats.TotalStats
				res.SpanToStats[spanStr].RangeCount = spanStats.RangeCount
				res.SpanToStats[spanStr].ReplicaCount = spanStats.ReplicaCount
				responses[spanStr] = struct{}{}
			}
		}
	}
}

func (s *systemStatusServer) getLocalStats(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	var res = &roachpb.SpanStatsResponse{SpanToStats: make(map[string]*roachpb.SpanStats)}
	for _, span := range req.Spans {
		spanStats, err := s.statsForSpan(ctx, span, req.SkipMvccStats)
		if err != nil {
			return nil, err
		}
		res.SpanToStats[span.String()] = spanStats
	}
	return res, nil
}

func (s *systemStatusServer) statsForSpan(
	ctx context.Context, span roachpb.Span, skipMVCCStats bool,
) (*roachpb.SpanStats, error) {
	ctx, sp := tracing.ChildSpan(ctx, "systemStatusServer.statsForSpan")
	defer sp.Finish()

	spanStats := &roachpb.SpanStats{}
	rSpan, err := keys.SpanAddr(span)
	if err != nil {
		return nil, err
	}

	// First, get the approximate disk bytes from each store.
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

	if skipMVCCStats {
		return spanStats, nil
	}

	var descriptors []roachpb.RangeDescriptor
	scanner := rangedesc.NewScanner(s.db)
	pageSize := int(RangeDescPageSize.Get(&s.st.SV))
	err = scanner.Scan(ctx, pageSize, func() {
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

	storeIDs := make(map[roachpb.StoreID]struct{})
	var fullyContainedKeysBatch []roachpb.Key
	// Iterate through the span's ranges.
	for _, desc := range descriptors {

		// Get the descriptor for the current range of the span.
		descSpan := desc.RSpan()
		spanStats.RangeCount += 1

		voterAndNonVoterReplicas := desc.Replicas().VoterAndNonVoterDescriptors()
		spanStats.ReplicaCount += int32(len(voterAndNonVoterReplicas))
		for _, repl := range voterAndNonVoterReplicas {
			storeIDs[repl.StoreID] = struct{}{}
		}

		// Is the descriptor fully contained by the request span?
		if rSpan.ContainsKeyRange(descSpan.Key, desc.EndKey) {
			// Collect into fullyContainedKeys batch.
			fullyContainedKeysBatch = append(fullyContainedKeysBatch, desc.StartKey.AsRawKey())
			// If we've exceeded the batch limit, request range stats for the current batch.
			if len(fullyContainedKeysBatch) > int(RangeStatsBatchLimit.Get(&s.st.SV)) {
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
			log.VEventf(ctx, 1, "Range %v exceeds span %v, calculating stats for subspan %v",
				descSpan, rSpan, roachpb.RSpan{Key: scanStart, EndKey: scanEnd},
			)

			err = s.stores.VisitStores(func(s *kvserver.Store) error {
				stats, err := storage.ComputeStats(
					ctx,
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

	spanStats.StoreIDs = make([]roachpb.StoreID, 0, len(storeIDs))
	for storeID := range storeIDs {
		spanStats.StoreIDs = append(spanStats.StoreIDs, storeID)
	}
	sort.Slice(spanStats.StoreIDs, func(i, j int) bool {
		return spanStats.StoreIDs[i] < spanStats.StoreIDs[j]
	})

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
	pageSize := int(RangeDescPageSize.Get(&s.st.SV))
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

// verifySpanStatsRequest returns an error if the request can not be serviced.
// Requests can not be serviced if the active cluster version is less than 23.1,
// or if the request is made using the pre 23.1 format.
func verifySpanStatsRequest(
	ctx context.Context, req *roachpb.SpanStatsRequest, version clusterversion.Handle,
) error {
	// If we receive a request using the old format.
	if isLegacyRequest(req) {
		// We want to force 23.1 callers to use the new format (e.g. Spans field).
		if req.NodeID == "0" {
			return errors.New(UnexpectedLegacyRequest)
		}
		// We want to error if we receive a legacy request from a 22.2
		// node (e.g. during a mixed-version fanout).
		return errors.New(MixedVersionErr)
	}

	return nil
}

// batchedSpanStats breaks the request spans down into batches that are
// batchSize large. impl is invoked for each batch. Then, responses from
// each batch are merged together and returned to the caller of this function.
func batchedSpanStats(
	ctx context.Context,
	req *roachpb.SpanStatsRequest,
	impl func(
		ctx context.Context, req *roachpb.SpanStatsRequest,
	) (*roachpb.SpanStatsResponse, error),
	batchSize int,
) (*roachpb.SpanStatsResponse, error) {

	if len(req.Spans) == 0 {
		return &roachpb.SpanStatsResponse{}, nil
	}

	if len(req.Spans) <= batchSize {
		return impl(ctx, req)
	}

	// Just in case, check for an invalid batch size. The batch size
	// should originate from server.span_stats.span_batch_limit, which
	// is validated to be a positive integer.
	if batchSize <= 0 {
		return nil, errors.Newf("invalid batch size of %d, "+
			"batch size must be positive", batchSize)
	}

	totalSpans := len(req.Spans)
	batches := (totalSpans + batchSize - 1) / batchSize

	// Keep a reference of the original spans slice.
	s := req.Spans
	res := &roachpb.SpanStatsResponse{}
	res.SpanToStats = make(map[string]*roachpb.SpanStats, totalSpans)

	for i := 0; i < batches; i++ {

		start := i * batchSize
		end := start + batchSize

		// The total number of spans may not divide evenly by the
		// batch size. If that's the case, take action here
		// to prevent the last batch from indexing past the end
		// of the spans slice.
		if i == batches-1 && totalSpans%batchSize != 0 {
			end = start + totalSpans%batchSize
		}

		req.Spans = s[start:end]
		batchRes, batchErr := impl(ctx, req)
		if batchErr != nil {
			return nil, batchErr
		}

		for k, v := range batchRes.SpanToStats {
			res.SpanToStats[k] = v
		}

		res.Errors = append(res.Errors, batchRes.Errors...)
	}

	return res, nil
}
