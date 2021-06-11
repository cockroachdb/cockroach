// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package collector

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingservicepb"
)

// NodeLiveness is the subset of the interface satisfied by CRDB's node liveness
// component that the tracing service relies upon.
type NodeLiveness interface {
	GetLivenessesFromKV(context.Context) ([]livenesspb.Liveness, error)
	IsLive(roachpb.NodeID) (bool, error)
}

// TraceCollector can be used to extract recordings from inflight spans for a
// given traceID, from all nodes of the cluster.
type TraceCollector struct {
	tracer       *tracing.Tracer
	dialer       *nodedialer.Dialer
	nodeliveness NodeLiveness
}

// New returns a TraceCollector.
func New(
	dialer *nodedialer.Dialer, nodeliveness NodeLiveness, tracer *tracing.Tracer,
) *TraceCollector {
	return &TraceCollector{
		dialer:       dialer,
		nodeliveness: nodeliveness,
		tracer:       tracer,
	}
}

// TraceCollectorIterator can be used to return RecordedSpans from all live
// nodes in the cluster, in a streaming manner. The iterator buffers the
// RecordedSpans of one node at a time.
type TraceCollectorIterator struct {
	collector *TraceCollector

	ctx context.Context

	traceID uint64

	// liveNodes represents all the nodes in the cluster that are considered live,
	// and will be contacted for inflight trace spans by the iterator.
	liveNodes []roachpb.NodeID

	// curNodeIndex maintains the node from which the iterator has pulled inflight
	// span recordings and buffered them in `recordedSpans` for consumption via
	// the iterator.
	curNodeIndex int

	// recordedSpanIndex maintains the current position of of the iterator in the
	// list of recorded spans. The recorded spans that the iterator points to are
	// buffered in `recordedSpans`.
	recordedSpanIndex int

	// recordedSpans represents all recorded spans for a given node currently
	// accessed by the iterator.
	recordedSpans []tracingpb.RecordedSpan

	iterErr error
}

// Start fetches the live nodes in the cluster, and configures the underlying
// iterator that is used to access recorded spans in a streaming fashion.
func (t *TraceCollector) Start(ctx context.Context, traceID uint64) *TraceCollectorIterator {
	tc := &TraceCollectorIterator{ctx: ctx, traceID: traceID, collector: t, curNodeIndex: -1}
	tc.liveNodes, tc.iterErr = nodesFromNodeLiveness(ctx, t.nodeliveness)
	return tc
}

// Valid returns whether the iterator is in a valid state, and can return a
// RecordedSpan.
// If we have exhausted the buffered recordedSpans, the method will poll other
// live nodes for inflight spans.
func (i *TraceCollectorIterator) Valid() bool {
	if i.iterErr != nil {
		return false
	}

	// If recordedSpanIndex is within recordedSpans and there are some buffered
	// recordedSpans, then we can return them when Next() is called.
	if i.recordedSpans != nil && i.recordedSpanIndex < len(i.recordedSpans) {
		return true
	}

	// Reset buffer variables.
	i.recordedSpans = nil
	i.recordedSpanIndex = 0

	// Otherwise, either there are no more spans or we have exhausted the
	// recordings from the current node, and we need to pull the inflight
	// recordings from another node.
	// Keep searching for recordings from all live nodes in the cluster.
	for i.recordedSpans == nil {
		i.curNodeIndex++
		// No more spans to return from any of the live nodes in the cluster.
		if !(i.curNodeIndex < len(i.liveNodes)) {
			return false
		}
		i.recordedSpans, i.iterErr = i.collector.getTraceSpanRecordingsForNode(i.ctx, i.traceID,
			i.liveNodes[i.curNodeIndex])
		// TODO(adityamaru): We might want to consider not failing if a single node
		// fails to return span recordings.
		if i.iterErr != nil {
			return false
		}
	}

	return true
}

// Next sets the iterator index to point to the next buffered RecordedSpan.
func (i *TraceCollectorIterator) Next() {
	i.recordedSpanIndex++
}

// Value returns the RecordedSpan pointed to by the iterator.
func (i *TraceCollectorIterator) Value() tracingpb.RecordedSpan {
	return i.recordedSpans[i.recordedSpanIndex]
}

// Error returns the error encountered by the iterator when fetching inflight
// span recordings.
func (i *TraceCollectorIterator) Error() error {
	return i.iterErr
}

// getTraceSpanRecordingsForNode returns the inflight span recordings for traces
// with traceID from the node with nodeID. The span recordings are sorted by
// StartTime.
// This method does not distinguish between requests for local and remote
// inflight spans, and relies on gRPC short circuiting local requests.
func (t *TraceCollector) getTraceSpanRecordingsForNode(
	ctx context.Context, traceID uint64, nodeID roachpb.NodeID,
) ([]tracingpb.RecordedSpan, error) {
	log.Infof(ctx, "executing GetSpanRecordings on nodes %s", nodeID.String())
	conn, err := t.dialer.Dial(ctx, nodeID, rpc.DefaultClass)
	if err != nil {
		return nil, err
	}
	traceClient := tracingservicepb.NewTracingClient(conn)
	resp, err := traceClient.GetSpanRecordings(ctx,
		&tracingservicepb.SpanRecordingRequest{TraceID: traceID})
	if err != nil {
		return nil, err
	}

	sort.SliceStable(resp.SpanRecordings, func(i, j int) bool {
		return resp.SpanRecordings[i].StartTime.Before(resp.SpanRecordings[j].StartTime)
	})

	return resp.SpanRecordings, nil
}
