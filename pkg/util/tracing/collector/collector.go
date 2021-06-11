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

// GetNodesForTraceCollection initializes the TraceCollector by identifying all
// non-decommissioned nodes in the cluster.
func (t *TraceCollector) GetNodesForTraceCollection(ctx context.Context) ([]roachpb.NodeID, error) {
	return nodesFromNodeLiveness(ctx, t.nodeliveness)
}

// GetTraceSpanRecordingsForNode returns the inflight span recordings for traces
// with traceID from the node with nodeID. The span recordings are sorted by
// StartTime.
//
// GetTraceSpanRecordingsForNode can be called concurrently to parallelize the
// RPC fan out to all the nodes in a cluster, but the user is responsible for
// budgeting for the span recordings buffered in memory.
//
// This method does not distinguish between requests for local and remote
// inflight spans, and relies on gRPC short circuiting local requests.
func (t *TraceCollector) GetTraceSpanRecordingsForNode(
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
