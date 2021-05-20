// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracingservice

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracingservice/tracingservicepb"
)

// NodeLiveness is the subset of the interface satisfied by CRDB's node liveness
// component that the tracing service relies upon.
type NodeLiveness interface {
	GetLivenessesFromKV(context.Context) ([]livenesspb.Liveness, error)
	IsLive(roachpb.NodeID) (bool, error)
}

// TraceClientDialer can be used to extract recordings from inflight spans for a
// given traceID, from all nodes of the cluster.
type TraceClientDialer struct {
	tracer       *tracing.Tracer
	dialer       *nodedialer.Dialer
	nodeliveness NodeLiveness
}

// NewTraceClientDialer returns a TraceClientDialer.
func NewTraceClientDialer(
	dialer *nodedialer.Dialer, nodeliveness NodeLiveness, tracer *tracing.Tracer,
) *TraceClientDialer {
	return &TraceClientDialer{
		dialer:       dialer,
		nodeliveness: nodeliveness,
		tracer:       tracer,
	}
}

// GetSpanRecordingsFromCluster returns the inflight span recordings from all
// nodes in the cluster.
func (t *TraceClientDialer) GetSpanRecordingsFromCluster(
	ctx context.Context, traceID uint64,
) ([]tracingpb.RecordedSpan, error) {
	var res []tracingpb.RecordedSpan
	ns, err := nodesFromNodeLiveness(ctx, t.nodeliveness)
	if err != nil {
		return res, err
	}

	// Collect spans from all clients.
	// We'll want to rate limit outgoing RPCs (limit pulled out of thin air).
	var mu syncutil.Mutex
	qp := quotapool.NewIntPool("every-node", 25)
	log.Infof(ctx, "executing GetSpanRecordings on nodes %s", ns)
	grp := ctxgroup.WithContext(ctx)

	for _, node := range ns {
		id := node.id // copy out of the loop variable
		alloc, err := qp.Acquire(ctx, 1)
		if err != nil {
			return res, err
		}

		grp.GoCtx(func(ctx context.Context) error {
			defer alloc.Release()

			conn, err := t.dialer.Dial(ctx, id, rpc.DefaultClass)
			if err != nil {
				return err
			}
			traceClient := tracingservicepb.NewTracingClient(conn)
			resp, err := traceClient.GetSpanRecordings(ctx,
				&tracingservicepb.SpanRecordingRequest{TraceID: traceID})
			if err != nil {
				return err
			}
			mu.Lock()
			res = append(res, resp.SpanRecordings...)
			mu.Unlock()
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return res, err
	}

	sort.SliceStable(res, func(i, j int) bool {
		return res[i].StartTime.Before(res[j].StartTime)
	})

	return res, nil
}
