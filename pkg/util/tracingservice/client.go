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

	"github.com/cockroachdb/cockroach/pkg/migration/migrationcluster"
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

// TraceClient provides an interface for accessing the recordings of inflight
// spans for a given traceID on a nodes' inflight registry.
type TraceClient interface {
	GetSpanRecordings(ctx context.Context, traceID uint64) (*tracingservicepb.SpanRecordingResponse, error)
}

var _ TraceClient = &localTraceClient{}

// localTraceClient executes the local tracing service's code to fetch inflight
// span recordings.
type localTraceClient struct {
	tracer *tracing.Tracer
}

// newLocalTraceClient returns a local tracing service client.
func newLocalTraceClient(tracer *tracing.Tracer) TraceClient {
	return &localTraceClient{tracer: tracer}
}

// GetSpanRecordings implements the TraceClient interface.
func (c *localTraceClient) GetSpanRecordings(
	_ context.Context, traceID uint64,
) (*tracingservicepb.SpanRecordingResponse, error) {
	var resp tracingservicepb.SpanRecordingResponse
	err := c.tracer.VisitSpans(func(span *tracing.Span) error {
		if span.TraceID() != traceID {
			return nil
		}
		for _, rec := range span.GetRecording() {
			resp.SpanRecordings = append(resp.SpanRecordings, rec)
		}
		return nil
	})
	return &resp, err
}

var _ TraceClient = &remoteTraceClient{}

// remoteClient uses the node dialer and tracing service clients to fetch
// span recordings from other nodes.
type remoteTraceClient struct {
	traceClient tracingservicepb.TraceClient
}

// newRemoteClient instantiates a remote tracing service client.
func newRemoteTraceClient(traceClient tracingservicepb.TraceClient) TraceClient {
	return &remoteTraceClient{traceClient: traceClient}
}

// GetSpanRecordings implements the TraceClient interface.
func (c *remoteTraceClient) GetSpanRecordings(
	ctx context.Context, traceID uint64,
) (*tracingservicepb.SpanRecordingResponse, error) {
	return c.traceClient.GetSpanRecordings(ctx, &tracingservicepb.SpanRecordingRequest{TraceID: traceID})
}

// TraceClientDialer can be used to extract recordings from inflight spans for a
// given traceID, from all nodes of the cluster.
type TraceClientDialer struct {
	tracer       *tracing.Tracer
	localNodeID  roachpb.NodeID
	dialer       *nodedialer.Dialer
	nodeliveness migrationcluster.NodeLiveness
}

// NewTraceClientDialer returns a TraceClientDialer.
func NewTraceClientDialer(
	localNodeID roachpb.NodeID,
	dialer *nodedialer.Dialer,
	nodeliveness migrationcluster.NodeLiveness,
	tracer *tracing.Tracer,
) *TraceClientDialer {
	return &TraceClientDialer{
		localNodeID:  localNodeID,
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
	ns, err := migrationcluster.NodesFromNodeLiveness(ctx, t.nodeliveness)
	if err != nil {
		return res, err
	}

	// Collect spans from the local client.
	localClient := newLocalTraceClient(t.tracer)
	localSpanRecordings, err := localClient.GetSpanRecordings(ctx, traceID)
	if err != nil {
		return res, err
	}
	var mu syncutil.Mutex
	res = append(res, localSpanRecordings.SpanRecordings...)

	// Collect spans from remote clients.
	// We'll want to rate limit outgoing RPCs (limit pulled out of thin air).
	qp := quotapool.NewIntPool("every-node", 25)
	log.Infof(ctx, "executing GetSpanRecordings on nodes %s", ns)
	grp := ctxgroup.WithContext(ctx)

	for _, node := range ns {
		if node.ID == t.localNodeID {
			continue
		}
		id := node.ID // copy out of the loop variable
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
			remoteClient := newRemoteTraceClient(tracingservicepb.NewTraceClient(conn))
			resp, err := remoteClient.GetSpanRecordings(ctx, traceID)
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
