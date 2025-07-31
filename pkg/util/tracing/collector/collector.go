// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package collector

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingservicepb"
)

// TraceCollector can be used to extract recordings from inflight spans for a
// given traceID, from all SQL instances.
type TraceCollector struct {
	tracer       *tracing.Tracer
	getInstances func(context.Context) ([]sqlinstance.InstanceInfo, error)
	dialer       *nodedialer.Dialer
}

// New returns a TraceCollector.
//
// Note that the second argument is not *instancestorage.Reader but an accessor
// method to allow for easier testing setup.
func New(
	tracer *tracing.Tracer,
	getInstances func(context.Context) ([]sqlinstance.InstanceInfo, error),
	dialer *nodedialer.Dialer,
) *TraceCollector {
	return &TraceCollector{
		tracer:       tracer,
		getInstances: getInstances,
		dialer:       dialer,
	}
}

// Iterator can be used to return tracing.Recordings from all instances in the
// cluster, in a streaming manner. The iterator buffers the tracing.Recordings
// of one instance at a time.
type Iterator struct {
	collector *TraceCollector

	traceID tracingpb.TraceID

	// instances stores all SQL instances in the cluster that will be contacted
	// for inflight trace spans by the iterator.
	instances []sqlinstance.InstanceInfo

	// curInstanceIdx maintains the index in instances from which the iterator
	// has pulled inflight span recordings and buffered them in `recordedSpans`
	// for consumption via the iterator.
	curInstanceIdx int

	// curInstanceID maintains the instance ID from which the iterator has
	// pulled inflight span recordings and buffered them in `recordings` for
	// consumption via the iterator.
	curInstanceID base.SQLInstanceID

	// recordingIndex maintains the current position of the iterator in the list
	// of tracing.Recordings. The tracingpb.Recording that the iterator points
	// to is buffered in `recordings`.
	recordingIndex int

	// recordings represent all the tracing.Recordings for a given SQL instance
	// currently accessed by the iterator.
	recordings []tracingpb.Recording
}

// StartIter fetches all SQL instances in the cluster, and configures the
// underlying Iterator that is used to access recorded spans in a streaming
// fashion.
func (t *TraceCollector) StartIter(
	ctx context.Context, traceID tracingpb.TraceID,
) (*Iterator, error) {
	tc := &Iterator{traceID: traceID, collector: t}
	var err error
	tc.instances, err = t.getInstances(ctx)
	if err != nil {
		return nil, err
	}

	// Calling Next() positions the Iterator in a valid state. It will fetch the
	// first set of valid (non-nil) inflight span recordings from the list of
	// SQL instances.
	tc.Next(ctx)

	return tc, nil
}

// Valid returns whether the Iterator is in a valid state to read values from.
func (i *Iterator) Valid() bool {
	return i.recordingIndex < len(i.recordings)
}

// Next sets the Iterator to point to the next value to be returned.
func (i *Iterator) Next(ctx context.Context) {
	i.recordingIndex++

	// If recordingIndex is within recordings and there are some buffered
	// recordings, it is valid to return from the buffer.
	if i.recordingIndex < len(i.recordings) {
		return
	}

	// Reset buffer variables.
	i.recordings = nil
	i.recordingIndex = 0

	// Either there are no more spans or we have exhausted the recordings from
	// the current instance, and we need to pull the inflight recordings from
	// another instance.
	// Keep searching for recordings from all SQL instances in the cluster.
	for len(i.recordings) == 0 {
		// No more spans to return from any of the SQL instances in the cluster.
		if !(i.curInstanceIdx < len(i.instances)) {
			return
		}
		i.curInstanceID = i.instances[i.curInstanceIdx].InstanceID
		i.recordings = i.collector.getTraceSpanRecordingsForInstance(ctx, i.traceID, i.curInstanceID)
		i.curInstanceIdx++
	}
}

// Value returns the current value pointed to by the Iterator.
func (i *Iterator) Value() (base.SQLInstanceID, tracingpb.Recording) {
	return i.curInstanceID, i.recordings[i.recordingIndex]
}

// getTraceSpanRecordingsForInstance returns the inflight span recordings for
// traces with traceID from the SQL instance with the given ID. The span
// recordings are sorted by StartTime. If any error is encountered, then nil
// slice is returned.
//
// This method does not distinguish between requests for local and remote
// inflight spans, and relies on gRPC short-circuiting local requests.
func (t *TraceCollector) getTraceSpanRecordingsForInstance(
	ctx context.Context, traceID tracingpb.TraceID, instanceID base.SQLInstanceID,
) []tracingpb.Recording {
	log.Infof(ctx, "getting span recordings from instance %s", instanceID)
	conn, err := t.dialer.Dial(ctx, roachpb.NodeID(instanceID), rpc.DefaultClass)
	if err != nil {
		log.Warningf(ctx, "failed to dial instance %s: %v", instanceID, err)
		return nil
	}
	traceClient := tracingservicepb.NewTracingClient(conn)
	var resp *tracingservicepb.GetSpanRecordingsResponse
	resp, err = traceClient.GetSpanRecordings(ctx,
		&tracingservicepb.GetSpanRecordingsRequest{TraceID: traceID})
	if err != nil {
		log.Warningf(ctx, "failed to get span recordings from instance %s: %v", instanceID, err)
		return nil
	}

	res := make([]tracingpb.Recording, 0, len(resp.Recordings))
	for _, recording := range resp.Recordings {
		if recording.RecordedSpans == nil {
			continue
		}
		res = append(res, recording.RecordedSpans)
	}
	resp.Recordings = nil

	// This sort ensures that if a node has multiple trace.Recordings then they
	// are ordered relative to each other by StartTime.
	sort.SliceStable(res, func(i, j int) bool {
		return res[i][0].StartTime.Before(res[j][0].StartTime)
	})

	return res
}
