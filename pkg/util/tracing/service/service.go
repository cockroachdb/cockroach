// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package service contains a gRPC service to be used for remote inflight
span access.

It is used for pulling inflight spans from all CockroachDB nodes. Each node will
run a trace service, which serves the inflight spans from the local span
registry on that node. Each node will also have a trace collector, which uses
the nodedialer to connect to another node's trace service, and access its
inflight spans.
*/
package service

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingservicepb"
)

// Service implements the gRPC TraceServer that exchanges inflight span
// recordings between different nodes.
type Service struct {
	tracer *tracing.Tracer
}

var _ tracingservicepb.TracingServer = &Service{}

// New instantiates a tracing service server.
func New(tracer *tracing.Tracer) *Service {
	return &Service{tracer: tracer}
}

// GetSpanRecordings implements the tracingpb.TraceServer interface.
//
// This method iterates over all active root spans registered with the nodes'
// local inflight span registry, and returns a tracing.Recording for each root
// span with a matching trace_id.
func (s *Service) GetSpanRecordings(
	_ context.Context, request *tracingservicepb.GetSpanRecordingsRequest,
) (*tracingservicepb.GetSpanRecordingsResponse, error) {
	var resp tracingservicepb.GetSpanRecordingsResponse
	err := s.tracer.VisitSpans(func(span *tracing.Span) error {
		if span.TraceID() != request.TraceID {
			return nil
		}
		recording := span.GetRecording()
		if recording != nil {
			resp.Recordings = append(resp.Recordings,
				tracingservicepb.GetSpanRecordingsResponse_Recording{RecordedSpans: recording})
		}
		return nil
	})
	return &resp, err
}
