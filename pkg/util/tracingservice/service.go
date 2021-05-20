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
Package tracingservice contains a gRPC service to be used for remote inflight
span access.

It is used for pulling inflight spans from all CockroachDB nodes.
Each node will run a trace service, which serves the inflight spans from the
local span registry on that node. Each node will also have a trace client
dialer, which uses the nodedialer to connect to another node's trace service,
and access its inflight spans. The trace client dialer is backed by a remote
trace client or a local trace client, which serve as the point of entry to this
service. Both clients support the `TraceClient` interface, which includes the
following functionalities:
  - GetSpanRecordings
*/

package tracingservice

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracingservice/tracingservicepb"
)

// Service implements the gRPC TraceServer that exchanges inflight span
// recordings between different nodes.
type Service struct {
	tracer *tracing.Tracer
}

var _ tracingservicepb.TraceServer = &Service{}

// NewTraceService instantiates a blob service server.
func NewTraceService(tracer *tracing.Tracer) *Service {
	return &Service{tracer: tracer}
}

// GetSpanRecordings implements the tracingpb.TraceServer interface.
func (s *Service) GetSpanRecordings(
	_ context.Context, request *tracingservicepb.SpanRecordingRequest,
) (*tracingservicepb.SpanRecordingResponse, error) {
	var resp tracingservicepb.SpanRecordingResponse
	err := s.tracer.VisitSpans(func(span *tracing.Span) error {
		if span.TraceID() != request.TraceID {
			return nil
		}
		for _, rec := range span.GetRecording() {
			resp.SpanRecordings = append(resp.SpanRecordings, rec)
		}
		return nil
	})
	return &resp, err
}
