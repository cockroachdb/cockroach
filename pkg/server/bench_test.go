// Copyright 2014 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"google.golang.org/grpc/metadata"
)

func BenchmarkSetupSpanForIncomingRPC(b *testing.B) {
	skip.UnderDeadlock(b, "span reuse triggers false-positives in the deadlock detector")
	defer leaktest.AfterTest(b)()

	for _, tc := range []struct {
		name      string
		traceInfo bool
		grpcMeta  bool
	}{
		{name: "traceInfo", traceInfo: true},
		{name: "grpcMeta", grpcMeta: true},
		{name: "no parent"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()

			ctx := context.Background()
			tr := tracing.NewTracerWithOpt(ctx,
				tracing.WithTracingMode(tracing.TracingModeActiveSpansRegistry),
				tracing.WithSpanReusePercent(100))
			parentSpan := tr.StartSpan("parent")
			defer parentSpan.Finish()

			ba := &roachpb.BatchRequest{}
			if tc.traceInfo {
				ba.TraceInfo = parentSpan.Meta().ToProto()
			} else if tc.grpcMeta {
				traceCarrier := tracing.MapCarrier{
					Map: make(map[string]string),
				}
				tr.InjectMetaInto(parentSpan.Meta(), traceCarrier)
				ctx = metadata.NewIncomingContext(ctx, metadata.New(traceCarrier.Map))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, sp := setupSpanForIncomingRPC(ctx, roachpb.SystemTenantID, ba, tr)
				sp.finish(ctx, nil /* br */)
			}
		})
	}
}
