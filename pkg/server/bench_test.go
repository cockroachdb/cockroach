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
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
)

func BenchmarkSetupSpanForIncomingRPC(b *testing.B) {
	skip.UnderDeadlock(b, "span reuse triggers false-positives in the deadlock detector")
	defer leaktest.AfterTest(b)()
	ctx := context.Background()
	ba := &roachpb.BatchRequest{Header: roachpb.Header{TraceInfo: tracingpb.TraceInfo{
		TraceID:       1,
		ParentSpanID:  2,
		RecordingMode: tracingpb.TraceInfo_NONE,
	}}}
	b.ReportAllocs()
	tr := tracing.NewTracerWithOpt(ctx,
		tracing.WithTracingMode(tracing.TracingModeActiveSpansRegistry),
		tracing.WithSpanReusePercent(100))
	for i := 0; i < b.N; i++ {
		_, sp := setupSpanForIncomingRPC(ctx, roachpb.SystemTenantID, ba, tr)
		sp.finish(ctx, nil /* br */)
	}
}
