// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	RegisterReadWriteCommand(roachpb.Barrier, declareKeysBarrier, Barrier)
}

func declareKeysBarrier(
	rs ImmutableRangeState,
	_ roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	// Barrier is special-cased in the concurrency manager to *not* actually
	// grab these latches. Instead, any conflicting latches with these are waited
	// on, but new latches aren't inserted.
	latchSpans.AddMVCC(spanset.SpanReadOnly, req.Header().Span(), hlc.MaxTimestamp)
}

// Barrier evaluation is a no-op, as all the latch waiting happens in
// the latch manager.
func Barrier(
	_ context.Context, _ storage.ReadWriter, cArgs CommandArgs, response roachpb.Response,
) (result.Result, error) {
	resp := response.(*roachpb.BarrierResponse)
	resp.Timestamp = cArgs.EvalCtx.Clock().Now()

	return result.Result{}, nil
}
