// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/sched"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestNilProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	var p *LegacyProcessor

	// All of the following should be no-ops.
	require.Equal(t, 0, p.Len())
	require.NotPanics(t, func() { p.Stop() })
	require.NotPanics(t, func() { p.StopWithErr(nil) })
	require.NotPanics(t, func() { p.ConsumeLogicalOps(ctx) })
	require.NotPanics(t, func() { p.ConsumeLogicalOps(ctx, make([]enginepb.MVCCLogicalOp, 5)...) })
	require.NotPanics(t, func() { p.ForwardClosedTS(ctx, hlc.Timestamp{}) })
	require.NotPanics(t, func() { p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 1}) })

	// The following should panic because they are not safe
	// to call on a nil Processor.
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	require.Panics(t, func() { _ = p.Start(stopper, nil) })
	require.Panics(t, func() {
		var done future.ErrorFuture
		p.Register(roachpb.RSpan{}, hlc.Timestamp{}, nil, false, nil,
			sched.NewClientScheduler(0, nil), func() {}, &done,
		)
	})
}
