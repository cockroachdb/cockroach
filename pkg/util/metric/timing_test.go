// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestTiming(t *testing.T) {
	ctx := context.Background()

	const (
		evStart = iota
		evLeaseStart
		evLeaseEnd
		evContentionStart
		evContentionEnd
		evSequenceBegin
		evSequenceEnd
		evEvalStart
		evEvalEnd
		evReplStart
		evReplEnd
	)
	mc := hlc.NewManualClock(0)
	tm := &Timing{Now: func() time.Time {
		return time.Unix(0, mc.UnixNano())
	}}
	r := rand.New(rand.NewSource(123))
	tick := func() {
		nanos := r.Int63n(5000)
		mc.Increment(nanos)
	}

	tm.Event(ctx, evStart)
	tick()
	tm.Event(ctx, evLeaseStart)
	tick()
	tm.Event(ctx, evLeaseEnd)
	tick()
	tm.Event(ctx, evSequenceBegin)
	tick()
	tm.Event(ctx, evSequenceEnd)
	tick()
	tm.Event(ctx, evContentionStart)
	tick()
	tm.Event(ctx, evContentionEnd)
	tick()
	tm.Event(ctx, evSequenceBegin)
	tick()
	tm.Event(ctx, evSequenceEnd)
	tick()
	tm.Event(ctx, evEvalStart)
	tick()
	tm.Event(ctx, evEvalEnd)
	tick()
	tm.Event(ctx, evReplStart)
	tick()
	tm.Event(ctx, evReplEnd)

	require.EqualValues(t, mc.UnixNano(), tm.Duration())
}
