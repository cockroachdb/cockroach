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
		evContentionStart
		evSequenceStart
		evEvalStart
		evReplStart
	)
	mc := hlc.NewManualClock(1)
	tm := &Timing{Now: func() time.Time {
		return time.Unix(0, mc.UnixNano())
	}}
	r := rand.New(rand.NewSource(123))
	_ = r
	tick := func() {
		nanos := r.Int63n(100)
		mc.Increment(nanos)
	}

	tm.Event(ctx, evStart)
	tick()
	tm.Event(ctx, evLeaseStart)
	tick()
	tm.Event(ctx, &End{evLeaseStart})
	tick()
	tm.Event(ctx, evSequenceStart)
	tick()
	tm.Event(ctx, &End{evSequenceStart})
	tick()
	tm.Event(ctx, evContentionStart)
	tick()
	tm.Event(ctx, &End{evContentionStart})
	tick()
	tm.Event(ctx, evSequenceStart)
	tick()
	tm.Event(ctx, &End{evSequenceStart})
	tick()
	tm.Event(ctx, evEvalStart)
	tick()
	tm.Event(ctx, &End{evEvalStart})
	tick()
	tm.Event(ctx, evReplStart)
	tick()
	tm.Event(ctx, &End{evReplStart})

	require.EqualValues(t, mc.UnixNano(), 1+tm.Duration())
	t.Error(tm.Summary())
}
