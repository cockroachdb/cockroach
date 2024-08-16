// Copyright 2018 The Cockroach Authors.
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

	_ "github.com/cockroachdb/cockroach/pkg/keys" // hook up pretty printer
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestUnbufferedRegistration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1, ev2 := new(kvpb.RangeFeedEvent), new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val})
	ev2.MustSetValue(&kvpb.RangeFeedValue{Key: keyB, Value: val})

	t.Run("registration with no catchup scan specified", func(t *testing.T) {
		s := newTestStream()
		noCatchupReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(unbuffered))
		noCatchupReg.publish(ctx, ev1, nil /* alloc */)
		noCatchupReg.publish(ctx, ev2, nil /* alloc */)
		if noCatchupReg, ok := noCatchupReg.(*bufferedRegistration); ok {
			require.Equal(t, len(noCatchupReg.buf), 2)
		}
		go noCatchupReg.runOutputLoop(ctx, 0)
		require.NoError(t, noCatchupReg.waitForCaughtUp(ctx))
		require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2}, s.Events())
		noCatchupReg.disconnect(nil)
	})
}
