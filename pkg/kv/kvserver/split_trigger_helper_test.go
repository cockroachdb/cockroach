// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/assert"
)

type testMsgAppDropper struct {
	initialized bool
	age         time.Duration
	lhs         bool

	startKey string // set by ShouldDrop
}

func (td *testMsgAppDropper) Args() (initialized bool, age time.Duration) {
	return td.initialized, td.age
}

func (td *testMsgAppDropper) ShouldDrop(
	ctx context.Context, startKey roachpb.RKey,
) (fmt.Stringer, bool) {
	if len(startKey) == 0 {
		panic("empty startKey")
	}
	td.startKey = string(startKey)
	return &Replica{}, td.lhs
}

func TestMaybeDropMsgApp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := map[testMsgAppDropper]bool{
		// Already load'ed.
		{initialized: true}: false,
		// Left hand side not found.
		{initialized: false}: false,
		// Drop message to wait for trigger.
		{initialized: false, lhs: true}: true,
		// Drop message to wait for trigger.
		{initialized: false, lhs: true, age: maxDelaySplitTriggerDur}: true,
		// Escape hatch fires.
		{initialized: false, lhs: true, age: maxDelaySplitTriggerDur + 1}: false,
	}

	msgHeartbeat := &raftpb.Message{
		Type: raftpb.MsgHeartbeat,
	}
	msgApp := &raftpb.Message{
		Type: raftpb.MsgApp,
	}
	ctx := context.Background()
	for dropper, exp := range testCases {
		t.Run(fmt.Sprintf("%v", dropper), func(t *testing.T) {
			assert.Equal(t, false, maybeDropMsgApp(ctx, &dropper, msgHeartbeat, nil))
			assert.Equal(t, false, maybeDropMsgApp(ctx, &dropper, msgApp, nil))
			assert.Equal(t, "", dropper.startKey)
			startKey := roachpb.RKey("foo")
			assert.Equal(t, exp, maybeDropMsgApp(ctx, &dropper, msgApp, startKey))
			if exp {
				assert.Equal(t, string(startKey), dropper.startKey)
			}
		})
	}
}

// TestProtoZeroNilSlice verifies that the proto encoding round-trips empty and
// nil byte slices correctly.
func TestProtoZeroNilSlice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "isNil", func(t *testing.T, isNil bool) {
		msg := &kvserverpb.RaftMessageRequest{}
		if !isNil {
			msg.RangeStartKey = roachpb.RKey("foo")
		}
		b, err := protoutil.Marshal(msg)
		assert.NoError(t, err)
		out := &kvserverpb.RaftMessageRequest{}
		assert.NoError(t, protoutil.Unmarshal(b, out))
		assert.Equal(t, isNil, out.RangeStartKey == nil)
	})
}
