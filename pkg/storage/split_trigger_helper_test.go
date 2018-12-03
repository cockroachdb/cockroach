// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/raft/raftpb"

	"github.com/cockroachdb/cockroach/pkg/roachpb"

	"github.com/stretchr/testify/assert"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type testMsgAppDropper struct {
	initialized bool
	ticks       int
	lhs         bool

	startKey string // set by ShouldDrop
}

func (td *testMsgAppDropper) Args() (initialized bool, ticks int) {
	return td.initialized, td.ticks
}

func (td *testMsgAppDropper) ShouldDrop(startKey roachpb.RKey) (fmt.Stringer, bool) {
	if len(startKey) == 0 {
		panic("empty startKey")
	}
	td.startKey = string(startKey)
	return &Replica{}, td.lhs
}

func TestMaybeDropMsgApp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := map[testMsgAppDropper]bool{
		// Already init'ed.
		{initialized: true}: false,
		// Left hand side not found.
		{initialized: false}: false,
		// Drop message to wait for trigger.
		{initialized: false, lhs: true}: true,
		// Drop message to wait for trigger.
		{initialized: false, lhs: true, ticks: maxDelaySplitTriggerTicks}: true,
		// Escape hatch fires.
		{initialized: false, lhs: true, ticks: maxDelaySplitTriggerTicks + 1}: false,
	}

	msgHeartbeat := &raftpb.Message{
		Type: raftpb.MsgHeartbeat,
	}
	msgAppNoContext := &raftpb.Message{
		Type:    raftpb.MsgApp,
		Context: nil,
	}
	msgAppFoo := &raftpb.Message{
		Type:    raftpb.MsgApp,
		Context: []byte("foo"),
	}
	ctx := context.Background()
	for dropper, exp := range testCases {
		t.Run(fmt.Sprintf("%v", dropper), func(t *testing.T) {
			assert.Equal(t, false, maybeDropMsgApp(ctx, &dropper, msgHeartbeat))
			assert.Equal(t, false, maybeDropMsgApp(ctx, &dropper, msgAppNoContext))
			assert.Equal(t, "", dropper.startKey)
			assert.Equal(t, exp, maybeDropMsgApp(ctx, &dropper, msgAppFoo))
			if exp {
				assert.Equal(t, string(msgAppFoo.Context), dropper.startKey)
			}
		})
	}
}
