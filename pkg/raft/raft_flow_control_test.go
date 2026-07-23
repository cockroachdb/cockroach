// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestMsgAppFlowControlFull ensures:
// 1. msgApp can fill the sending window until full
// 2. when the window is full, no more msgApp can be sent.

func TestMsgAppFlowControlFull(t *testing.T) {
	r := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.becomeCandidate()
	r.becomeLeader()

	pr2 := r.trk.Progress(2)
	// force the progress to be in replicate state
	pr2.BecomeReplicate()
	// fill in the inflights window
	for i := 0; i < r.maxInflight; i++ {
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
		ms := r.readMessages()
		require.Len(t, ms, 1)
		require.Equal(t, ms[0].Type, pb.MsgApp)
	}

	// ensure 1
	if !pr2.IsPaused() {
		t.Fatal("paused = false, want true")
	}

	// ensure 2
	for i := 0; i < 10; i++ {
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
		ms := r.readMessages()
		require.Empty(t, ms)
	}
}

// TestMsgAppFlowControlMoveForward ensures msgAppResp can move
// forward the sending window correctly:
// 1. valid msgAppResp.index moves the windows to pass all smaller or equal index.
// 2. out-of-dated msgAppResp has no effect on the sliding window.
func TestMsgAppFlowControlMoveForward(t *testing.T) {
	r := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.becomeCandidate()
	r.becomeLeader()

	pr2 := r.trk.Progress(2)
	// force the progress to be in replicate state
	pr2.BecomeReplicate()
	// fill in the inflights window
	for i := 0; i < r.maxInflight; i++ {
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
		r.readMessages()
	}

	// 1 is noop, 2 is the first proposal we just sent.
	// so we start with 2.
	for tt := 2; tt < r.maxInflight; tt++ {
		// move forward the window
		r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgAppResp, Index: uint64(tt)})
		r.readMessages()

		// fill in the inflights window again
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
		ms := r.readMessages()
		require.Len(t, ms, 1)
		require.Equal(t, ms[0].Type, pb.MsgApp)

		// ensure 1
		if !pr2.IsPaused() {
			t.Fatalf("#%d: paused = false, want true", tt)
		}

		// ensure 2
		for i := 0; i < tt; i++ {
			r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgAppResp, Index: uint64(i)})
			if !pr2.IsPaused() {
				t.Fatalf("#%d.%d: paused = false, want true", tt, i)
			}
		}
	}
}

// TestMsgAppFlowControl ensures that if storeliveness is disabled, a heartbeat
// response frees one slot if the window is full. If storelivess is enabled,
// a similar thing happens but on the next heartbeat timeout.
func TestMsgAppFlowControl(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testOptions := emptyTestConfigModifierOpt()
			if !storeLivenessEnabled {
				testOptions = withStoreLiveness(raftstoreliveness.Disabled{})
			}

			r := newTestRaft(1, 5, 1,
				newTestMemoryStorage(withPeers(1, 2)), testOptions)
			r.becomeCandidate()
			r.becomeLeader()

			pr2 := r.trk.Progress(2)
			// force the progress to be in replicate state
			pr2.BecomeReplicate()
			// fill in the inflights window
			for i := 0; i < r.maxInflight; i++ {
				r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp,
					Entries: []pb.Entry{{Data: []byte("somedata")}}})
				r.readMessages()
			}

			for tt := 1; tt < 5; tt++ {
				for i := 0; i < tt; i++ {
					if !pr2.IsPaused() {
						t.Fatalf("#%d.%d: paused = false, want true", tt, i)
					}

					// Unpauses the progress, sends an empty MsgApp, and pauses it again.
					// When storeliveness is enabled, we do this on the next heartbeat
					// timeout. However, when storeliveness is disabled, we do this on
					// the next heartbeat response.
					if storeLivenessEnabled {
						for ticks := r.heartbeatTimeout; ticks > 0; ticks-- {
							r.tick()
						}
						ms := r.readMessages()
						require.Len(t, ms, 2)
						require.Equal(t, ms[0].Type, pb.MsgFortifyLeader)
						require.Equal(t, ms[1].Type, pb.MsgApp)
						require.Empty(t, ms[1].Entries)
					} else {
						r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeatResp})
						ms := r.readMessages()
						require.Len(t, ms, 1)
						require.Equal(t, ms[0].Type, pb.MsgApp)
						require.Empty(t, ms[0].Entries)
					}
				}

				// No more appends are sent if there are no heartbeats.
				for i := 0; i < 10; i++ {
					if !pr2.IsPaused() {
						t.Fatalf("#%d.%d: paused = false, want true", tt, i)
					}
					r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp,
						Entries: []pb.Entry{{Data: []byte("somedata")}}})
					ms := r.readMessages()
					require.Empty(t, ms)
				}

				// clear all pending messages.
				for ticks := r.heartbeatTimeout; ticks > 0; ticks-- {
					r.tick()
				}
				r.readMessages()
			}
		})
}
