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
	"fmt"
	"math"
	"strings"
	"testing"

	rt "github.com/cockroachdb/cockroach/pkg/raft/rafttype"
	"github.com/stretchr/testify/require"
)

var testFormatter EntryFormatter = func(data []byte) string {
	return strings.ToUpper(string(data))
}

func TestDescribeEntry(t *testing.T) {
	entry := rt.Entry{
		Term:  1,
		Index: 2,
		Type:  rt.EntryNormal,
		Data:  []byte("hello\x00world"),
	}
	require.Equal(t, `1/2 EntryNormal "hello\x00world"`, DescribeEntry(entry, nil))
	require.Equal(t, "1/2 EntryNormal HELLO\x00WORLD", DescribeEntry(entry, testFormatter))
}

func TestLimitSize(t *testing.T) {
	ents := []rt.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}
	prefix := func(size int) []rt.Entry {
		return append([]rt.Entry{}, ents[:size]...) // protect the original slice
	}
	for _, tt := range []struct {
		maxSize uint64
		want    []rt.Entry
	}{
		{math.MaxUint64, prefix(len(ents))}, // all entries are returned
		// Even if maxSize is zero, the first entry should be returned.
		{0, prefix(1)},
		// Limit to 2.
		{uint64(ents[0].Size() + ents[1].Size()), prefix(2)},
		{uint64(ents[0].Size() + ents[1].Size() + ents[2].Size()/2), prefix(2)},
		{uint64(ents[0].Size() + ents[1].Size() + ents[2].Size() - 1), prefix(2)},
		// All.
		{uint64(ents[0].Size() + ents[1].Size() + ents[2].Size()), prefix(3)},
	} {
		t.Run("", func(t *testing.T) {
			got := limitSize(ents, entryEncodingSize(tt.maxSize))
			require.Equal(t, tt.want, got)
			size := entsSize(got)
			require.True(t, len(got) == 1 || size <= entryEncodingSize(tt.maxSize))
		})
	}
}

func TestIsLocalMsg(t *testing.T) {
	tests := []struct {
		msgt    rt.MessageType
		isLocal bool
	}{
		{rt.MsgHup, true},
		{rt.MsgBeat, true},
		{rt.MsgUnreachable, true},
		{rt.MsgSnapStatus, true},
		{rt.MsgCheckQuorum, true},
		{rt.MsgTransferLeader, false},
		{rt.MsgProp, false},
		{rt.MsgApp, false},
		{rt.MsgAppResp, false},
		{rt.MsgVote, false},
		{rt.MsgVoteResp, false},
		{rt.MsgSnap, false},
		{rt.MsgHeartbeat, false},
		{rt.MsgHeartbeatResp, false},
		{rt.MsgTimeoutNow, false},
		{rt.MsgPreVote, false},
		{rt.MsgPreVoteResp, false},
		{rt.MsgStorageAppend, true},
		{rt.MsgStorageAppendResp, true},
		{rt.MsgStorageApply, true},
		{rt.MsgStorageApplyResp, true},
		{rt.MsgForgetLeader, false},
		{rt.MsgFortifyLeader, false},
		{rt.MsgFortifyLeaderResp, false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.msgt), func(t *testing.T) {
			require.Equal(t, tt.isLocal, IsLocalMsg(tt.msgt))
		})
	}
}

func TestIsResponseMsg(t *testing.T) {
	tests := []struct {
		msgt       rt.MessageType
		isResponse bool
	}{
		{rt.MsgHup, false},
		{rt.MsgBeat, false},
		{rt.MsgUnreachable, true},
		{rt.MsgSnapStatus, false},
		{rt.MsgCheckQuorum, false},
		{rt.MsgTransferLeader, false},
		{rt.MsgProp, false},
		{rt.MsgApp, false},
		{rt.MsgAppResp, true},
		{rt.MsgVote, false},
		{rt.MsgVoteResp, true},
		{rt.MsgSnap, false},
		{rt.MsgHeartbeat, false},
		{rt.MsgHeartbeatResp, true},
		{rt.MsgTimeoutNow, false},
		{rt.MsgPreVote, false},
		{rt.MsgPreVoteResp, true},
		{rt.MsgStorageAppend, false},
		{rt.MsgStorageAppendResp, true},
		{rt.MsgStorageApply, false},
		{rt.MsgStorageApplyResp, true},
		{rt.MsgForgetLeader, false},
		{rt.MsgFortifyLeader, false},
		{rt.MsgFortifyLeaderResp, true},
	}

	for i, tt := range tests {
		got := IsResponseMsg(tt.msgt)
		if got != tt.isResponse {
			t.Errorf("#%d: got %v, want %v", i, got, tt.isResponse)
		}
	}
}

func TestMsgFromLeader(t *testing.T) {
	tests := []struct {
		msgt       rt.MessageType
		isResponse bool
	}{
		{rt.MsgHup, false},
		{rt.MsgBeat, false},
		{rt.MsgUnreachable, false},
		{rt.MsgSnapStatus, false},
		{rt.MsgCheckQuorum, false},
		{rt.MsgTransferLeader, false},
		{rt.MsgProp, false},
		{rt.MsgApp, true},
		{rt.MsgAppResp, false},
		{rt.MsgVote, false},
		{rt.MsgVoteResp, false},
		{rt.MsgSnap, true},
		{rt.MsgHeartbeat, true},
		{rt.MsgHeartbeatResp, false},
		{rt.MsgTimeoutNow, true},
		{rt.MsgPreVote, false},
		{rt.MsgPreVoteResp, false},
		{rt.MsgStorageAppend, false},
		{rt.MsgStorageAppendResp, false},
		{rt.MsgStorageApply, false},
		{rt.MsgStorageApplyResp, false},
		{rt.MsgForgetLeader, false},
		{rt.MsgFortifyLeader, true},
		{rt.MsgFortifyLeaderResp, false},
	}

	for i, tt := range tests {
		got := IsMsgFromLeader(tt.msgt)
		if got != tt.isResponse {
			t.Errorf("#%d: got %v, want %v", i, got, tt.isResponse)
		}
	}
}

// TestPayloadSizeOfEmptyEntry ensures that payloadSize of empty entry is always zero.
// This property is important because new leaders append an empty entry to their log,
// and we don't want this to count towards the uncommitted log quota.
func TestPayloadSizeOfEmptyEntry(t *testing.T) {
	e := rt.Entry{Data: nil}
	require.Equal(t, 0, int(payloadSize(e)))
}
