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

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
)

var testFormatter EntryFormatter = func(data []byte) string {
	return strings.ToUpper(string(data))
}

func TestDescribeEntry(t *testing.T) {
	entry := pb.Entry{
		Term:  1,
		Index: 2,
		Type:  pb.EntryNormal,
		Data:  []byte("hello\x00world"),
	}
	require.Equal(t, `1/2 EntryNormal "hello\x00world"`, DescribeEntry(entry, nil))
	require.Equal(t, "1/2 EntryNormal HELLO\x00WORLD", DescribeEntry(entry, testFormatter))
}

func TestLimitSize(t *testing.T) {
	ents := []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}
	prefix := func(size int) []pb.Entry {
		return append([]pb.Entry{}, ents[:size]...) // protect the original slice
	}
	for _, tt := range []struct {
		maxSize uint64
		want    []pb.Entry
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
		msgt    pb.MessageType
		isLocal bool
	}{
		{pb.MsgHup, true},
		{pb.MsgBeat, true},
		{pb.MsgUnreachable, true},
		{pb.MsgSnapStatus, true},
		{pb.MsgTransferLeader, false},
		{pb.MsgProp, false},
		{pb.MsgApp, false},
		{pb.MsgAppResp, false},
		{pb.MsgVote, false},
		{pb.MsgVoteResp, false},
		{pb.MsgSnap, false},
		{pb.MsgHeartbeat, false},
		{pb.MsgHeartbeatResp, false},
		{pb.MsgTimeoutNow, false},
		{pb.MsgPreVote, false},
		{pb.MsgPreVoteResp, false},
		{pb.MsgStorageAppend, true},
		{pb.MsgStorageAppendResp, true},
		{pb.MsgStorageApply, true},
		{pb.MsgStorageApplyResp, true},
		{pb.MsgForgetLeader, false},
		{pb.MsgFortifyLeader, false},
		{pb.MsgFortifyLeaderResp, false},
		{pb.MsgDeFortifyLeader, false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.msgt), func(t *testing.T) {
			require.Equal(t, tt.isLocal, IsLocalMsg(tt.msgt))
		})
	}
}

func TestIsResponseMsg(t *testing.T) {
	tests := []struct {
		msgt       pb.MessageType
		isResponse bool
	}{
		{pb.MsgHup, false},
		{pb.MsgBeat, false},
		{pb.MsgUnreachable, true},
		{pb.MsgSnapStatus, false},
		{pb.MsgTransferLeader, false},
		{pb.MsgProp, false},
		{pb.MsgApp, false},
		{pb.MsgAppResp, true},
		{pb.MsgVote, false},
		{pb.MsgVoteResp, true},
		{pb.MsgSnap, false},
		{pb.MsgHeartbeat, false},
		{pb.MsgHeartbeatResp, true},
		{pb.MsgTimeoutNow, false},
		{pb.MsgPreVote, false},
		{pb.MsgPreVoteResp, true},
		{pb.MsgStorageAppend, false},
		{pb.MsgStorageAppendResp, true},
		{pb.MsgStorageApply, false},
		{pb.MsgStorageApplyResp, true},
		{pb.MsgForgetLeader, false},
		{pb.MsgFortifyLeader, false},
		{pb.MsgFortifyLeaderResp, true},
		{pb.MsgDeFortifyLeader, false},
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
		msgt            pb.MessageType
		isMsgFromLeader bool
	}{
		{pb.MsgHup, false},
		{pb.MsgBeat, false},
		{pb.MsgUnreachable, false},
		{pb.MsgSnapStatus, false},
		{pb.MsgTransferLeader, false},
		{pb.MsgProp, false},
		{pb.MsgApp, true},
		{pb.MsgAppResp, false},
		{pb.MsgVote, false},
		{pb.MsgVoteResp, false},
		{pb.MsgSnap, false},
		{pb.MsgHeartbeat, true},
		{pb.MsgHeartbeatResp, false},
		{pb.MsgTimeoutNow, true},
		{pb.MsgPreVote, false},
		{pb.MsgPreVoteResp, false},
		{pb.MsgStorageAppend, false},
		{pb.MsgStorageAppendResp, false},
		{pb.MsgStorageApply, false},
		{pb.MsgStorageApplyResp, false},
		{pb.MsgForgetLeader, false},
		{pb.MsgFortifyLeader, true},
		{pb.MsgFortifyLeaderResp, false},
		{pb.MsgDeFortifyLeader, true},
	}

	for i, tt := range tests {
		got := IsMsgFromLeader(tt.msgt)
		if got != tt.isMsgFromLeader {
			t.Errorf("#%d: got %v, want %v", i, got, tt.isMsgFromLeader)
		}
		if got {
			require.True(t, IsMsgIndicatingLeader(tt.msgt),
				"IsMsgFromLeader should imply IsMsgIndicatingLeader")
		}
	}
}

func TestMsgIndicatingLeader(t *testing.T) {
	tests := []struct {
		msgt                  pb.MessageType
		isMsgIndicatingLeader bool
	}{
		{pb.MsgHup, false},
		{pb.MsgBeat, false},
		{pb.MsgUnreachable, false},
		{pb.MsgSnapStatus, false},
		{pb.MsgTransferLeader, false},
		{pb.MsgProp, false},
		{pb.MsgApp, true},
		{pb.MsgAppResp, false},
		{pb.MsgVote, false},
		{pb.MsgVoteResp, false},
		{pb.MsgSnap, true},
		{pb.MsgHeartbeat, true},
		{pb.MsgHeartbeatResp, false},
		{pb.MsgTimeoutNow, true},
		{pb.MsgPreVote, false},
		{pb.MsgPreVoteResp, false},
		{pb.MsgStorageAppend, false},
		{pb.MsgStorageAppendResp, false},
		{pb.MsgStorageApply, false},
		{pb.MsgStorageApplyResp, false},
		{pb.MsgForgetLeader, false},
		{pb.MsgFortifyLeader, true},
		{pb.MsgFortifyLeaderResp, false},
		{pb.MsgDeFortifyLeader, true},
	}

	for i, tt := range tests {
		got := IsMsgIndicatingLeader(tt.msgt)
		if got != tt.isMsgIndicatingLeader {
			t.Errorf("#%d: got %v, want %v", i, got, tt.isMsgIndicatingLeader)
		}
	}
}

// TestPayloadSizeOfEmptyEntry ensures that payloadSize of empty entry is always zero.
// This property is important because new leaders append an empty entry to their log,
// and we don't want this to count towards the uncommitted log quota.
func TestPayloadSizeOfEmptyEntry(t *testing.T) {
	e := pb.Entry{Data: nil}
	require.Equal(t, 0, int(payloadSize(e)))
}
