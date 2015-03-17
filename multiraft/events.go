// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package multiraft

import (
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft/raftpb"
)

// An EventLeaderElection is broadcast when a group starts or completes
// an election. NodeID is zero when an election is in progress.
type EventLeaderElection struct {
	GroupID uint64
	NodeID  NodeID
	Term    uint64
}

// An EventCommandCommitted is broadcast whenever a command has been committed.
type EventCommandCommitted struct {
	GroupID uint64
	// CommandID is the application-supplied ID for this command. The same CommandID
	// may be seen multiple times, so the application should remember this CommandID
	// for deduping.
	CommandID string
	// Index is the raft log index for this event. The application should persist
	// the Index of the last applied command atomically with any effects of that
	// command.
	Index   uint64
	Command []byte
}

// An EventMembershipChangeCommitted is broadcast whenever a membership change
// has been committed.
type EventMembershipChangeCommitted struct {
	// GroupID, CommandID, and Index are the same as for EventCommandCommitted.
	GroupID    uint64
	CommandID  string
	Index      uint64
	NodeID     NodeID
	ChangeType raftpb.ConfChangeType
	Payload    []byte

	// Callback should be invoked when this event and its payload have been
	// processed. A non-nil error aborts the membership change.
	Callback func(error)
}

// Commands are encoded with a 1-byte version (currently 0), a 16-byte ID,
// followed by the payload. This inflexible encoding is used so we can efficiently
// parse the command id while processing the logs.
const (
	commandIDLen                = 16
	commandEncodingVersion byte = 0
)

func encodeCommand(commandID string, command []byte) []byte {
	if len(commandID) != commandIDLen {
		log.Fatalf("invalid command ID length; %d != %d", len(commandID), commandIDLen)
	}
	x := make([]byte, 1, 1+commandIDLen+len(command))
	x[0] = commandEncodingVersion
	x = append(x, []byte(commandID)...)
	x = append(x, command...)
	return x
}

func decodeCommand(data []byte) (commandID string, command []byte) {
	if data[0] != commandEncodingVersion {
		log.Fatalf("unknown command encoding version %v", data[0])
	}
	return string(data[1 : 1+commandIDLen]), data[1+commandIDLen:]
}
