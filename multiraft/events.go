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

import "github.com/cockroachdb/cockroach/util/log"

// An EventLeaderElection is broadcast when a group starts or completes
// an election. NodeID is zero when an election is in progress.
type EventLeaderElection struct {
	GroupID uint64
	NodeID  uint64
	Term    uint64
}

// An EventCommandCommitted is broadcast whenever a command has been committed.
type EventCommandCommitted struct {
	CommandID []byte
	Command   []byte
}

// Commands are encoded with a 1-byte version (currently 0), a 16-byte ID,
// followed by the payload. This inflexible encoding is used so we can efficiently
// parse the command id while processing the logs.
const (
	commandIDLen           = 16
	commandEncodingVersion = 0
)

func encodeCommand(commandID, command []byte) []byte {
	if len(commandID) != commandIDLen {
		log.Fatalf("invalid command ID length; %d != %d", len(commandID), commandIDLen)
	}
	x := make([]byte, 1, 1+commandIDLen+len(command))
	x[0] = commandEncodingVersion
	x = append(x, commandID...)
	x = append(x, command...)
	return x
}

func decodeCommand(data []byte) (commandID, command []byte) {
	if data[0] != commandEncodingVersion {
		log.Fatalf("unknown command encoding version %v", data[0])
	}
	return data[1 : 1+commandIDLen], data[1+commandIDLen:]
}
