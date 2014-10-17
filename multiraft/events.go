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

// An EventLeaderElection is broadcast when a group completes an election.
// TODO(bdarnell): emit EventLeaderElection from follower nodes as well.
type EventLeaderElection struct {
	GroupID uint64
	NodeID  uint64
}

// An EventCommandCommitted is broadcast whenever a command has been committed.
type EventCommandCommitted struct {
	Command []byte
}
