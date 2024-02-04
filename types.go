// Copyright 2024 The etcd Authors
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

import pb "go.etcd.io/raft/v3/raftpb"

// entryID uniquely identifies a raft log entry.
//
// Every entry is associated with a leadership term which issued this entry and
// initially appended it to the log. There can only be one leader at any term,
// and a leader never issues two entries with the same index.
type entryID struct {
	term  uint64
	index uint64
}

// pbEntryID returns the ID of the given pb.Entry.
func pbEntryID(entry *pb.Entry) entryID {
	return entryID{term: entry.Term, index: entry.Index}
}
