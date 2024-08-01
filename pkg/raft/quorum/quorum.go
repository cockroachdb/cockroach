// This code has been modified from its original form by Cockroach Labs, Inc.
// All modifications are Copyright 2024 Cockroach Labs, Inc.
//
// Copyright 2019 The etcd Authors
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

package quorum

import (
	"cmp"
	"math"
	"strconv"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

// Index is a Raft log position.
type Index uint64

func (i Index) String() string {
	if i == math.MaxUint64 {
		return "∞"
	}
	return strconv.FormatUint(uint64(i), 10)
}

// Compare returns -1 if this Index is lesser than the one supplied, 1 if it is
// greater, and 0 if they're equal.
func (a Index) Compare(b Index) int {
	return cmp.Compare(a, b)
}

// AckedIndexer is a specialization of ComparableMap that allows looking up
// commit indexes for a given ID.
type AckedIndexer ComparableMap[Index]

// mapAckIndexer is a type of AckedIndexer. It's primarily used as a type to
// mock commit index lookups in tests. See ProgressMap for the production
// implementation.
type mapAckIndexer map[pb.PeerID]Index

// Get implements the ComparableMap interface.
func (m mapAckIndexer) Get(id pb.PeerID) (Index, bool) {
	idx, ok := m[id]
	return idx, ok
}

// VoteResult indicates the outcome of a vote.
//
//go:generate stringer -type=VoteResult
type VoteResult uint8

const (
	// VotePending indicates that the decision of the vote depends on future
	// votes, i.e. neither "yes" or "no" has reached quorum yet.
	VotePending VoteResult = iota + 1
	// VoteLost indicates that the quorum has voted "no".
	VoteLost
	// VoteWon indicates that the quorum has voted "yes".
	VoteWon
)
